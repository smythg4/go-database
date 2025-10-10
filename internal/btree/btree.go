package btree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"godb/internal/pager"
	"godb/internal/schema"
	"os"
)

type BTree struct {
	pc *pager.PageCache
}

func NewBTree(dm *pager.DiskManager, header *pager.TableHeader) *BTree {
	npc := pager.NewPageCache(dm, header)
	return &BTree{
		pc: npc,
	}
}

func (bt *BTree) allocatePage() pager.PageID {
	return bt.pc.AllocatePage()
}

func (bt *BTree) GetDepth() int {
	breadcrumbs := &BTStack{}
	_, _ = bt.findLeaf(0, breadcrumbs)
	return breadcrumbs.Length() + 1
}

func (bt *BTree) loadNode(pageID pager.PageID) (*BNode, error) {
	sp, err := bt.pc.Fetch(pageID)
	if err != nil {
		return nil, err
	}
	return &BNode{SlottedPage: sp}, nil
}

func (bt *BTree) writeNode(node *BNode) error {
	// if page not in cache, it must be a newly created page
	if !bt.pc.Contains(node.PageID) {
		if err := bt.pc.AddNewPage(node.SlottedPage); err != nil {
			return err
		}
	}
	return bt.pc.MakeDirty(node.PageID)
}

func (bn *BNode) IsLeaf() bool {
	return bn.PageType == pager.LEAF
}

func (bt *BTree) findLeaf(key uint64, breadcrumbs *BTStack) (pager.PageID, error) {
	currentPageID := bt.pc.GetRootPageID()
	for {
		node, err := bt.loadNode(currentPageID)
		if err != nil {
			return 0, err
		}
		defer bt.pc.UnPin(node.PageID)

		if node.IsLeaf() {
			break
		}

		childPageID, insertionIndex := node.SearchInternal(key)
		breadcrumbs.push(currentPageID, insertionIndex)
		currentPageID = childPageID
	}
	return currentPageID, nil
}

func (bt *BTree) handleRootSplit(promotedKey uint64, leftChildID, rightChildID pager.PageID) error {
	// allocate a page for the new root node
	newRootID := bt.allocatePage()

	nsp := pager.NewSlottedPage(newRootID, pager.INTERNAL)
	newRoot := &BNode{SlottedPage: nsp}
	defer bt.pc.UnPin(newRoot.PageID)

	// Insert: [promotedKey -> oldRoot]
	// The old root will become the left child at this key
	internalRecord := pager.SerializeInternalRecord(promotedKey, leftChildID)
	_, err := newRoot.InsertRecordSorted(internalRecord)
	if err != nil {
		return err
	}

	// the new sibling from the split becomes the rightmostchild of the new root
	newRoot.RightmostChild = rightChildID
	// update tree header to point to new root
	bt.pc.SetRootPageID(newRootID)

	// write the new root to the disk
	return bt.writeNode(newRoot)
}

func (bt *BTree) propogateSplit(promotedKey uint64, rightPageID, leftPageID pager.PageID, breadcrumbs *BTStack) error {
	// leftPageID and rightPageID represent the two children from the most recent split
	// promotedKey is the separator between them

	for !breadcrumbs.isEmpty() {
		bc, _ := breadcrumbs.pop()
		parent, err := bt.loadNode(bc.PageID)
		if err != nil {
			return err
		}
		defer bt.pc.UnPin(parent.PageID)

		// insert the promoted key into the parent
		// [promotedKey, leftPageID] means keys < promotedKey go to leftPageID
		internalRecord := pager.SerializeInternalRecord(promotedKey, leftPageID)
		insertIndex, err := parent.InsertRecordSorted(internalRecord)

		if err == nil {
			// success - write the parent node
			if insertIndex+1 < int(parent.NumSlots) {
				// update the next record to point to the right child
				oldKey, _ := pager.DeserializeInternalRecord(parent.Records[insertIndex+1])
				parent.Records[insertIndex+1] = pager.SerializeInternalRecord(oldKey, rightPageID)
			} else {
				// inserted as last key - update the RightmostChild
				parent.RightmostChild = rightPageID
			}
			return bt.writeNode(parent)
		}

		if err.Error() != "page full" {
			// something went wrong other than a full page
			return err
		}

		// page must have been full, now we split
		nextPage := bt.allocatePage()
		rightNode, newPromotedKey, err := parent.splitNode(nextPage)
		if err != nil {
			return err
		}
		defer bt.pc.UnPin(rightNode.PageID)

		// write both halves of parent split and increment NextPageID
		if err := bt.writeNode(parent); err != nil {
			return err
		}
		if err := bt.writeNode(rightNode); err != nil {
			return err
		}

		// update for the next iteration
		// the parent that just split becomes the left child for the next level
		promotedKey = newPromotedKey
		leftPageID = parent.PageID
		rightPageID = rightNode.PageID
	}

	// breadcrumbs is empty - the root must split
	// leftPageID is the old root (left side), rightPageID is the new sibling (right side)
	return bt.handleRootSplit(promotedKey, leftPageID, rightPageID)
}

func (bt *BTree) Insert(key uint64, data []byte) error {
	breadcrumbs := &BTStack{}
	defer func() {
		bt.pc.FlushHeader()
	}()

	// traverse to leaf, collecting breadcrumbs
	leafPageID, err := bt.findLeaf(key, breadcrumbs)
	if err != nil {
		return err
	}

	leaf, err := bt.loadNode(leafPageID)
	if err != nil {
		return err
	}
	defer bt.pc.UnPin(leaf.PageID)
	if _, present := leaf.Search(key); present {
		return fmt.Errorf("key %d already exists", key)
	}
	_, err = leaf.InsertRecordSorted(data)

	if err == nil {
		// happy path
		return bt.writeNode(leaf)
	}

	if err.Error() != "page full" {
		// bad path
		return err
	}

	// page was full, time to split
	nextPage := bt.allocatePage()
	rightNode, promotedKey, err := leaf.splitNode(nextPage)
	if err != nil {
		return err
	}
	defer bt.pc.UnPin(rightNode.PageID)

	// write both halves
	if err := bt.writeNode(leaf); err != nil {
		return err
	}
	if err := bt.writeNode(rightNode); err != nil {
		return err
	}

	// retry insert into appropriate half
	if key < promotedKey {
		_, err = leaf.InsertRecordSorted(data)
	} else {
		_, err = rightNode.InsertRecordSorted(data)
	}
	if err != nil {
		return err
	}

	// write whichever node we just inserted into
	if key < promotedKey {
		err = bt.writeNode(leaf)
	} else {
		err = bt.writeNode(rightNode)
	}
	if err != nil {
		return err
	}

	return bt.propogateSplit(promotedKey, rightNode.PageID, leafPageID, breadcrumbs)
}

func (bt *BTree) Search(key uint64) ([]byte, bool, error) {
	// safety net
	maxDepth := 100

	currentPageID := bt.pc.GetRootPageID()

	// traverse down to leaf
	for depth := 0; depth < maxDepth; depth++ {
		node, err := bt.loadNode(currentPageID)
		if err != nil {
			return nil, false, err
		}

		if node.IsLeaf() {
			// search in the leaf node
			slotIndex, found := node.Search(key)
			if !found {
				bt.pc.UnPin(node.PageID)
				return nil, false, nil // key not found
			}

			// get the data record
			data, err := node.GetRecord(slotIndex)
			if err != nil {
				bt.pc.UnPin(node.PageID)
				return nil, false, err
			}
			bt.pc.UnPin(node.PageID)
			return data, true, nil
		}

		// internal node - find child
		childPageID, _ := node.SearchInternal(key)
		bt.pc.UnPin(node.PageID)
		currentPageID = childPageID
	}
	return nil, false, nil // key not found in 100 rounds
}

func (bt *BTree) RangeScan(startKey, endKey uint64) ([][]byte, error) {
	// start at the leaf containing startKey
	leafPageID, _ := bt.findLeaf(startKey, &BTStack{})

	var results [][]byte
	visited := make(map[pager.PageID]bool) // cycle detection

	for leafPageID != 0 { // 0 = end of the line
		// Check for cycles
		if visited[leafPageID] {
			// Build chain for debugging
			chain := "Cycle: "
			current, _ := bt.findLeaf(startKey, &BTStack{})
			for i := 0; i < 20 && current != 0; i++ {
				n, _ := bt.loadNode(current)
				chain += fmt.Sprintf("%d->", current)
				current = n.NextLeaf
				bt.pc.UnPin(n.PageID)
			}
			return nil, fmt.Errorf("cycle detected in leaf chain at page %d. Chain: %s", leafPageID, chain)
		}
		visited[leafPageID] = true

		leaf, err := bt.loadNode(leafPageID)
		if err != nil {
			return nil, err
		}

		for i := 0; i < int(leaf.NumSlots); i++ {
			key := leaf.GetKey(i)
			if key >= startKey && key <= endKey {
				data, _ := leaf.GetRecord(i)
				results = append(results, data)
			} else if key > endKey {
				// break out of both loops, we're done
				leafPageID = 0
				break
			}
		}
		bt.pc.UnPin(leaf.PageID)
		leafPageID = leaf.NextLeaf
	}
	return results, nil
}

func (bt *BTree) findLeftSibling(parent *BNode, childIndex int) (pager.PageID, int, bool) {
	if childIndex == 0 {
		return 0, -1, false // no left sibling
	}
	if parent.PageType != pager.INTERNAL {
		return 0, -1, false // parent must be an internal page (not a leaf)
	}

	var siblingID pager.PageID
	var separatorIndex int

	if childIndex == -1 {
		// we're rightmostchild, left sibling is last non-tombstone record
		separatorIndex = int(parent.NumSlots) - 1
		if separatorIndex < 0 {
			return 0, -1, false // no records
		}
		_, siblingID = pager.DeserializeInternalRecord(parent.Records[separatorIndex])
	} else {
		// normal case: left sibling is at childIndex-1
		separatorIndex = childIndex - 1
		_, siblingID = pager.DeserializeInternalRecord(parent.Records[separatorIndex])
	}

	return siblingID, separatorIndex, true
}

func (bt *BTree) findRightSibling(parent *BNode, childIndex int) (pager.PageID, int, bool) {
	if childIndex == -1 {
		return 0, -1, false // we're the rightmostchild
	}
	if parent.PageType != pager.INTERNAL {
		return 0, -1, false // parent must be an internal page (not a leaf)
	}

	var siblingID pager.PageID
	separatorIndex := childIndex

	if childIndex+1 >= int(parent.NumSlots) {
		// no records after childIndex, right sibling is rightmostChild
		siblingID = parent.RightmostChild
	} else {
		// right sibling is at childIndex+1
		_, siblingID = pager.DeserializeInternalRecord(parent.Records[childIndex+1])
	}
	return siblingID, separatorIndex, true
}

func (bt *BTree) mergeInternalNodes(sibling, underflowNode, parent *BNode, separatorIndex int, mergeIntoSibling bool) error {
	// true = we get merged into left, false = right merged into us
	if sibling.IsLeaf() || underflowNode.IsLeaf() || parent.IsLeaf() {
		return errors.New("both siblings and parent must be INTERNAL nodes, not leaves")
	}

	// determine left or right
	var leftNode, rightNode *BNode
	if mergeIntoSibling {
		leftNode = sibling
		rightNode = underflowNode
	} else {
		leftNode = underflowNode
		rightNode = sibling
	}

	// get the key to demote
	separatorKey, _ := pager.DeserializeInternalRecord(parent.Records[separatorIndex])

	// demote: insert separator into the left, pointing to the left's rightmostchild
	demotedRecord := pager.SerializeInternalRecord(separatorKey, leftNode.RightmostChild)
	_, err := leftNode.InsertRecordSorted(demotedRecord)
	if err != nil {
		return err
	}

	// merge right into left
	err = leftNode.MergeInternals(rightNode.SlottedPage)
	if err != nil {
		return err
	}

	// write merged node
	if err := bt.writeNode(leftNode); err != nil {
		return err
	}

	bt.pc.FreePage(rightNode.PageID)

	// remove separator from parent
	if err := parent.DeleteRecord(separatorIndex); err != nil {
		return err
	}

	// after compact, records shift - separatorIndex now points to what was the next record
	if separatorIndex < int(parent.NumSlots) {
		// update this record's pointer to point to merged node
		oldKey, _ := pager.DeserializeInternalRecord(parent.Records[separatorIndex])
		parent.Records[separatorIndex] = pager.SerializeInternalRecord(oldKey, leftNode.PageID)
	} else {
		// no more records, update rightmostchild
		parent.RightmostChild = leftNode.PageID
	}

	return nil
}

func (bt *BTree) mergeLeafNodes(sibling, underflowNode, parent *BNode, separatorIndex int, mergeIntoSibling bool) error {
	// true = we get merged into left, false = right merged into us
	if !sibling.IsLeaf() || !underflowNode.IsLeaf() || parent.IsLeaf() {
		return errors.New("both siblings must be LEAF, parent must be INTERNAL")
	}

	var leftNode, rightNode *BNode
	if mergeIntoSibling {
		// Found left sibling - merge right into left
		leftNode = sibling
		rightNode = underflowNode
	} else {
		// Found right sibling - merge right into left
		leftNode = underflowNode
		rightNode = sibling
	}

	// always merge right into left
	if err := leftNode.MergeLeaf(rightNode.SlottedPage); err != nil {
		return err
	}

	// write merged (left) node
	if err := bt.writeNode(leftNode); err != nil {
		return err
	}

	// right node is always orphaned
	bt.pc.FreePage(rightNode.PageID)

	// remove separator from parent
	if err := parent.DeleteRecord(separatorIndex); err != nil {
		return err
	}

	// update parent pointer to point to left (survivor)
	if separatorIndex < int(parent.NumSlots) {
		oldKey, _ := pager.DeserializeInternalRecord(parent.Records[separatorIndex])
		parent.Records[separatorIndex] = pager.SerializeInternalRecord(oldKey, leftNode.PageID)
	} else {
		parent.RightmostChild = leftNode.PageID
	}

	return nil
}

func (bt *BTree) handleRootUnderflow(pageID pager.PageID) error {
	if pageID != bt.pc.GetRootPageID() {
		return fmt.Errorf("rootpageid mismatch; actual: %d, expected: %d", pageID, bt.pc.GetRootPageID())
	}
	root, err := bt.loadNode(pageID)
	if err != nil {
		return err
	}
	defer bt.pc.UnPin(root.PageID)
	if root.IsLeaf() {
		// root leaf is underfull but can't collapse - just accept it
		return nil
	}

	// Internal root - only collapse if completely empty (NumSlots == 0)
	if root.NumSlots > 0 {
		// Still has keys - underfull is OK for root
		return nil
	}

	// Root has 0 slots, only RightmostChild remains - collapse it
	if root.RightmostChild == 0 {
		return fmt.Errorf("internal root has no children")
	}

	bt.pc.SetRootPageID(root.RightmostChild)
	return nil
}

func (bt *BTree) handleUnderflow(pageID pager.PageID, breadcrumbs *BTStack) error {
	if breadcrumbs.isEmpty() {
		// root underflow -- promote root's rightmostchild to root
		return bt.handleRootUnderflow(pageID)
	}

	bc, err := breadcrumbs.pop()
	if err != nil {
		// should never occur since we checked for an empty stack already
		return err
	}

	parent, err := bt.loadNode(bc.PageID)
	if err != nil {
		// error loading the node
		return err
	}
	defer bt.pc.UnPin(parent.PageID)

	underflowNode, err := bt.loadNode(pageID)
	if err != nil {
		return err
	}
	defer bt.pc.UnPin(underflowNode.PageID)

	var siblingID pager.PageID
	var separatorIndex int
	var mergeIntoSibling bool // true = we get merged into left, false = right merged into us

	// try left sibling first
	leftSiblingID, leftSepIdx, hasLeft := bt.findLeftSibling(parent, bc.Index)

	if hasLeft {
		siblingID = leftSiblingID
		separatorIndex = leftSepIdx
		mergeIntoSibling = true
	} else {
		// right will only be used if no left
		rightSiblingID, rightSepIdx, hasRight := bt.findRightSibling(parent, bc.Index)
		if hasRight {
			siblingID = rightSiblingID
			separatorIndex = rightSepIdx
			mergeIntoSibling = false
		} else {
			return fmt.Errorf("node has no siblings to merge with")
		}
	}

	sibling, err := bt.loadNode(siblingID)
	if err != nil {
		return err
	}
	defer bt.pc.UnPin(sibling.PageID)

	// check if merge is possible (skip if too large)
	if !sibling.CanMergeWith(underflowNode.SlottedPage) {
		return nil // no merge occurs (this can be optimized later with "borrowing" or rebalances)
	}

	// perform merge based on node type
	if underflowNode.IsLeaf() {
		err = bt.mergeLeafNodes(sibling, underflowNode, parent, separatorIndex, mergeIntoSibling)
	} else {
		err = bt.mergeInternalNodes(sibling, underflowNode, parent, separatorIndex, mergeIntoSibling)
	}

	if err != nil {
		// error with merge
		return err
	}

	// CRITICAL: Write parent BEFORE checking underflow
	// Parent has updated child pointers that must be persisted
	if err := bt.writeNode(parent); err != nil {
		return err
	}

	// check if parent is now underfull
	if parent.IsUnderfull() {
		return bt.handleUnderflow(parent.PageID, breadcrumbs)
	}

	return nil
}

func (bt *BTree) Delete(key uint64) error {
	breadcrumbs := &BTStack{}
	defer func() {
		bt.pc.FlushHeader()
	}()
	// traverse to leaf, collecting breadcrumbs
	leafPageID, err := bt.findLeaf(key, breadcrumbs)
	if err != nil {
		return err
	}
	leaf, err := bt.loadNode(leafPageID)
	if err != nil {
		return err
	}
	defer bt.pc.UnPin(leaf.PageID)
	idx, present := leaf.Search(key)
	if !present {
		return fmt.Errorf("key %d was not found", key)
	}
	err = leaf.DeleteRecord(idx)
	if err != nil {
		return err
	}

	// It was working when I commented out the merge. I think the node needs to write before starting the merge
	if err := bt.writeNode(leaf); err != nil {
		return err
	}

	// check if nodes need to merge
	if leaf.IsUnderfull() {
		return bt.handleUnderflow(leafPageID, breadcrumbs)
	}
	return bt.writeNode(leaf)
}

type BNode struct {
	*pager.SlottedPage
}

type BreadCrumb struct {
	PageID pager.PageID
	Index  int // which child pointer we followed
}

type BTStack struct {
	Crumbs []BreadCrumb
}

func (s *BTStack) Length() int {
	return len(s.Crumbs)
}

func (s *BTStack) isEmpty() bool {
	return len(s.Crumbs) == 0
}

func (s *BTStack) push(id pager.PageID, idx int) {
	s.Crumbs = append(s.Crumbs, BreadCrumb{PageID: id, Index: idx})
}

func (s *BTStack) pop() (BreadCrumb, error) {
	if s.isEmpty() {
		return BreadCrumb{}, errors.New("breadcrumb stack empty")
	}
	bc := s.Crumbs[len(s.Crumbs)-1]
	s.Crumbs = s.Crumbs[:len(s.Crumbs)-1]
	return bc, nil
}

func (s *BTStack) peek() (BreadCrumb, error) {
	if s.isEmpty() {
		return BreadCrumb{}, errors.New("breadcrumb stack empty")
	}
	return s.Crumbs[len(s.Crumbs)-1], nil
}

func (bn *BNode) splitNode(newPageID pager.PageID) (*BNode, uint64, error) {
	switch bn.PageType {
	case pager.LEAF:
		sp, promKey, err := bn.SplitLeaf(newPageID)
		if err != nil {
			return nil, 0, err
		}
		return &BNode{SlottedPage: sp}, promKey, nil
	case pager.INTERNAL:
		sp, promKey, err := bn.SplitInternal(newPageID)
		if err != nil {
			return nil, 0, err
		}
		return &BNode{SlottedPage: sp}, promKey, nil
	default:
		return nil, 0, fmt.Errorf("invalid page type: %v", bn.PageType)
	}
}

func (bt *BTree) Stats() string {
	root, err := bt.loadNode(bt.pc.GetRootPageID())
	if err != nil {
		return fmt.Sprintf("error loading node %d", bt.pc.GetRootPageID())
	}
	defer bt.pc.UnPin(root.PageID)
	depth := bt.GetDepth()
	return fmt.Sprintf("Root: page %d, Type: %v, NextPageID: %d, NumPages: %d, Tree Depth: %d",
		bt.pc.GetRootPageID(), root.PageType, bt.pc.GetHeader().NextPageID, bt.pc.GetHeader().NumPages, depth)
}

func (bt *BTree) Vacuum() error {
	// update to bulk loading
	// update to push file operations into PageCache? Maybe use Vacuum() to build a new tree
	// then have pc.UpdateTree(bt BTree) actually handle the behind the curtain file swap

	tempFile := bt.pc.GetSchema().TableName + ".db.tmp"
	f, err := os.Create(tempFile)
	if err != nil {
		return err
	}
	tempDM := pager.NewDiskManager(f)

	// Create fresh header with same schema but reset page IDs
	freshHeader := pager.DefaultTableHeader(bt.pc.GetSchema())
	tempDM.SetHeader(freshHeader)
	if err := tempDM.WriteHeader(); err != nil {
		return err
	}

	// Create fresh root page
	rootPage := pager.NewSlottedPage(1, pager.LEAF)
	if err := tempDM.WriteSlottedPage(rootPage); err != nil {
		return err
	}

	newTree := NewBTree(&tempDM, &freshHeader)
	leafID, err := bt.findLeaf(0, &BTStack{})
	if err != nil {
		return err
	}

	for leafID != 0 {
		leaf, err := bt.loadNode(leafID)
		if err != nil {
			return err
		}

		for i := 0; i < int(leaf.NumSlots); i++ {
			record := leaf.Records[i]
			if len(record) < 8 {
				continue // skip malformed records
			}
			// Extract key from first 8 bytes
			key := binary.LittleEndian.Uint64(record[:8])
			if err := newTree.Insert(key, record); err != nil {
				// Skip duplicates (caused by stale pointers to freed pages)
				if err.Error() != fmt.Sprintf("key %d already exists", key) {
					bt.pc.UnPin(leaf.PageID)
					return err
				}
			}
		}
		bt.pc.UnPin(leaf.PageID)
		leafID = leaf.NextLeaf
	}

	// Flush new tree (pages + header) and close the file
	if err := newTree.pc.Close(); err != nil {
		return err
	}

	// close the old file
	if err := bt.pc.Close(); err != nil {
		return err
	}

	// rename temp file to original file name
	origFile := bt.pc.GetSchema().TableName + ".db"
	if err := os.Rename(tempFile, origFile); err != nil {
		return err
	}

	// reopen with new file
	f, err = os.OpenFile(origFile, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	return bt.pc.UpdateFile(f)
}

func (bt *BTree) ExtractPrimaryKey(record schema.Record) (uint64, error) {
	return bt.pc.GetHeader().Schema.ExtractPrimaryKey(record)
}

func (bt *BTree) SerializeRecord(record schema.Record) ([]byte, error) {
	return bt.pc.GetHeader().Schema.SerializeRecord(record)
}

func (bt *BTree) DeserializeRecord(data []byte) (uint64, schema.Record, error) {
	return bt.pc.GetHeader().Schema.DeserializeRecord(data)
}

func (bt *BTree) GetSchema() schema.Schema {
	return bt.pc.GetHeader().Schema
}

func (bt *BTree) Close() error {
	return bt.pc.Close()
}
