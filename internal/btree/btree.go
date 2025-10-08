package btree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"godb/internal/pager"
	"os"
)

type BTree struct {
	dm     *pager.DiskManager
	Header *pager.TableHeader
}

func NewBTree(dm *pager.DiskManager, header *pager.TableHeader) *BTree {
	return &BTree{
		dm:     dm,
		Header: header,
	}
}

func (bt *BTree) allocatePage() pager.PageID {
	if len(bt.Header.FreePageIDs) > 0 {
		// pop from free page list
		pageID := bt.Header.FreePageIDs[len(bt.Header.FreePageIDs)-1]
		bt.Header.FreePageIDs = bt.Header.FreePageIDs[:len(bt.Header.FreePageIDs)-1]
		return pageID
	}

	pageID := bt.Header.NextPageID
	bt.Header.NextPageID++
	return pageID
}

func (bt *BTree) GetDepth() int {
	breadcrumbs := &BTStack{}
	_, _ = bt.findLeaf(0, breadcrumbs)
	return breadcrumbs.Length() + 1
}

func (bt *BTree) loadNode(pageID pager.PageID) (*BNode, error) {
	sp, err := bt.dm.ReadSlottedPage(pageID)
	if err != nil {
		return nil, err
	}
	return &BNode{SlottedPage: sp}, nil
}

func (bt *BTree) writeNode(node *BNode) error {
	return bt.dm.WriteSlottedPage(node.SlottedPage)
}

func (bn *BNode) IsLeaf() bool {
	return bn.PageType == pager.LEAF
}

func (bt *BTree) findLeaf(key uint64, breadcrumbs *BTStack) (pager.PageID, error) {
	currentPageID := bt.Header.RootPageID
	for {
		node, err := bt.loadNode(currentPageID)
		if err != nil {
			return 0, err
		}

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
	bt.Header.RootPageID = newRootID

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
		// we were writing stale headers. This will ensure that it's up to date before writing to the disk
		bt.Header.NumPages = uint32(bt.Header.NextPageID - 1)
		bt.dm.SetHeader(*bt.Header)
		bt.dm.WriteHeader()
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

	currentPageID := bt.Header.RootPageID

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
				return nil, false, nil // key not found
			}

			// get the data record
			data, err := node.GetRecord(slotIndex)
			if err != nil {
				return nil, false, err
			}

			return data, true, nil
		}

		// internal node - find child
		childPageID, _ := node.SearchInternal(key)
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
			return nil, fmt.Errorf("cycle detected in leaf chain at page %d", leafPageID)
		}
		visited[leafPageID] = true

		leaf, _ := bt.loadNode(leafPageID)
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
		// we're rightmostchild, left sibling is last record
		separatorIndex = int(parent.NumSlots) - 1
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

	if childIndex == int(parent.NumSlots)-1 {
		// right sibling is rightmostchild
		siblingID = parent.RightmostChild
	} else {
		// normal case: right sibling is at childIndex+1
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

	orphanedPageID := rightNode.PageID
	bt.Header.FreePageIDs = append(bt.Header.FreePageIDs, orphanedPageID)
	// remove separator from parent
	if err := parent.DeleteRecord(separatorIndex); err != nil {
		return err
	}

	// update parent pointer
	/// this will need to change if we shift to a tombstone model
	if separatorIndex < int(parent.NumSlots) {
		oldKey, _ := pager.DeserializeInternalRecord(parent.Records[separatorIndex])
		parent.Records[separatorIndex] = pager.SerializeInternalRecord(oldKey, leftNode.PageID)
	} else {
		parent.RightmostChild = leftNode.PageID
	}

	return nil
}

func (bt *BTree) mergeLeafNodes(sibling, underflowNode, parent *BNode, separatorIndex int, mergeIntoSibling bool) error {
	// true = we get merged into left, false = right merged into us
	if !sibling.IsLeaf() || !underflowNode.IsLeaf() || parent.IsLeaf() {
		return errors.New("both siblings must be LEAF, parent must be INTERNAL")
	}

	var mergedNode *BNode
	var err error
	if mergeIntoSibling {
		// this means we found a left sibling, merge into that
		err = sibling.MergeLeaf(underflowNode.SlottedPage)
		mergedNode = sibling
	} else {
		// this means we found a right sibling, merge us into it
		err = underflowNode.MergeLeaf(sibling.SlottedPage)
		mergedNode = underflowNode
	}
	if err != nil {
		return err
	}

	// write mergedNode
	if err := bt.writeNode(mergedNode); err != nil {
		return err
	}

	var orphanedPageID pager.PageID
	if mergeIntoSibling {
		orphanedPageID = underflowNode.PageID
	} else {
		orphanedPageID = sibling.PageID
	}
	bt.Header.FreePageIDs = append(bt.Header.FreePageIDs, orphanedPageID)
	// remove separator from parent
	if err := parent.DeleteRecord(separatorIndex); err != nil {
		return err
	}

	// update parent pointer - after DeleteRecord slots shift down
	// what was Records[separatorIndex+1] is now Records[separatorIndex]
	/// this will need to change if we shift to a tombstone model
	if separatorIndex < int(parent.NumSlots) {
		oldKey, _ := pager.DeserializeInternalRecord(parent.Records[separatorIndex])
		parent.Records[separatorIndex] = pager.SerializeInternalRecord(oldKey, mergedNode.PageID)
	} else {
		// deleted the last record, just update rightmostchild
		parent.RightmostChild = mergedNode.PageID
	}

	return nil
}

func (bt *BTree) handleRootUnderflow(pageID pager.PageID) error {
	if pageID != bt.Header.RootPageID {
		return fmt.Errorf("rootpageid mismatch; actual: %d, expected: %d", pageID, bt.Header.RootPageID)
	}
	root, err := bt.loadNode(pageID)
	if err != nil {
		return err
	}
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

	bt.Header.RootPageID = root.RightmostChild
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

	underflowNode, err := bt.loadNode(pageID)
	if err != nil {
		return err
	}

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
		// we were writing stale headers. This will ensure that it's up to date before writing to the disk
		bt.Header.NumPages = uint32(bt.Header.NextPageID - 1)
		bt.dm.SetHeader(*bt.Header)
		bt.dm.WriteHeader()
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
	root, _ := bt.loadNode(bt.Header.RootPageID)
	depth := bt.GetDepth()
	return fmt.Sprintf("Root: page %d, Type: %v, NextPageID: %d, NumPages: %d, Tree Depth: %d",
		bt.Header.RootPageID, root.PageType, bt.Header.NextPageID, bt.Header.NumPages, depth)
}

func (bt *BTree) Vacuum() error {
	// update to bulk loading

	tempFile := bt.Header.Schema.TableName + ".db.tmp"
	f, err := os.Create(tempFile)
	if err != nil {
		return err
	}
	tempDM := pager.NewDiskManager(f)

	// Create fresh header with same schema but reset page IDs
	freshHeader := pager.DefaultTableHeader(bt.Header.Schema)
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
			if leaf.Slots[i].Offset == 0 {
				continue // skip deleted slots
			}
			record := leaf.Records[i]
			if len(record) < 8 {
				continue // skip malformed records
			}
			// Extract key from first 8 bytes
			key := binary.LittleEndian.Uint64(record[:8])
			if err := newTree.Insert(key, record); err != nil {
				// Skip duplicates (caused by stale pointers to freed pages)
				if err.Error() != fmt.Sprintf("key %d already exists", key) {
					return err
				}
			}
		}
		leafID = leaf.NextLeaf
	}

	// Update header with new tree's state and write it
	newTree.Header.NumPages = uint32(newTree.Header.NextPageID - 1)
	newTree.dm.SetHeader(*newTree.Header)
	err = newTree.dm.WriteHeader()
	if err != nil {
		return err
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}

	if err := bt.dm.Close(); err != nil {
		return err
	}

	origFile := bt.Header.Schema.TableName + ".db"
	os.Rename(tempFile, origFile)

	f, err = os.OpenFile(origFile, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	bt.dm.SetFile(f)
	if err := bt.dm.ReadHeader(); err != nil {
		return err
	}
	bt.Header = bt.dm.GetHeader()
	return nil
}
