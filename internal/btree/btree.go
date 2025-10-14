package btree

import (
	"errors"
	"fmt"
	"godb/internal/pager"
	"godb/internal/schema"
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

func (bt *BTree) findLeaf(key uint64, breadcrumbs *BTStack) (pager.PageID, error) {
	currentPageID := bt.pc.GetRootPageID()
	for {
		node, err := bt.loadNode(currentPageID)
		if err != nil {
			return 0, fmt.Errorf("failed to load page %d: %w", currentPageID, err)
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

func (bt *BTree) propogateSplit(promotedKey uint64, rightPageID, leftPageID pager.PageID, breadcrumbs *BTStack, sequential bool) error {
	// leftPageID and rightPageID represent the two children from the most recent split
	// promotedKey is the separator between them

	for !breadcrumbs.isEmpty() {
		bc, _ := breadcrumbs.pop()
		parent, err := bt.loadNode(bc.PageID)
		if err != nil {
			return fmt.Errorf("failed to load page %d: %w", bc.PageID, err)
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

		if !errors.Is(err, pager.ErrPageFull) {
			// something went wrong other than a full page
			return err
		}

		// page must have been full, now we split
		nextPage := bt.allocatePage()
		rightNode, newPromotedKey, err := parent.splitNode(nextPage, sequential) // never consider internals sequential
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
		return fmt.Errorf("failed to load page %d: %w", leafPageID, err)
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

	if !errors.Is(err, pager.ErrPageFull) {
		// bad path
		return err
	}

	// page was full, time to split
	nextPage := bt.allocatePage()

	// check to see if insertion is sequential
	sequential := len(leaf.Records) > 0 && key > leaf.GetKey(int(leaf.NumSlots)-1)

	rightNode, promotedKey, err := leaf.splitNode(nextPage, sequential)
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

	return bt.propogateSplit(promotedKey, rightNode.PageID, leafPageID, breadcrumbs, sequential)
}

func (bt *BTree) Search(key uint64) ([]byte, bool, error) {
	// safety net
	maxDepth := 100

	currentPageID := bt.pc.GetRootPageID()

	// traverse down to leaf
	for depth := 0; depth < maxDepth; depth++ {
		node, err := bt.loadNode(currentPageID)
		if err != nil {
			return nil, false, fmt.Errorf("failed to load page %d: %w", currentPageID, err)
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
	leafPageID, err := bt.findLeaf(startKey, &BTStack{})
	if err != nil {
		return nil, err
	}

	var results [][]byte
	visited := make(map[pager.PageID]bool) // cycle detection

	for leafPageID != 0 { // 0 = end of the line
		// Check for cycles
		if visited[leafPageID] {
			// Build chain for debugging
			chain := "Cycle: "
			current, _ := bt.findLeaf(startKey, &BTStack{})
			for i := 0; i < 20 && current != 0; i++ {
				n, _ := bt.loadNode(current) // swallows errors, but this an error case anyway
				chain += fmt.Sprintf("%d->", current)
				current = n.NextLeaf
				bt.pc.UnPin(n.PageID)
			}
			return nil, fmt.Errorf("cycle detected in leaf chain at page %d. Chain: %s", leafPageID, chain)
		}
		visited[leafPageID] = true

		leaf, err := bt.loadNode(leafPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to load page %d: %w", leafPageID, err)
		}

		for i := 0; i < int(leaf.NumSlots); i++ {
			key := leaf.GetKey(i)
			if key >= startKey && key <= endKey {
				data, _ := leaf.GetRecord(i)
				results = append(results, data)
			} else if key > endKey {
				bt.pc.UnPin(leafPageID)
				return results, nil
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

func (bt *BTree) mergeInternalNodes(leftNode, rightNode, parent *BNode, separatorIndex int) error {
	// true = we get merged into left, false = right merged into us
	if leftNode.IsLeaf() || rightNode.IsLeaf() || parent.IsLeaf() {
		return errors.New("both siblings and parent must be INTERNAL nodes, not leaves")
	}

	// get the key to demote
	separatorKey, _ := pager.DeserializeInternalRecord(parent.Records[separatorIndex])

	// demote: insert separator into the left, pointing to the left's rightmostchild
	demotedRecord := pager.SerializeInternalRecord(separatorKey, leftNode.RightmostChild)
	_, err := leftNode.InsertRecordSorted(demotedRecord)
	if err != nil {
		return fmt.Errorf("failed to insert demoted record into left node: %w", err)
	}

	// merge right into left
	err = leftNode.MergeInternals(rightNode.SlottedPage)
	if err != nil {
		return fmt.Errorf("failed to merge internal nodes %d and %d: %w", leftNode.PageID, rightNode.PageID, err)
	}

	// write merged node
	if err := bt.writeNode(leftNode); err != nil {
		return fmt.Errorf("failed to write page %d: %w", leftNode.PageID, err)
	}

	bt.pc.FreePage(rightNode.PageID)

	// remove separator from parent
	if err := parent.DeleteRecord(separatorIndex); err != nil {
		return fmt.Errorf("failed to delete parent record at %d: %w", separatorIndex, err)
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

func (bt *BTree) mergeLeafNodes(leftNode, rightNode, parent *BNode, separatorIndex int) error {
	// true = we get merged into left, false = right merged into us
	if !leftNode.IsLeaf() || !rightNode.IsLeaf() || parent.IsLeaf() {
		return errors.New("both siblings must be LEAF, parent must be INTERNAL")
	}

	// always merge right into left
	if err := leftNode.MergeLeaf(rightNode.SlottedPage); err != nil {
		return fmt.Errorf("failed to merge leaf nodes %d and %d: %w", leftNode.PageID, rightNode.PageID, err)
	}

	// write merged (left) node
	if err := bt.writeNode(leftNode); err != nil {
		return fmt.Errorf("failed to write page %d: %w", leftNode.PageID, err)
	}

	// right node is always orphaned
	bt.pc.FreePage(rightNode.PageID)

	// remove separator from parent
	if err := parent.DeleteRecord(separatorIndex); err != nil {
		return fmt.Errorf("failed to delete parent record at %d: %w", separatorIndex, err)
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
		return fmt.Errorf("failed to load page %d: %w", pageID, err)
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
		return fmt.Errorf("failed to load page %d: %w", bc.PageID, err)
	}
	defer bt.pc.UnPin(parent.PageID)

	underflowNode, err := bt.loadNode(pageID)
	if err != nil {
		return fmt.Errorf("failed to load page %d: %w", pageID, err)
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
		return fmt.Errorf("failed to load page %d: %w", siblingID, err)
	}
	defer bt.pc.UnPin(sibling.PageID)

	var leftNode, rightNode *BNode
	if mergeIntoSibling {
		// found left sibling - left is healthy, right is underfull
		leftNode = sibling
		rightNode = underflowNode
	} else {
		// found right sibling - left is underfull, right is healthy
		leftNode = underflowNode
		rightNode = sibling
	}

	// try to borrow from the left node first, otherwise try to borrow from the right node
	if leftNode.CanLendKeys() {
		if leftNode.IsLeaf() {
			return bt.borrowFromLeftLeaf(leftNode, rightNode, parent, separatorIndex)
		} else {
			return bt.borrowFromLeftInternal(leftNode, rightNode, parent, separatorIndex)
		}
	} else if rightNode.CanLendKeys() {
		if leftNode.IsLeaf() {
			return bt.borrowFromRightLeaf(leftNode, rightNode, parent, separatorIndex)
		} else {
			return bt.borrowFromRightInternal(leftNode, rightNode, parent, separatorIndex)
		}
	}

	// check if merge is possible (skip if too large)
	if !leftNode.CanMergeWith(rightNode.SlottedPage) {
		return nil // no merge occurs
	}

	// perform merge based on node type
	if leftNode.IsLeaf() {
		err = bt.mergeLeafNodes(leftNode, rightNode, parent, separatorIndex)
	} else {
		err = bt.mergeInternalNodes(leftNode, rightNode, parent, separatorIndex)
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
		return fmt.Errorf("failed to load page %d: %w", leafPageID, err)
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
		return fmt.Errorf("delete: failed to write page %d: %w", leaf.PageID, err)
	}

	// check if nodes need to merge
	if leaf.IsUnderfull() {
		return bt.handleUnderflow(leafPageID, breadcrumbs)
	}
	return bt.writeNode(leaf)
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
	pages, rootID, err := bt.BulkLoad()
	if err != nil {
		return err
	}
	return bt.pc.ReplaceTreeFromPages(pages, rootID)
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

func (bt *BTree) buildLeafLayer() ([]*pager.SlottedPage, error) {
	// find the left most leaf node to start scan
	oldLeftLeaf, err := bt.findLeaf(0, &BTStack{})
	if err != nil {
		return nil, err
	}

	// build out a leaf slice and initialize a first page
	leaves := []*pager.SlottedPage{}
	newLeafIndex := pager.PageID(1)
	newLeaf := pager.NewSlottedPage(newLeafIndex, pager.LEAF)

	// follow leaf links to scan left to right
	currPageID := oldLeftLeaf
	for currPageID != 0 { // 0 means no sibling to the right, we're done
		currentLeaf, err := bt.loadNode(currPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to load page %d: %w", currPageID, err)
		}
		// iterate over leaf records
		for i := 0; i < int(currentLeaf.NumSlots); i++ {
			record, err := currentLeaf.GetRecord(i)
			if err != nil {
				return nil, fmt.Errorf("failed to get record %d from currentLeaf: %w", i, err)
			}
			// insert record into the newly created leaf node
			_, err = newLeaf.InsertRecordSorted(record)
			if err != nil && errors.Is(err, pager.ErrPageFull) {
				// if the page was full, add it to the return slice, allocate a new leaf node
				// and insert into that one
				leaves = append(leaves, newLeaf)
				newLeafIndex++
				newLeaf = pager.NewSlottedPage(pager.PageID(newLeafIndex), pager.LEAF)
				_, err = newLeaf.InsertRecordSorted(record)
				if err != nil {
					return nil, fmt.Errorf("failed to insert record from page %d into newLeaf (after page full): %w", currentLeaf.PageID, err)
				}
			} else if err != nil {
				return nil, fmt.Errorf("failed to insert record from page %d into newLeaf: %w", currentLeaf.PageID, err)
			}
		}
		bt.pc.UnPin(currentLeaf.PageID)
		// move to next leaf node in original tree
		currPageID = currentLeaf.NextLeaf
	}

	// Don't forget the last leaf!
	if newLeaf.NumSlots > 0 {
		leaves = append(leaves, newLeaf)
	}

	// link the leaves
	for i := 0; i < len(leaves)-1; i++ {
		leaves[i].NextLeaf = leaves[i+1].PageID
	}
	if len(leaves) > 0 {
		leaves[len(leaves)-1].NextLeaf = 0
	}
	return leaves, nil
}

func buildInternalLayer(children []*pager.SlottedPage) ([]*pager.SlottedPage, error) {
	if len(children) == 1 {
		// this is the root node
		return children, nil
	}
	nextPageID := pager.PageID(1)
	if len(children) > 0 {
		nextPageID = children[len(children)-1].PageID + 1 // next available page number
	}

	parents := []*pager.SlottedPage{}
	currentParent := pager.NewSlottedPage(nextPageID, pager.INTERNAL)

	for i := 0; i < len(children)-1; i++ {
		separatorKey := children[i+1].GetKey(0) // first key of right child
		childPageID := children[i].PageID

		record := pager.SerializeInternalRecord(separatorKey, childPageID)
		_, err := currentParent.InsertRecordSorted(record)

		if err != nil && errors.Is(err, pager.ErrPageFull) {
			parents = append(parents, currentParent)
			nextPageID++
			currentParent = pager.NewSlottedPage(nextPageID, pager.INTERNAL)
			_, err := currentParent.InsertRecordSorted(record)
			if err != nil {
				return nil, fmt.Errorf("failed to insert record from page %d into currentParent (after page full): %w", childPageID, err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("failed to insert record from page %d into currentParent: %w", childPageID, err)
		}
	}

	// last child becomes RightmostChild of parent
	currentParent.RightmostChild = children[len(children)-1].PageID
	parents = append(parents, currentParent)

	return parents, nil
}

func (bt *BTree) BulkLoad() ([]*pager.SlottedPage, pager.PageID, error) {
	// phase 1: build leaves
	leaves, err := bt.buildLeafLayer()
	if err != nil {
		return nil, 0, err
	}

	// Track all pages to write
	allPages := []*pager.SlottedPage{}
	allPages = append(allPages, leaves...) // add all the leaves

	// phase 2: build internal layers recursively
	currentLayer := leaves
	for len(currentLayer) > 1 {
		currentLayer, err = buildInternalLayer(currentLayer)
		if err != nil {
			return nil, 0, err
		}
		allPages = append(allPages, currentLayer...) // add each layer
	}

	root := currentLayer[0]

	return allPages, root.PageID, nil
}

func (bt *BTree) GetWalMetadata() (rootPageID, nextPageID uint32) {
	h := bt.pc.GetHeader()
	return uint32(h.RootPageID), uint32(h.NextPageID)
}

func (bt *BTree) Checkpoint() error {
	return bt.pc.FlushAll()
}

func (bt *BTree) borrowFromRightLeaf(leftNode, rightNode, parent *BNode, separatorIndex int) error {
	if !leftNode.IsLeaf() || !rightNode.IsLeaf() || parent.IsLeaf() {
		return errors.New("both siblings must be LEAF, parent must be INTERNAL")
	}

	// 1. get first record from right sibling
	if rightNode.NumSlots == 0 {
		return fmt.Errorf("right sibling has no records to lend")
	}
	borrowedRecord := rightNode.Records[0]

	// 2. remove from right leaf
	if err := rightNode.DeleteRecord(0); err != nil {
		return err
	}

	// 3. Insert into left
	if _, err := leftNode.InsertRecordSorted(borrowedRecord); err != nil {
		return err
	}

	// 4. Update the parent separator
	if rightNode.NumSlots == 0 {
		return fmt.Errorf("right sibling empty after borrowing")
	}

	// considers shift from compact on delete
	newSeparatorKey := rightNode.GetKey(0) // right's new first key (was second key)
	parent.Records[separatorIndex] = pager.SerializeInternalRecord(newSeparatorKey, leftNode.PageID)

	// 5. write all three modified pages
	if err := bt.writeNode(leftNode); err != nil {
		return fmt.Errorf("borrow: failed to write leftNode page %d: %w", leftNode.PageID, err)
	}
	if err := bt.writeNode(rightNode); err != nil {
		return fmt.Errorf("borrow: failed to write rightNode page %d: %w", rightNode.PageID, err)
	}
	if err := bt.writeNode(parent); err != nil {
		return fmt.Errorf("borrow: failed to write parent page %d: %w", parent.PageID, err)
	}

	return nil
}

func (bt *BTree) borrowFromRightInternal(leftNode, rightNode, parent *BNode, separatorIndex int) error {
	if leftNode.IsLeaf() || rightNode.IsLeaf() || parent.IsLeaf() {
		return errors.New("both siblings and parent must be INTERNAL nodes, not leaves")
	}

	// get the key to demote
	separatorKey, _ := pager.DeserializeInternalRecord(parent.Records[separatorIndex])

	// 1. demote: insert separator into the left, pointing to the left's rightmostchild
	demotedRecord := pager.SerializeInternalRecord(separatorKey, leftNode.RightmostChild)
	_, err := leftNode.InsertRecordSorted(demotedRecord)
	if err != nil {
		return err
	}

	// 2. Get first record from right
	if rightNode.NumSlots == 0 {
		return fmt.Errorf("right sibling has no records to lend")
	}
	_, borrowedChild := pager.DeserializeInternalRecord(rightNode.Records[0])

	// 3. move child pointer from right to left
	leftNode.RightmostChild = borrowedChild

	// 4. remove borrow record from right
	if err := rightNode.DeleteRecord(0); err != nil {
		return err
	}

	// 5. promote right's new first key to parent (after delete index 0 is now old index 1 from compact after delete)
	if rightNode.NumSlots == 0 {
		return fmt.Errorf("right sibling empty after borrow")
	}

	newSeparatorKey := rightNode.GetKey(0)
	parent.Records[separatorIndex] = pager.SerializeInternalRecord(newSeparatorKey, leftNode.PageID)

	// 6. write all three modified pages
	if err := bt.writeNode(leftNode); err != nil {
		return fmt.Errorf("borrow: failed to write leftNode page %d: %w", leftNode.PageID, err)
	}
	if err := bt.writeNode(rightNode); err != nil {
		return fmt.Errorf("borrow: failed to write rightNode page %d: %w", rightNode.PageID, err)
	}
	if err := bt.writeNode(parent); err != nil {
		return fmt.Errorf("borrow: failed to write parent page %d: %w", parent.PageID, err)
	}

	return nil
}

func (bt *BTree) borrowFromLeftLeaf(leftNode, rightNode, parent *BNode, separatorIndex int) error {
	if !leftNode.IsLeaf() || !rightNode.IsLeaf() || parent.IsLeaf() {
		return errors.New("both siblings must be LEAF, parent must be INTERNAL")
	}

	// 1. get last record from left sibling
	if leftNode.NumSlots == 0 {
		return fmt.Errorf("left sibling has no records to lend")
	}
	borrowedRecord := leftNode.Records[len(leftNode.Records)-1]

	// 2. remove from left leaf
	if err := leftNode.DeleteRecord(len(leftNode.Records) - 1); err != nil {
		return err
	}

	// 3. Insert into right
	if _, err := rightNode.InsertRecordSorted(borrowedRecord); err != nil {
		return err
	}

	// 4. Update the parent separator
	if rightNode.NumSlots == 0 {
		return fmt.Errorf("right sibling empty after borrowing")
	}

	// considers shift from compact on delete
	newSeparatorKey := rightNode.GetKey(0) // right's new first key
	parent.Records[separatorIndex] = pager.SerializeInternalRecord(newSeparatorKey, leftNode.PageID)

	// 5. write all three modified pages
	if err := bt.writeNode(leftNode); err != nil {
		return fmt.Errorf("borrow: failed to write leftNode page %d: %w", leftNode.PageID, err)
	}
	if err := bt.writeNode(rightNode); err != nil {
		return fmt.Errorf("borrow: failed to write rightNode page %d: %w", rightNode.PageID, err)
	}
	if err := bt.writeNode(parent); err != nil {
		return fmt.Errorf("borrow: failed to write parent page %d: %w", parent.PageID, err)
	}

	return nil
}

func (bt *BTree) borrowFromLeftInternal(leftNode, rightNode, parent *BNode, separatorIndex int) error {
	if leftNode.IsLeaf() || rightNode.IsLeaf() || parent.IsLeaf() {
		return errors.New("both siblings and parent must be INTERNAL")
	}

	// get the key to demote
	separatorKey, _ := pager.DeserializeInternalRecord(parent.Records[separatorIndex])

	// get key and child from left's last record (before deletion)
	if leftNode.NumSlots == 0 {
		return fmt.Errorf("left sibling has no records to lend")
	}
	lastKey, lastChild := pager.DeserializeInternalRecord(leftNode.Records[len(leftNode.Records)-1])

	// 1. Demote separator into right, pointing to left's RightmostChild
	demotedRecord := pager.SerializeInternalRecord(separatorKey, leftNode.RightmostChild)
	if _, err := rightNode.InsertRecordSorted(demotedRecord); err != nil {
		return err
	}

	// 2. Delete left's last record
	if err := leftNode.DeleteRecord(len(leftNode.Records) - 1); err != nil {
		return err
	}

	// 3. Left's new RightmostChild is the child from the deleted record
	leftNode.RightmostChild = lastChild

	// 4. Promote the deleted key to parent
	parent.Records[separatorIndex] = pager.SerializeInternalRecord(lastKey, leftNode.PageID)

	// 5. write all three modified pages
	if err := bt.writeNode(leftNode); err != nil {
		return fmt.Errorf("borrow: failed to write leftNode page %d: %w", leftNode.PageID, err)
	}
	if err := bt.writeNode(rightNode); err != nil {
		return fmt.Errorf("borrow: failed to write rightNode page %d: %w", rightNode.PageID, err)
	}
	if err := bt.writeNode(parent); err != nil {
		return fmt.Errorf("borrow: failed to write parent page %d: %w", parent.PageID, err)
	}

	return nil
}
