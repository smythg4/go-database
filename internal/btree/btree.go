package btree

import (
	"errors"
	"fmt"
	"godb/internal/pager"
)

type BTree struct {
	dm     *pager.DiskManager
	header *pager.TableHeader
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
	currentPageID := bt.header.RootPageID
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
	newRootID := bt.header.NextPageID
	bt.header.NextPageID++
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
	bt.header.RootPageID = newRootID

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
		rightNode, newPromotedKey, err := parent.splitNode(bt.header.NextPageID)
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
		bt.header.NextPageID++

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
	defer bt.dm.WriteHeader()

	// traverse to leaf, collecting breadcrumbs
	leafPageID, err := bt.findLeaf(key, breadcrumbs)
	if err != nil {
		return err
	}

	leaf, err := bt.loadNode(leafPageID)
	if err != nil {
		return err
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
	rightNode, promotedKey, err := leaf.splitNode(bt.header.NextPageID)
	if err != nil {
		return err
	}
	bt.header.NextPageID++

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

	currentPageID := bt.header.RootPageID

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
	for leafPageID != 0 { // 0 = end of the line
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
	// this is incomplete! We're only scanning a single page. We need sibling pointers to neighbor leaves
	return results, nil
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
