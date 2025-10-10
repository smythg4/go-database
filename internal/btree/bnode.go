package btree

import (
	"fmt"
	"godb/internal/pager"
)

type BNode struct {
	*pager.SlottedPage
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

func (bn *BNode) IsLeaf() bool {
	return bn.PageType == pager.LEAF
}
