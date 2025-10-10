package btree

import (
	"errors"
	"godb/internal/pager"
)

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
