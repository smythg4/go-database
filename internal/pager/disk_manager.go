package pager

import (
	"fmt"
	"os"
)

type DiskManager struct {
	file   *os.File
	header TableHeader
}

func NewDiskManager(file *os.File) DiskManager {
	return DiskManager{
		file: file,
	}
}

func (dm *DiskManager) Close() error {
	return dm.file.Close()
}

func (dm *DiskManager) SetFile(file *os.File) {
	dm.file = file
}

func (dm *DiskManager) SetHeader(h TableHeader) {
	dm.header = h
}

func (dm *DiskManager) GetHeader() *TableHeader {
	return &dm.header
}

func (dm *DiskManager) ReadHeader() error {
	data := make([]byte, PAGE_SIZE)
	_, err := dm.file.ReadAt(data, 0)
	if err != nil {
		return fmt.Errorf("failed to read header from disk: %w", err)
	}
	th, err := DeserializeTableHeader(data)
	if err != nil {
		return fmt.Errorf("failed to deserialize header: %w", err)
	}
	dm.header = *th
	return nil
}

func (dm *DiskManager) WriteHeader() error {
	data, err := dm.header.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize header: %w", err)
	}
	padded := make([]byte, PAGE_SIZE)
	copy(padded, data)

	_, err = dm.file.WriteAt(padded, 0)
	if err != nil {
		return fmt.Errorf("failed to write header to disk: %w", err)
	}
	// ensure write to disk is completed
	return dm.file.Sync()
}

func (dm *DiskManager) ReadPage(pageID PageID) (Page, error) {
	offset := int64(pageID) * PAGE_SIZE
	data := make([]byte, PAGE_SIZE)

	_, err := dm.file.ReadAt(data, offset)
	if err != nil {
		return Page{}, fmt.Errorf("failed to read page from disk (offset=%d): %w", offset, err)
	}

	return Page{PageID: pageID, Data: [PAGE_SIZE]byte(data)}, nil
}

func (dm *DiskManager) WritePage(page Page) error {
	offset := int64(page.PageID) * PAGE_SIZE
	_, err := dm.file.WriteAt(page.Data[:], offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.PageID, err)
	}
	return nil
}

func (dm *DiskManager) ReadSlottedPage(pageID PageID) (*SlottedPage, error) {
	page, err := dm.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	return DeserializeSlottedPage(page)
}

func (dm *DiskManager) WriteSlottedPage(sp *SlottedPage) error {
	page := sp.Serialize()
	return dm.WritePage(page)
}

func (dm *DiskManager) Sync() error {
	return dm.file.Sync()
}
