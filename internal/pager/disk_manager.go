package pager

import "os"

type DiskManager struct {
	file   *os.File
	header TableHeader
}

func (dm *DiskManager) SetFile(file *os.File) {
	dm.file = file
}

func (dm *DiskManager) SetHeader(h TableHeader) {
	dm.header = h
}

func (dm *DiskManager) ReadHeader() error {
	data := make([]byte, PAGE_SIZE)
	_, err := dm.file.ReadAt(data, 0)
	if err != nil {
		return err
	}
	th, err := DeserializeTableHeader(data)
	if err != nil {
		return err
	}
	dm.header = *th
	return nil
}

func (dm *DiskManager) WriteHeader() error {
	data, err := dm.header.Serialize()
	if err != nil {
		return err
	}
	padded := make([]byte, PAGE_SIZE)
	copy(padded, data)

	_, err = dm.file.WriteAt(padded, 0)
	return err
}

func (dm *DiskManager) ReadPage(pageID PageID) (Page, error) {
	offset := int64(pageID) * PAGE_SIZE
	data := make([]byte, PAGE_SIZE)

	_, err := dm.file.ReadAt(data, offset)
	if err != nil {
		return Page{}, err
	}

	return Page{PageID: pageID, Data: [PAGE_SIZE]byte(data)}, nil
}

func (dm *DiskManager) WritePage(page Page) error {
	offset := int64(page.PageID) * PAGE_SIZE
	_, err := dm.file.WriteAt(page.Data[:], offset)
	return err
}

func (dm *DiskManager) ReadSlottedPage(pageID PageID) (*SlottedPage, error) {
	page, err := dm.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	return DeserializeSlottedPage(page)
}

func (dm *DiskManager) WriteSlottedPage(sp *SlottedPage) error {
	page := sp.Serialize()
	return dm.WritePage(page)
}
