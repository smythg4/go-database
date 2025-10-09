package pager

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const PAGE_SIZE = 4096

type PageID uint32
type PageType uint8

const (
	LEAF PageType = iota
	INTERNAL
)

type SlottedPage struct {
	PageID         PageID
	PageType       PageType
	NumSlots       uint16
	FreeSpacePtr   uint16
	RightmostChild PageID   // only used on INTERNAL pages
	NextLeaf       PageID   // used to navigate to neighbor leaf nodes
	Slots          []Slot   // pointers to data records
	Records        [][]byte // Raw record data
}

type Slot struct {
	Offset uint16
	Length uint16
}

// raw bytes - disk representation
type Page struct {
	PageID PageID
	Data   [PAGE_SIZE]byte
}

func (sp *SlottedPage) GetUsedSpace() uint16 {
	slotArraySize := 13 + (len(sp.Slots) * 4) // header + slot array
	dataSize := 0

	for _, slot := range sp.Slots {
		if slot.Offset > 0 { // skip tombstones
			dataSize += int(slot.Length)
		}
	}
	return uint16(slotArraySize + dataSize)
}

func (sp *SlottedPage) IsUnderfull() bool {
	// threshold used to determine if a merge is warranted
	return sp.GetUsedSpace() < PAGE_SIZE/2
}

func (sp *SlottedPage) Serialize() Page {
	var page Page
	page.PageID = sp.PageID

	// write header
	page.Data[0] = byte(sp.PageType)
	binary.LittleEndian.PutUint16(page.Data[1:3], sp.NumSlots)
	binary.LittleEndian.PutUint16(page.Data[3:5], sp.FreeSpacePtr)
	binary.LittleEndian.PutUint32(page.Data[5:9], uint32(sp.RightmostChild))
	binary.LittleEndian.PutUint32(page.Data[9:13], uint32(sp.NextLeaf))

	// write slot array
	for i, slot := range sp.Slots {
		offset := 13 + (i * 4)
		binary.LittleEndian.PutUint16(page.Data[offset:offset+2], slot.Offset)
		binary.LittleEndian.PutUint16(page.Data[offset+2:offset+4], slot.Length)
	}

	// write record data
	for i, record := range sp.Records {
		if len(record) > 0 {
			copy(page.Data[sp.Slots[i].Offset:], record)
		}
	}

	return page
}

func DeserializeSlottedPage(page Page) (*SlottedPage, error) {
	sp := &SlottedPage{
		PageID:         page.PageID,
		PageType:       PageType(page.Data[0]),
		NumSlots:       binary.LittleEndian.Uint16(page.Data[1:3]),
		FreeSpacePtr:   binary.LittleEndian.Uint16(page.Data[3:5]),
		RightmostChild: PageID(binary.LittleEndian.Uint32(page.Data[5:9])),
		NextLeaf:       PageID(binary.LittleEndian.Uint32(page.Data[9:13])),
	}

	// read slots
	sp.Slots = make([]Slot, sp.NumSlots)
	for i := 0; i < int(sp.NumSlots); i++ {
		offset := 13 + (i * 4)
		sp.Slots[i].Offset = binary.LittleEndian.Uint16(page.Data[offset : offset+2])
		sp.Slots[i].Length = binary.LittleEndian.Uint16(page.Data[offset+2 : offset+4])
	}

	// read records
	sp.Records = make([][]byte, sp.NumSlots)
	for i, slot := range sp.Slots {
		if slot.Offset > 0 && slot.Length > 0 {
			sp.Records[i] = make([]byte, slot.Length)
			copy(sp.Records[i], page.Data[slot.Offset:slot.Offset+slot.Length])
		}
	}
	return sp, nil
}

func NewSlottedPage(pageID PageID, pageType PageType) *SlottedPage {
	return &SlottedPage{
		PageID:       pageID,
		PageType:     pageType,
		NumSlots:     0,
		FreeSpacePtr: PAGE_SIZE,
		Slots:        []Slot{},
		Records:      [][]byte{},
	}
}

func (sp *SlottedPage) InsertRecordSorted(data []byte) (int, error) {
	if len(data) < 8 {
		return -1, errors.New("record too small to contain a key")
	}
	key := binary.LittleEndian.Uint64(data[:8])

	insertPos := sp.findInsertionPosition(key)

	slotArrayEnd := 13 + (len(sp.Slots)+1)*4
	newFreePtr := sp.FreeSpacePtr - uint16(len(data))

	if newFreePtr < uint16(slotArrayEnd) {
		return -1, errors.New("page full")
	}

	slot := Slot{
		Offset: newFreePtr,
		Length: uint16(len(data)),
	}

	sp.Slots = append(sp.Slots[:insertPos], append([]Slot{slot}, sp.Slots[insertPos:]...)...)
	sp.Records = append(sp.Records[:insertPos], append([][]byte{data}, sp.Records[insertPos:]...)...)

	sp.NumSlots++
	sp.FreeSpacePtr = newFreePtr
	return insertPos, nil
}

func (sp *SlottedPage) InsertRecord(data []byte) (int, error) {
	slotArrayEnd := 13 + (len(sp.Slots)+1)*4
	newFreePtr := sp.FreeSpacePtr - uint16(len(data))

	if newFreePtr < uint16(slotArrayEnd) {
		return -1, errors.New("page full")
	}

	slot := Slot{
		Offset: newFreePtr,
		Length: uint16(len(data)),
	}

	sp.Slots = append(sp.Slots, slot)
	sp.Records = append(sp.Records, data)

	sp.NumSlots++
	sp.FreeSpacePtr = newFreePtr

	return int(sp.NumSlots) - 1, nil
}

func (sp *SlottedPage) GetRecord(slotIndex int) ([]byte, error) {
	if slotIndex >= int(sp.NumSlots) {
		return nil, errors.New("slot out of range")
	}
	if sp.Slots[slotIndex].Offset == 0 {
		return nil, errors.New("record deleted")
	}

	return sp.Records[slotIndex], nil
}

func (sp *SlottedPage) DeleteRecord(slotIndex int) error {
	if slotIndex >= int(sp.NumSlots) {
		return errors.New("slot out of range")
	}
	if sp.Records[slotIndex] == nil {
		return errors.New("record already deleted")
	}

	sp.Slots[slotIndex].Offset = 0
	sp.Slots[slotIndex].Length = 0
	sp.Records[slotIndex] = nil
	sp.NumSlots--

	// this compact call is inefficient, but tombstone tracking was f**king me up!
	return sp.Compact()
}

func (sp *SlottedPage) GetKey(slotIndex int) uint64 {
	if slotIndex >= int(sp.NumSlots) {
		return 0
	}
	// first 8 bytes of record is the key
	return binary.LittleEndian.Uint64(sp.Records[slotIndex][:8])
}

func (sp *SlottedPage) findInsertionPosition(key uint64) int {
	left, right := 0, int(sp.NumSlots)
	for left < right {
		mid := (left + right) / 2

		if sp.GetKey(mid) < key {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

func (sp *SlottedPage) SearchInternal(key uint64) (PageID, int) {
	left, right := 0, int(sp.NumSlots)
	for left < right {
		mid := (left + right) / 2

		midKey := sp.GetKey(mid)
		if midKey < key {
			left = mid + 1
		} else {
			right = mid
		}
	}

	if left >= int(sp.NumSlots) {
		return sp.RightmostChild, -1
	}

	_, childPageID := DeserializeInternalRecord(sp.Records[left])
	return childPageID, left
}

func (sp *SlottedPage) Search(key uint64) (int, bool) {
	left, right := 0, int(sp.NumSlots)
	for left < right {
		mid := (left + right) / 2
		midKey := sp.GetKey(mid)
		if midKey == key {
			return mid, true
		} else if midKey < key {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return -1, false
}

func SerializeInternalRecord(key uint64, childPageID PageID) []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.LittleEndian, key)
	binary.Write(buf, binary.LittleEndian, childPageID)

	return buf.Bytes()
}

func DeserializeInternalRecord(data []byte) (uint64, PageID) {
	key := binary.LittleEndian.Uint64(data[:8])
	pgid := binary.LittleEndian.Uint32(data[8:])
	return key, PageID(pgid)
}

func (sp *SlottedPage) Compact() error {
	activeRecords := [][]byte{}

	for i, slot := range sp.Slots {
		if slot.Offset > 0 && slot.Length > 0 {
			activeRecords = append(activeRecords, sp.Records[i])
		}
	}

	// reset page to empty
	sp.Slots = []Slot{}
	sp.Records = [][]byte{}
	sp.NumSlots = 0
	sp.FreeSpacePtr = PAGE_SIZE

	// re-insert all active records
	for _, record := range activeRecords {
		_, err := sp.InsertRecord(record)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sp *SlottedPage) SplitLeaf(newPageID PageID) (*SlottedPage, uint64, error) {
	if sp.PageType != LEAF {
		return nil, 0, errors.New("attempting to split a non-leaf page as LEAF")
	}

	// Compact first to remove tombstones - NumSlots must equal len(Records)
	if err := sp.Compact(); err != nil {
		return nil, 0, err
	}

	mid := sp.NumSlots / 2
	newPage := NewSlottedPage(newPageID, sp.PageType)
	for i := mid; i < sp.NumSlots; i++ {
		record := sp.Records[i]
		_, err := newPage.InsertRecordSorted(record)
		if err != nil {
			return nil, 0, err
		}
	}
	// truncate and compact the original leaf node
	sp.Slots = sp.Slots[:mid]
	sp.Records = sp.Records[:mid]
	err := sp.Compact()
	if err != nil {
		return nil, 0, err
	}
	promotedKey := newPage.GetKey(0)

	// update sibling pointers
	newPage.NextLeaf = sp.NextLeaf // new "right node" points to the original node's neighbor
	sp.NextLeaf = newPageID        // new "left node" points to the new right node

	return newPage, promotedKey, nil
}

func (sp *SlottedPage) SplitInternal(newPageID PageID) (*SlottedPage, uint64, error) {
	if sp.PageType != INTERNAL {
		return nil, 0, errors.New("attempting to split a non-internal page as INTERNAL")
	}

	// Compact first to remove tombstones - NumSlots must equal len(Records)
	if err := sp.Compact(); err != nil {
		return nil, 0, err
	}

	mid := sp.NumSlots / 2
	promotedKey := sp.GetKey(int(mid))
	newPage := NewSlottedPage(newPageID, sp.PageType)
	for i := mid + 1; i < sp.NumSlots; i++ {
		record := sp.Records[i]
		_, err := newPage.InsertRecordSorted(record)
		if err != nil {
			return nil, 0, err
		}
	}

	newPage.RightmostChild = sp.RightmostChild

	_, childID := DeserializeInternalRecord(sp.Records[mid])
	sp.RightmostChild = childID

	sp.Slots = sp.Slots[:mid]
	sp.Records = sp.Records[:mid]
	sp.Compact()

	return newPage, promotedKey, nil
}

func (sp *SlottedPage) CanMergeWith(sibling *SlottedPage) bool {
	// check to see if two pages can be merged into a single  page
	leftSize := sp.GetUsedSpace()
	rightSize := sibling.GetUsedSpace()
	combinedSize := leftSize + rightSize

	// calc size needed for slot array
	combinedSlots := sp.NumSlots + sibling.NumSlots
	slotArraySize := 13 + (combinedSlots * 4) // header + slots

	return uint16(slotArraySize)+combinedSize <= PAGE_SIZE
}

func (sp *SlottedPage) MergeLeaf(sibling *SlottedPage) error {
	if sp.PageType != LEAF || sibling.PageType != LEAF {
		return errors.New("both pages must be leaf pages")
	}

	if !sp.CanMergeWith(sibling) {
		return errors.New("pages too large to merge")
	}

	// copy all records from sibling to this page
	for i := 0; i < int(sibling.NumSlots); i++ {
		_, err := sp.InsertRecordSorted(sibling.Records[i])
		if err != nil {
			return err
		}
	}

	// update sibling pointer chain
	sp.NextLeaf = sibling.NextLeaf

	return nil
}

func (sp *SlottedPage) MergeInternals(sibling *SlottedPage) error {
	if sp.PageType != INTERNAL || sibling.PageType != INTERNAL {
		return errors.New("all pages must be INTERNAL")
	}

	if !sp.CanMergeWith(sibling) {
		return errors.New("pages too large to merge")
	}

	// copy all records from sibling to this page
	for i := 0; i < int(sibling.NumSlots); i++ {
		_, err := sp.InsertRecordSorted(sibling.Records[i])
		if err != nil {
			return err
		}
	}
	// update the rightmost child to reflect the sibling's rightmostchild
	sp.RightmostChild = sibling.RightmostChild
	return nil
}
