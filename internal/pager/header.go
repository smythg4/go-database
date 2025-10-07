package pager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"godb/internal/schema"
)

type TableHeader struct {
	Magic       [4]byte // "GDBT"
	Version     uint16
	RootPageID  PageID
	NextPageID  PageID
	NumPages    uint32
	Schema      schema.Schema
	FreePageIDs []PageID
}

func DefaultTableHeader(sch schema.Schema) TableHeader {
	return TableHeader{
		Magic:      [4]byte{'G', 'D', 'B', 'T'},
		Version:    1,
		RootPageID: 1,
		NextPageID: 2,
		NumPages:   1,
		Schema:     sch,
	}
}

func (th *TableHeader) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	// magic number
	_, err := buf.Write([]byte("GDBT"))
	if err != nil {
		return nil, err
	}

	// version
	err = binary.Write(buf, binary.LittleEndian, th.Version)
	if err != nil {
		return nil, err
	}

	// root page
	err = binary.Write(buf, binary.LittleEndian, th.RootPageID)
	if err != nil {
		return nil, err
	}

	// next page
	err = binary.Write(buf, binary.LittleEndian, th.NextPageID)
	if err != nil {
		return nil, err
	}

	// num pages
	err = binary.Write(buf, binary.LittleEndian, th.NumPages)
	if err != nil {
		return nil, err
	}

	// write schema
	schBytes, err := th.Schema.Serialize()
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(schBytes)
	if err != nil {
		return nil, err
	}

	// write free page list
	freePageLen := uint32(len(th.FreePageIDs))
	err = binary.Write(buf, binary.LittleEndian, freePageLen)
	if err != nil {
		return nil, err
	}
	for _, pageID := range th.FreePageIDs {
		err = binary.Write(buf, binary.LittleEndian, pageID)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func DeserializeTableHeader(data []byte) (*TableHeader, error) {
	r := bytes.NewReader(data)
	th := &TableHeader{}

	// read magic
	_, err := r.Read(th.Magic[:])
	if err != nil {
		return nil, err
	}
	if string(th.Magic[:]) != "GDBT" {
		return nil, errors.New("invalid magic number")
	}

	// read version
	err = binary.Read(r, binary.LittleEndian, &th.Version)
	if err != nil {
		return nil, err
	}

	// read root page id
	err = binary.Read(r, binary.LittleEndian, &th.RootPageID)
	if err != nil {
		return nil, err
	}

	// read next page id
	err = binary.Read(r, binary.LittleEndian, &th.NextPageID)
	if err != nil {
		return nil, err
	}

	// read num pages
	err = binary.Read(r, binary.LittleEndian, &th.NumPages)
	if err != nil {
		return nil, err
	}

	// read schema
	th.Schema, err = schema.Deserialize(r)
	if err != nil {
		return nil, err
	}

	// read free page list
	var freePageLen uint32
	var pageID PageID
	err = binary.Read(r, binary.LittleEndian, &freePageLen)
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(freePageLen); i++ {
		err = binary.Read(r, binary.LittleEndian, &pageID)
		if err != nil {
			return nil, err
		}
		th.FreePageIDs = append(th.FreePageIDs, pageID)
	}
	return th, nil
}
