package godatabase

import (
	"bytes"
	"encoding/binary"
	"errors"
	"godb/internal/schema"
)

type TableHeader struct {
	Magic      [4]byte // "GDBT"
	Version    uint16
	RootPageID PageID
	NextPageID PageID
	NumPages   uint32
	Schema     schema.Schema
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

	return th, nil
}
