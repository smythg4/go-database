package store

import (
	"encoding/binary"
	"fmt"
	"godb/internal/schema"
	"io"
	"os"
)

type TableStore struct {
	File       *os.File
	Schema     schema.Schema
	HeaderSize int64
}

func NewTableStore(filename string) (*TableStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	sch := schema.Schema{
		TableName: "users",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "name", Type: schema.StringType},
			{Name: "age", Type: schema.IntType},
		},
	}

	ts := &TableStore{File: file, Schema: sch}

	stat, _ := file.Stat()
	if stat.Size() == 0 {
		ts.WriteSchema()
	} else {
		ts.ReadSchema()
	}

	return ts, nil
}

func CreateTableStore(filename string, sch schema.Schema) (*TableStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	ts := &TableStore{
		File:   file,
		Schema: sch,
	}

	stat, _ := file.Stat()
	if stat.Size() == 0 {
		ts.WriteSchema()
	} else {
		return nil, fmt.Errorf("file already exists: %s", filename)
	}

	return ts, nil
}

func (ts *TableStore) Insert(record schema.Record) error {
	// seek to the end (past header and all records)
	if _, err := ts.File.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	// write each field in schema order
	for _, field := range ts.Schema.Fields {
		value, ok := record[field.Name]
		if !ok {
			return fmt.Errorf("missing field: %s", field.Name)
		}

		if err := writeValue(ts.File, field.Type, value); err != nil {
			return err
		}
	}

	return ts.File.Sync()
}

func (ts *TableStore) ReadSchema() (schema.Schema, error) {
	sch := schema.Schema{}
	if _, err := ts.File.Seek(0, io.SeekStart); err != nil {
		return schema.Schema{}, err
	}

	// read the name of the table
	name, err := readTableName(ts.File)
	if err != nil {
		return schema.Schema{}, err
	}
	sch.TableName = name

	// determine how many fields there are
	numFieldsBytes := make([]byte, 4)
	_, err = io.ReadFull(ts.File, numFieldsBytes)
	if err != nil {
		return schema.Schema{}, err
	}
	numFields := binary.LittleEndian.Uint32(numFieldsBytes)

	// read each field from the file header
	for i := 0; i < int(numFields); i++ {
		field, err := readField(ts.File)
		if err != nil {
			return schema.Schema{}, err
		}
		sch.Fields = append(sch.Fields, field)
	}

	ts.HeaderSize, _ = ts.File.Seek(0, io.SeekCurrent)
	ts.Schema = sch
	return sch, nil
}

func (ts *TableStore) WriteSchema() error {
	// write the table metadata
	// table name data
	if err := writeString(ts.File, ts.Schema.TableName); err != nil {
		return err
	}

	// write number of fields
	if err := writeUint32(ts.File, uint32(len(ts.Schema.Fields))); err != nil {
		return err
	}

	// write schema field breakdowns
	for _, field := range ts.Schema.Fields {
		// field name
		if err := writeString(ts.File, field.Name); err != nil {
			return err
		}
		// field type
		if _, err := ts.File.Write([]byte{byte(field.Type)}); err != nil {
			return err
		}
	}
	ts.HeaderSize, _ = ts.File.Seek(0, io.SeekCurrent)
	return nil
}

func (ts *TableStore) Find(id int) (schema.Record, error) {
	if _, err := ts.File.Seek(ts.HeaderSize, io.SeekStart); err != nil {
		return nil, err
	}

	var latestRecord schema.Record
	found := false

	for {
		record := make(schema.Record)

		for _, field := range ts.Schema.Fields {
			value, err := readValue(ts.File, field.Type)
			if err == io.EOF {
				if !found {
					return nil, fmt.Errorf("key not found: %d", id)
				}
				return latestRecord, nil
			}
			if err != nil {
				return nil, err
			}
			record[field.Name] = value
		}

		if record["id"].(int32) == int32(id) {
			latestRecord = record
			found = true
		}
	}
}

func (ts *TableStore) ScanAll() ([]schema.Record, error) {
	if _, err := ts.File.Seek(ts.HeaderSize, io.SeekStart); err != nil {
		return nil, err
	}

	recordMap := make(map[int32]schema.Record)

	for {
		record := make(schema.Record)

		// try to read all fields for this record
		for _, field := range ts.Schema.Fields {
			value, err := readValue(ts.File, field.Type)
			if err == io.EOF {
				// end of file - return what we have
				uniqueRecords := make([]schema.Record, 0, len(recordMap))
				for _, rec := range recordMap {
					uniqueRecords = append(uniqueRecords, rec)
				}
				return uniqueRecords, nil
			}
			if err != nil {
				return nil, fmt.Errorf("reading field %s: %w", field.Name, err)
			}
			record[field.Name] = value
		}

		id := record["id"].(int32)
		recordMap[id] = record
	}
}

func writeUint32(w io.Writer, v uint32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	_, err := w.Write(buf)
	return err
}

func writeString(w io.Writer, s string) error {
	if err := writeUint32(w, uint32(len(s))); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

func readTableName(r io.Reader) (string, error) {
	lenBytes := make([]byte, 4)
	_, err := io.ReadFull(r, lenBytes)
	if err != nil {
		return "", err
	}
	nameLength := binary.LittleEndian.Uint32(lenBytes)

	nameBytes := make([]byte, nameLength)
	_, err = io.ReadFull(r, nameBytes)
	if err != nil {
		return "", err
	}

	return string(nameBytes), nil
}

func readField(r io.Reader) (schema.Field, error) {
	lenBytes := make([]byte, 4)
	_, err := io.ReadFull(r, lenBytes)
	if err != nil {
		return schema.Field{}, err
	}
	nameLength := binary.LittleEndian.Uint32(lenBytes)

	nameBytes := make([]byte, nameLength)
	_, err = io.ReadFull(r, nameBytes)
	if err != nil {
		return schema.Field{}, err
	}
	fieldName := string(nameBytes)

	typeByte := make([]byte, 1)
	_, err = io.ReadFull(r, typeByte)
	if err != nil {
		return schema.Field{}, err
	}
	fieldType := schema.FieldType(typeByte[0])

	return schema.Field{
		Name: fieldName,
		Type: fieldType,
	}, nil
}

func writeValue(w io.Writer, fieldType schema.FieldType, value any) error {
	switch fieldType {
	case schema.IntType:
		v := value.(int32)
		return writeUint32(w, uint32(v))
	case schema.StringType:
		s := value.(string)
		return writeString(w, s)
	case schema.BoolType:
		b := value.(bool)
		if b {
			_, err := w.Write([]byte{1})
			return err
		} else {
			_, err := w.Write([]byte{0})
			return err
		}
	default:
		return fmt.Errorf("unsupported type: %v", fieldType)
	}
}

func readValue(r io.Reader, fieldType schema.FieldType) (any, error) {
	switch fieldType {
	case schema.IntType:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return int32(binary.LittleEndian.Uint32(buf)), nil
	case schema.StringType:
		return readTableName(r)
	case schema.BoolType:
		buf := make([]byte, 1)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf[0] != 0, nil
	default:
		return nil, fmt.Errorf("unsupported type: %v", fieldType)
	}
}
