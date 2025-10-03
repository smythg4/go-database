package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type TableStore struct {
	file       *os.File
	schema     Schema
	headerSize int64
}

func NewTableStore(filename string) (*TableStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	schema := Schema{
		TableName: "users",
		Fields: []Field{
			{Name: "id", Type: IntType},
			{Name: "name", Type: StringType},
			{Name: "age", Type: IntType},
		},
	}

	ts := &TableStore{file: file, schema: schema}

	stat, _ := file.Stat()
	if stat.Size() == 0 {
		ts.WriteSchema()
	} else {
		ts.ReadSchema()
	}

	return ts, nil
}

func (ts *TableStore) Insert(record Record) error {
	// seek to the end (past header and all records)
	if _, err := ts.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	// write each field in schema order
	for _, field := range ts.schema.Fields {
		value, ok := record[field.Name]
		if !ok {
			return fmt.Errorf("missing field: %s", field.Name)
		}

		if err := writeValue(ts.file, field.Type, value); err != nil {
			return err
		}
	}

	return ts.file.Sync()
}

func (ts *TableStore) ReadSchema() (Schema, error) {
	schema := Schema{}
	if _, err := ts.file.Seek(0, io.SeekStart); err != nil {
		return Schema{}, err
	}

	// read the name of the table
	name, err := readTableName(ts.file)
	if err != nil {
		return Schema{}, err
	}
	schema.TableName = name

	// determine how many fields there are
	numFieldsBytes := make([]byte, 4)
	_, err = io.ReadFull(ts.file, numFieldsBytes)
	if err != nil {
		return Schema{}, err
	}
	numFields := binary.LittleEndian.Uint32(numFieldsBytes)

	// read each field from the file header
	for i := 0; i < int(numFields); i++ {
		field, err := readField(ts.file)
		if err != nil {
			return Schema{}, err
		}
		schema.Fields = append(schema.Fields, field)
	}

	ts.headerSize, _ = ts.file.Seek(0, io.SeekCurrent)
	ts.schema = schema
	return schema, nil
}

func (ts *TableStore) WriteSchema() error {
	// write the table metadata
	// table name data
	if err := writeString(ts.file, ts.schema.TableName); err != nil {
		return err
	}

	// write number of fields
	if err := writeUint32(ts.file, uint32(len(ts.schema.Fields))); err != nil {
		return err
	}

	// write schema field breakdowns
	for _, field := range ts.schema.Fields {
		// field name
		if err := writeString(ts.file, field.Name); err != nil {
			return err
		}
		// field type
		if _, err := ts.file.Write([]byte{byte(field.Type)}); err != nil {
			return err
		}
	}
	ts.headerSize, _ = ts.file.Seek(0, io.SeekCurrent)
	return nil
}

func (ts *TableStore) ScanAll() ([]Record, error) {
	if _, err := ts.file.Seek(ts.headerSize, io.SeekStart); err != nil {
		return nil, err
	}

	var records []Record
	for {
		record := make(Record)

		for _, field := range ts.schema.Fields {
			value, err := readValue(ts.file, field.Type)
			if err == io.EOF {
				return records, nil
			}
			if err != nil {
				return nil, fmt.Errorf("reading field %s: %w", field.Name, err)
			}
			record[field.Name] = value
		}

		records = append(records, record)
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

func readField(r io.Reader) (Field, error) {
	lenBytes := make([]byte, 4)
	_, err := io.ReadFull(r, lenBytes)
	if err != nil {
		return Field{}, err
	}
	nameLength := binary.LittleEndian.Uint32(lenBytes)

	nameBytes := make([]byte, nameLength)
	_, err = io.ReadFull(r, nameBytes)
	if err != nil {
		return Field{}, err
	}
	fieldName := string(nameBytes)

	typeByte := make([]byte, 1)
	_, err = io.ReadFull(r, typeByte)
	if err != nil {
		return Field{}, err
	}
	fieldType := FieldType(typeByte[0])

	return Field{
		Name: fieldName,
		Type: fieldType,
	}, nil
}

func writeValue(w io.Writer, fieldType FieldType, value any) error {
	switch fieldType {
	case IntType:
		v := value.(int32)
		return writeUint32(w, uint32(v))
	case StringType:
		s := value.(string)
		return writeString(w, s)
	case BoolType:
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

func readValue(r io.Reader, fieldType FieldType) (any, error) {
	switch fieldType {
	case IntType:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return int32(binary.LittleEndian.Uint32(buf)), nil
	case StringType:
		return readTableName(r)
	case BoolType:
		buf := make([]byte, 1)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf[0] != 0, nil
	default:
		return nil, fmt.Errorf("unsupported type: %v", fieldType)
	}
}
