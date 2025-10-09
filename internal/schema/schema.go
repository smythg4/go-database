package schema

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"godb/internal/encoding"
	"io"
	"strconv"
	"time"
)

type FieldType int

const (
	IntType FieldType = iota
	StringType
	BoolType
	FloatType
	DateType
)

func ParseFieldType(s string) (FieldType, error) {
	switch s {
	case "int":
		return IntType, nil
	case "string":
		return StringType, nil
	case "bool":
		return BoolType, nil
	case "float":
		return FloatType, nil
	case "date":
		return DateType, nil
	default:
		return 0, fmt.Errorf("unknown type: %s", s)
	}
}

func ParseValue(s string, fieldType FieldType) (any, error) {
	switch fieldType {
	case IntType:
		val, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		return int32(val), nil
	case StringType:
		return s, nil
	case BoolType:
		val, err := strconv.ParseBool(s)
		if err != nil {
			return nil, err
		}
		return val, nil
	case FloatType:
		val, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		return val, nil
	case DateType:
		t, err := time.Parse("2006-01-02", s)
		val := t.Unix()
		if err != nil {
			return nil, err
		}
		return val, nil
	default:
		return nil, fmt.Errorf("unsupported type: %v", fieldType)
	}
}

type Field struct {
	Name string
	Type FieldType
}

type Schema struct {
	TableName string
	Fields    []Field
}

func (s Schema) GetFieldNames() []string {
	names := make([]string, len(s.Fields))
	for i, field := range s.Fields {
		names[i] = field.Name
	}
	return names
}

func (s *Schema) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)

	// write table name data
	if err := encoding.WriteString(buf, s.TableName); err != nil {
		return nil, err
	}
	// write number of fields
	if err := encoding.WriteUint32(buf, uint32(len(s.Fields))); err != nil {
		return nil, err
	}

	for _, field := range s.Fields {
		// field name
		if err := encoding.WriteString(buf, field.Name); err != nil {
			return nil, err
		}
		// field type
		if _, err := buf.Write([]byte{byte(field.Type)}); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func Deserialize(r io.Reader) (Schema, error) {
	sch := Schema{}

	// read table name
	name, err := encoding.ReadString(r)
	if err != nil {
		return Schema{}, err
	}
	sch.TableName = name

	// read number of fields
	numFields, err := encoding.ReadUint32(r)
	if err != nil {
		return Schema{}, err
	}

	// read each field
	sch.Fields = make([]Field, numFields)
	for i := 0; i < int(numFields); i++ {
		fieldName, err := encoding.ReadString(r)
		if err != nil {
			return Schema{}, err
		}

		typeByte := make([]byte, 1)
		_, err = r.Read(typeByte)
		if err != nil {
			return Schema{}, err
		}

		sch.Fields[i] = Field{
			Name: fieldName,
			Type: FieldType(typeByte[0]),
		}
	}

	return sch, nil
}

func (s *Schema) SerializeRecord(rec Record) ([]byte, error) {
	buf := new(bytes.Buffer)

	// first field is always the key
	keyField := s.Fields[0]
	keyVal, ok := rec[keyField.Name]
	if !ok {
		return nil, fmt.Errorf("missing the key field: %s", keyField.Name)
	}

	// write key as uint64
	var key uint64
	switch v := keyVal.(type) {
	case int32:
		key = uint64(v)
	default:
		return nil, fmt.Errorf("key must be int32 for now")
	}

	if err := binary.Write(buf, binary.LittleEndian, key); err != nil {
		return nil, err
	}

	// write all fields (including key again for complete record)
	for _, field := range s.Fields {
		val, ok := rec[field.Name]
		if !ok {
			return nil, fmt.Errorf("missing field: %s", field.Name)
		}

		if err := writeFieldValue(buf, field.Type, val); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (s *Schema) DeserializeRecord(data []byte) (uint64, Record, error) {
	r := bytes.NewReader(data)

	// read key
	var key uint64
	if err := binary.Read(r, binary.LittleEndian, &key); err != nil {
		return 0, nil, err
	}

	// read all fields
	rec := make(Record)
	for _, field := range s.Fields {
		val, err := readFieldValue(r, field.Type)
		if err != nil {
			return 0, nil, err
		}
		rec[field.Name] = val
	}

	return key, rec, nil
}

type Record map[string]any

func writeFieldValue(w io.Writer, fieldType FieldType, value any) error {
	switch fieldType {
	case IntType:
		v := value.(int32)
		return encoding.WriteUint32(w, uint32(v))
	case StringType:
		s := value.(string)
		return encoding.WriteString(w, s)
	case BoolType:
		b := value.(bool)
		if b {
			_, err := w.Write([]byte{1})
			return err
		}
		_, err := w.Write([]byte{0})
		return err
	case FloatType:
		f := value.(float64)
		return encoding.WriteFloat64(w, f)
	case DateType:
		v := value.(int64)
		return encoding.WriteInt64(w, v)
	default:
		return fmt.Errorf("unsupported type: %v", fieldType)
	}
}

func readFieldValue(r io.Reader, fieldType FieldType) (any, error) {
	switch fieldType {
	case IntType:
		val, err := encoding.ReadUint32(r)
		return int32(val), err
	case StringType:
		return encoding.ReadString(r)
	case BoolType:
		buf := make([]byte, 1)
		_, err := r.Read(buf)
		return buf[0] != 0, err
	case FloatType:
		return encoding.ReadFloat64(r)
	case DateType:
		unixTimestamp, err := encoding.ReadInt64(r)
		if err != nil {
			return nil, err
		}
		t := time.Unix(unixTimestamp, 0)
		return t.UTC().Format("2006-01-02"), nil
	default:
		return nil, fmt.Errorf("unsupported type: %v", fieldType)
	}
}

func (s *Schema) ExtractPrimaryKey(record Record) (uint64, error) {
	if len(s.Fields) == 0 {
		return 0, errors.New("schema has no fields")
	}

	firstField := s.Fields[0]
	id, ok := record[firstField.Name].(int32)
	if !ok {
		return 0, fmt.Errorf("primary key %s must be int32", firstField.Name)
	}
	if id < 0 {
		return 0, fmt.Errorf("primary key cannot be negative: %d", id)
	}
	return uint64(id), nil
}
