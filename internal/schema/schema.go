package schema

import (
	"fmt"
	"strconv"
)

type FieldType int

const (
	IntType FieldType = iota
	StringType
	BoolType
	FloatType
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

func (s *Schema) GetFieldNames() []string {
	names := make([]string, len(s.Fields))
	for i, field := range s.Fields {
		names[i] = field.Name
	}
	return names
}

type Record map[string]any
