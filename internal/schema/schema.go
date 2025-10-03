package schema

type FieldType int

const (
	IntType FieldType = iota
	StringType
	BoolType
	FloatType
)

type Field struct {
	Name string
	Type FieldType
}

type Schema struct {
	TableName string
	Fields    []Field
}

type Record map[string]any
