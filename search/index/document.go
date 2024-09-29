package index

type FieldType int

type DocumentId uint32

const (
	TextFieldType FieldType = iota
	ByteFieldType
)

type Field struct {
	FieldType FieldType
	Name      string
	Value     []byte
}

type Document []Field
