package dumbdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

type TypeID uint8

const (
	TypeInt = iota
	TypeVarchar
)

func (t TypeID) String() string {
	switch t {
	case TypeInt:
		return "int"
	case TypeVarchar:
		return "varchar"
	}

	return "<invalid type id>"
}

type Field struct {
	Name   string `json:"name"`
	TypeID TypeID `json:"type_id"`
	Len    uint8  `json:"len"`
}

func (field *Field) Typecheck(v *Value) error {
	if field.TypeID != v.TypeID {
		return fmt.Errorf("unexpected type for %v (expected %v, got %v)", field.Name, field.TypeID, v.TypeID)
	}

	switch field.TypeID {
	case TypeInt:
		// nothing
	case TypeVarchar:
		if len(v.Str) > int(field.Len) {
			return fmt.Errorf("value for %v is too long (%v is max)", field.Name, field.Len)
		}
	default:
		panic("unhandled type id")
	}

	return nil
}

func (field *Field) Read(data []byte) Value {
	v := Value{
		TypeID: field.TypeID,
	}
	switch field.TypeID {
	case TypeInt:
		v.Int = int32(binary.LittleEndian.Uint32(data[:4]))
	case TypeVarchar:
		v.Str = string(data[:field.Len])
	default:
		panic("unhandled type id")
	}
	return v
}

func (field *Field) Write(data []byte, val Value) {
	switch val.TypeID {
	case TypeInt:
		binary.LittleEndian.PutUint32(data, uint32(val.Int))
	case TypeVarchar:
		copy(data, []byte(val.Str))
		for i := len(val.Str); i < int(field.Len); i++ {
			data[i] = 0
		}
	default:
		panic("unhandled type id")
	}
}

type Value struct {
	TypeID TypeID
	Int    int32
	Str    string
}

func (val *Value) String() string {
	if val == nil {
		return "<nil value>"
	}

	switch val.TypeID {
	case TypeInt:
		return strconv.FormatInt(int64(val.Int), 10)
	case TypeVarchar:
		trailingZeros := len(val.Str) - 1
		for trailingZeros > 0 && val.Str[trailingZeros] == 0 {
			trailingZeros--
		}

		if trailingZeros > 0 {
			trailingZeros++
		}
		return val.Str[:trailingZeros]
	}
	return "<invalid value>"
}

type Row []Value

func (row *Row) Project(indexes []int) Row {
	values := []Value(*row)
	newRow := make([]Value, 0, len(indexes))
	for _, idx := range indexes {
		newRow = append(newRow, values[idx])
	}
	return newRow
}

type Schema struct {
	Fields   []Field `json:"fields"`
	TotalLen int     `json:"total_len"`
}

func NewSchema(desc []FieldDescription) Schema {
	schema := Schema{
		Fields:   make([]Field, 0, len(desc)),
		TotalLen: 0,
	}

	for _, field := range desc {
		f := Field{
			Name: field.Name,
		}
		switch {
		case field.Type.Integer:
			f.TypeID = TypeInt
			f.Len = 4
		case field.Type.Varchar != 0:
			f.TypeID = TypeVarchar
			f.Len = uint8(field.Type.Varchar)
		default:
			panic("unhandled type")
		}

		schema.addField(f)
	}

	return schema
}

func (schema *Schema) addField(field Field) {
	schema.TotalLen += int(field.Len)
	schema.Fields = append(schema.Fields, field)
}

func (schema *Schema) getField(name string) (int, Field) {
	for idx, field := range schema.Fields {
		if field.Name == name {
			return idx, field
		}
	}
	return -1, Field{}
}

func (schema *Schema) RowSize() int {
	return schema.TotalLen
}

func (schema *Schema) ColumnNames() []string {
	names := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		names = append(names, field.Name)
	}
	return names
}

// Check whether row matches the schema, returns nil on success
func (schema *Schema) Typecheck(row Row) error {
	if len(schema.Fields) != len(row) {
		return errors.New("number of values doesn't match number of columns")
	}

	for i := 0; i < len(schema.Fields); i++ {
		err := schema.Fields[i].Typecheck(&row[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (schema *Schema) Project(names []string) (Schema, []int, error) {
	indexes := make([]int, 0, len(names))
	newSchema := Schema{}
	for _, fieldName := range names {
		idx, field := schema.getField(fieldName)
		if idx == -1 {
			return Schema{}, nil, fmt.Errorf("no column named %v in the schema", fieldName)
		}

		indexes = append(indexes, idx)
		newSchema.addField(field)

	}

	return newSchema, indexes, nil
}

func (schema *Schema) ReadRow(data []byte, row *Row) error {
	if len(data) < schema.TotalLen {
		return errors.New("not enough data")
	}

	offset := 0
	for _, field := range schema.Fields {
		val := field.Read(data[offset:])
		*row = append(*row, val)
		offset += int(field.Len)
	}

	return nil
}

func (schema *Schema) WriteRow(dst []byte, row Row) error {
	if len(dst) < schema.TotalLen {
		return errors.New("not enough space")
	}

	offset := 0
	for i, field := range schema.Fields {
		field.Write(dst[offset:], row[i])
		offset += int(field.Len)
	}

	return nil
}
