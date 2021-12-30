package main

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
	name   string
	typeID TypeID
	len    uint8
}

func (field *Field) Typecheck(v *Value) error {
	if field.typeID != v.TypeID {
		return fmt.Errorf("unexpected type for %v (expected %v, got %v)", field.name, field.typeID, v.TypeID)
	}

	switch field.typeID {
	case TypeInt:
		// nothing
	case TypeVarchar:
		if len(v.Str) > int(field.len) {
			return fmt.Errorf("value for %v is too long (%v is max)", field.name, field.len)
		}
	default:
		panic("unhandled type id")
	}

	return nil
}

func (field *Field) Read(data []byte) Value {
	v := Value{
		TypeID: field.typeID,
	}
	switch field.typeID {
	case TypeInt:
		v.Int = int32(binary.LittleEndian.Uint32(data[:4]))
	case TypeVarchar:
		v.Str = string(data[:field.len])
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
		for i := len(val.Str); i < int(field.len); i++ {
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
		return val.Str
	}
	return "<invalid value>"
}

type Row []Value

type Schema struct {
	fields   []Field
	totalLen int
}

func NewSchema(desc []FieldDescription) Schema {
	totalLen := 0
	fields := make([]Field, 0, len(desc))
	for _, field := range desc {
		f := Field{
			name: field.Name,
		}
		switch {
		case field.Type.Integer:
			f.typeID = TypeInt
			f.len = 4
		case field.Type.Varchar != 0:
			f.typeID = TypeVarchar
			f.len = uint8(field.Type.Varchar)
		default:
			panic("unhandled type")
		}
		totalLen += int(f.len)
		fields = append(fields, f)
	}

	return Schema{
		fields:   fields,
		totalLen: totalLen,
	}
}

func (schema *Schema) RowSize() int {
	return schema.totalLen
}

// Check whether row matches the schema, returns nil on success
func (schema *Schema) Typecheck(row Row) error {
	if len(schema.fields) != len(row) {
		return errors.New("number of values doesn't match number of columns")
	}

	for i := 0; i < len(schema.fields); i++ {
		err := schema.fields[i].Typecheck(&row[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (schema *Schema) ReadRow(data []byte, row *Row) error {
	if len(data) < schema.totalLen {
		return errors.New("not enough data")
	}

	offset := 0
	for _, field := range schema.fields {
		val := field.Read(data[offset:])
		*row = append(*row, val)
		offset += int(field.len)
	}

	return nil
}

func (schema *Schema) WriteRow(dst []byte, row Row) error {
	if len(dst) < schema.totalLen {
		return errors.New("not enough space")
	}

	offset := 0
	for i, field := range schema.fields {
		field.Write(dst[offset:], row[i])
		offset += int(field.len)
	}

	return nil
}
