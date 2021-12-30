//nolint:govet
package main

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// A custom lexer for INI files. This illustrates a relatively complex Regexp lexer, as well
// as use of the Unquote filter, which unquotes string tokens.
var iniLexer = lexer.MustSimple([]lexer.Rule{
	{Name: `Ident`, Pattern: `[a-zA-Z][a-zA-Z_\d]*`},
	{Name: `String`, Pattern: `"(?:\\.|[^"])*"`},
	{Name: `Int`, Pattern: `\d+`},
	{Name: `Float`, Pattern: `\d+(?:\.\d+)?`},
	{Name: `Operators`, Pattern: `<>|!=|<=|>=|[-+*/%,.()=<>]`},
	{Name: "comment", Pattern: `[#;][^\n]*`},
	{Name: "whitespace", Pattern: `\s+`},
})

type Type struct {
	Integer bool `@"int"`
	Varchar int  `| "varchar" "(" @Int ")"`
}

func (t *Type) String() string {
	if t == nil {
		return "<nil type>"
	}

	if t.Integer {
		return "int"
	}

	return fmt.Sprintf("varchar(%d)", t.Varchar)
}

type FieldDescription struct {
	Name string `@Ident`
	Type *Type  `@@`
}

func (f *FieldDescription) String() string {
	if f == nil {
		return "<nil field description>"
	}
	return fmt.Sprintf("%s %v", f.Name, f.Type)
}

type Create struct {
	Name   string             `"create" "table" @Ident`
	Fields []FieldDescription `"(" @@ ("," @@)*  ")"`
}

func (q *Create) String() string {
	if q == nil {
		return "<nil create query>"
	}

	var s strings.Builder
	s.WriteString("create table ")
	s.WriteString(q.Name)
	s.WriteString(" (")
	for i, field := range q.Fields {
		s.WriteString(field.String())
		if i != len(q.Fields)-1 {
			s.WriteString(", ")
		}
	}
	s.WriteByte(')')
	return s.String()
}

type Query struct {
	Create *Create `@@`
}

func (q *Query) String() string {
	if q == nil {
		return "<nil query>"
	}

	if q.Create != nil {
		return q.Create.String()
	}

	return "<invalid query>"
}

var parser = participle.MustBuild(&Query{},
	participle.Lexer(iniLexer),
	participle.Unquote("String"),
)

func ParseQuery(query string) (*Query, error) {
	q := &Query{}
	err := parser.ParseString("", query, q)
	if err != nil {
		return nil, err
	}
	return q, nil
}
