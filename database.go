package main

import (
	"errors"
	"fmt"
	"strings"
)

type Result struct {
	schema Schema
	rows   []Row
}

func (result *Result) String() string {
	if result == nil {
		return "<nil result>"
	}

	var s strings.Builder
	for _, field := range result.schema.fields {
		s.WriteString(field.name)
		s.WriteByte('\t')
	}

	for _, row := range result.rows {
		for _, field := range row {
			s.WriteString(field.String())
			s.WriteByte('\n')
		}
	}

	return s.String()
}

type Database struct {
	tables map[string]*Table
}

func NewDatabase() *Database {
	return &Database{
		tables: make(map[string]*Table),
	}
}

func (db *Database) Close() {
	for _, table := range db.tables {
		table.Close()
	}
}

func (db *Database) Execute(query *Query) (*Result, error) {
	switch {
	case query.Create != nil:
		create := query.Create
		_, ok := db.tables[create.Name]
		if ok {
			return nil, errors.New("table with such name already exist")
		}

		table, err := NewTable(create.Name, create.Fields)
		if err != nil {
			return nil, err
		}

		db.tables[create.Name] = table
	case query.Insert != nil:
		insert := query.Insert
		table, ok := db.tables[insert.Name]
		if !ok {
			return nil, errors.New("no table with such name")
		}

		rows := ConvertRows(insert.Rows)
		for i, row := range rows {
			err := table.schema.Typecheck(row)
			if err != nil {
				return nil, fmt.Errorf("row #%d %v", i, err)
			}
		}

		err := table.Insert(rows)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unhandled query")
	}

	return nil, nil
}
