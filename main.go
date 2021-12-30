package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/chzyer/readline"
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

type Table struct {
	schema Schema
	file   *os.File
	pager  *Pager
}

func NewTable(name string, fields []FieldDescription) (*Table, error) {
	schema := NewSchema(fields)
	file, err := os.OpenFile(name+".bin", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	pager, err := NewPager(file)
	if err != nil {
		return nil, err
	}

	return &Table{
		schema: schema,
		file:   file,
		pager:  pager,
	}, nil
}

func (table *Table) Close() {
	err := table.pager.SyncAll()
	if err != nil {
		log.Println("Failed to sync:", err)
	}
	table.file.Close()
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

func (db *Database) Do(query *Query) (*Result, error) {
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
		return nil, nil
	}

	return nil, errors.New("unhandled query")
}

func main() {
	db := NewDatabase()
	defer db.Close()
	rl, err := readline.New("> ")
	if err != nil {
		log.Fatal("Failed to initialize readline", err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}

		query, err := ParseQuery(line)
		if err != nil {
			fmt.Println("Failed to parse query:", err)
			continue
		}

		result, err := db.Do(query)
		if err != nil {
			fmt.Println("Failed to process query:", err)
			continue
		}

		fmt.Println(result)
	}
}
