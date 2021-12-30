package main

import (
	"errors"
	"log"
	"os"
)

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

// TODO: make it atomic
func (table *Table) Insert(rows []Row) error {
	return errors.New("not implemented")
}

func (table *Table) Close() {
	err := table.pager.SyncAll()
	if err != nil {
		log.Println("Failed to sync:", err)
	}
	table.file.Close()
}
