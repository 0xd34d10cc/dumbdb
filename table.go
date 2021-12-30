package main

import (
	"encoding/binary"
	"log"
	"os"
)

type DataPage struct {
	nRows uint16
	id    PageID
	page  *Page
}

func ReadDataPage(id PageID, page *Page) DataPage {
	nRows := binary.LittleEndian.Uint16(page.Data()[:2])
	return DataPage{
		nRows: nRows,
		id:    id,
		page:  page,
	}
}

// Returns true on success
func (p *DataPage) TryInsert(row Row, schema *Schema) bool {
	offset := 2 + schema.RowSize()*int(p.nRows)
	if offset+schema.RowSize() > len(p.page.Data()) {
		return false
	}

	err := schema.WriteRow(p.page.Data()[offset:], row)
	if err != nil {
		return false
	}

	p.nRows += 1
	binary.LittleEndian.PutUint16(p.page.Data(), p.nRows)
	p.page.MarkDirty()
	return true
}

type Table struct {
	schema Schema
	file   *os.File
	pager  *Pager
}

// Create a new table
func NewTable(name string, fields []FieldDescription) (*Table, error) {
	return initTable(name, fields, true)
}

// Open existing table
func OpenTable(name string, fields []FieldDescription) (*Table, error) {
	return initTable(name, fields, false)
}

func initTable(name string, fields []FieldDescription, isNew bool) (*Table, error) {
	schema := NewSchema(fields)
	flags := os.O_RDWR | os.O_CREATE
	if isNew {
		flags |= os.O_EXCL
	}

	file, err := os.OpenFile(name+".bin", flags, 0600)
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
	i := 0
	// first try inserting into existing pages
	for id := table.pager.FirstPage(); id != InvalidPageID; id = table.pager.NextPage(id) {
		page, err := table.pager.FetchPage(id)
		if err != nil {
			return err
		}

		dataPage := ReadDataPage(id, page)
		for i < len(rows) && dataPage.TryInsert(rows[i], &table.schema) {
			i++
			// TODO: sync page?
		}

		if i == len(rows) {
			return nil
		}
	}

	// no space on existing pages, allocate new pages
	for {
		id, err := table.pager.AllocatePage()
		if err != nil {
			return err
		}

		page, err := table.pager.FetchPage(id)
		if err != nil {
			return err
		}

		dataPage := ReadDataPage(id, page)
		for i < len(rows) && dataPage.TryInsert(rows[i], &table.schema) {
			i++
			// TODO: sync page?
		}

		if i == len(rows) {
			return nil
		}
	}
}

func (table *Table) Close() {
	err := table.pager.SyncAll()
	if err != nil {
		log.Println("Failed to sync:", err)
	}
	table.file.Close()
}
