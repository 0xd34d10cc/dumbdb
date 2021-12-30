package main

import (
	"errors"
	"fmt"
	"io"
)

type Storage interface {
	io.ReaderAt
	io.WriterAt
	io.Seeker

	Sync() error
	Truncate(size int64) error
}

// type RowID struct { PageID, SlotID }
type PageID uint32

const PageSize uint32 = 4096
const InvalidPageID PageID = PageID(0xf0000000)

var (
	ErrInvalidStorageSize = errors.New("storage size should be multiple of page size")
	ErrNoFreePages        = errors.New("no free pages")
	ErrPageNotAllocated   = errors.New("page not allocated")
)

func (id PageID) String() string {
	if id == InvalidPageID {
		return "PageID(invalid)"
	}
	return fmt.Sprintf("PageID(%d)", uint32(id))
}

type Page struct {
	dirty bool
	data  [PageSize]byte
}

func (page *Page) Data() []byte {
	return page.data[:]
}

func (page *Page) IsDirty() bool {
	return page.dirty
}

func (page *Page) MarkDirty() {
	page.dirty = true
}

func (page *Page) markClean() {
	page.dirty = false
}

// Manages pool of pages in memory abstracting away details of file storage
type Pager struct {
	storage     Storage
	storageSize int64
	memory      map[PageID]*Page

	index *Page
}

// Create a new pager backed by storage
func NewPager(storage Storage) (*Pager, error) {
	storageSize, err := storage.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	if storageSize%int64(PageSize) != 0 {
		return nil, ErrInvalidStorageSize
	}

	pager := &Pager{
		storage:     storage,
		storageSize: storageSize,
		memory:      make(map[PageID]*Page),
		index:       nil,
	}

	pager.index, err = pager.readPageAt(0)
	if err != nil {
		return nil, err
	}

	return pager, nil
}

// Obtain a page by id
func (pager *Pager) FetchPage(id PageID) (*Page, error) {
	// first check the memory cache
	page, ok := pager.memory[id]
	if ok {
		return page, nil
	}

	// read from the disk
	page, err := pager.readPage(id)
	if err != nil {
		return nil, err
	}

	pager.putInPool(id, page)
	return page, nil
}

// Allocate a new page on the disk, this only changes the metadata
func (pager *Pager) AllocatePage() (PageID, error) {
	// Check metadata for free pages to reuse
	for id := 0; id < len(pager.index.data); id++ {
		if !pager.isPageAllocated(PageID(id)) {
			// page 0 is reserved for metadata
			pager.markAllocated(PageID(id))
			return PageID(id), nil
		}
	}

	return InvalidPageID, ErrNoFreePages
}

// Mark page as deallocated, this only changes the metadata
func (pager *Pager) DeallocatePage(id PageID) {
	// remove from memory
	delete(pager.memory, id)

	// mark as deallocated
	pager.markDeallocated(id)
}

// Flush page to disk by id
func (pager *Pager) SyncPageByID(id PageID) error {
	page, ok := pager.memory[id]
	if !ok {
		// page is not cached, nothing to sync
		return nil
	}

	return pager.SyncPage(id, page)
}

// Flush page to disk
func (pager *Pager) SyncPage(id PageID, page *Page) error {
	if !page.IsDirty() {
		// no changes in page, nothing to sync
		return nil
	}

	err := pager.writePage(id, page)
	if err != nil {
		return err
	}

	page.markClean()
	return nil
}

// Flush all metadata pages to the disk
func (pager *Pager) SyncMetadata() error {
	page := pager.index
	if !page.IsDirty() {
		return nil
	}

	err := pager.writePageAt(0, page)
	if err != nil {
		return err
	}

	page.markClean()
	return nil
}

// Flush all the pages to the disk
func (pager *Pager) SyncAll() error {
	err := pager.SyncMetadata()
	if err != nil {
		return err
	}

	for id, page := range pager.memory {
		err = pager.SyncPage(id, page)
		if err != nil {
			return err
		}
	}

	return nil
}

// Get ID of the first page. Returns InvalidPageID if db is empty
func (pager *Pager) FirstPage() PageID {
	id := PageID(^uint32(0)) // uint32(-1)
	return pager.NextPage(id)
}

func (pager *Pager) NextPage(pid PageID) PageID {
	for id := int(pid + 1); id < len(pager.index.data); id++ {
		if pager.isPageAllocated(PageID(id)) {
			return PageID(id)
		}
	}
	return InvalidPageID
}

// Check whether or not page is allocated according to the metadata
func (pager *Pager) isPageAllocated(id PageID) bool {
	slotOffset := int(id)
	if slotOffset >= len(pager.index.data) {
		// Out of bounds
		return false
	}
	return pager.index.data[slotOffset] == 1
}

// Mark page as allocated
func (pager *Pager) markAllocated(id PageID) {
	if !pager.isPageAllocated(id) {
		pager.index.data[int(id)] = 1
		pager.index.MarkDirty()
	}
}

// Mark page as free
func (pager *Pager) markDeallocated(id PageID) {
	if pager.isPageAllocated(id) {
		pager.index.data[int(id)] = 0
		pager.index.MarkDirty()
	}
}

// Put a page in memory pager, evicting if neccesarry
func (pager *Pager) putInPool(id PageID, page *Page) {
	// TODO: add upper limit on number of pages in memory
	pager.memory[id] = page
}

// Map page id to file offset
func (pager *Pager) offsetFromID(id PageID) (int64, error) {
	if !pager.isPageAllocated(id) {
		return 0, ErrPageNotAllocated
	}

	// TODO: store offsets in the metadata page
	return (int64(id) + 1) * int64(PageSize), nil
}

// Allocate empty page in memory
func (pager *Pager) allocateMemoryPage() *Page {
	// TODO: use a fixed pager of N (fixed) + K (allocation request limit) pages
	return &Page{}
}

// Read page at offset
func (pager *Pager) readPageAt(offset int64) (*Page, error) {
	if offset >= pager.storageSize {
		newSize := offset + int64(PageSize)
		err := pager.storage.Truncate(newSize)
		if err != nil {
			return nil, err
		}
		pager.storageSize = newSize
	}

	page := pager.allocateMemoryPage()
	_, err := pager.storage.ReadAt(page.data[:], offset)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// Read page from the disk
func (pager *Pager) readPage(id PageID) (*Page, error) {
	offset, err := pager.offsetFromID(id)
	if err != nil {
		return nil, err
	}

	return pager.readPageAt(offset)
}

// Write page at offset
func (pager *Pager) writePageAt(offset int64, page *Page) error {
	_, err := pager.storage.WriteAt(page.data[:], offset)
	return err
}

// Write page to the disk
func (pager *Pager) writePage(id PageID, page *Page) error {
	offset, err := pager.offsetFromID(id)
	if err != nil {
		return err
	}

	return pager.writePageAt(offset, page)
}
