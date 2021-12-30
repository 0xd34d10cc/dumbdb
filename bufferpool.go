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
type BufferPool struct {
	storage     Storage
	storageSize int64
	memory      map[PageID]*Page

	index *Page
}

// Create a new buffer pool backed by storage
func NewBufferPool(storage Storage) (*BufferPool, error) {
	storageSize, err := storage.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	if storageSize%int64(PageSize) != 0 {
		return nil, ErrInvalidStorageSize
	}

	pool := &BufferPool{
		storage:     storage,
		storageSize: storageSize,
		memory:      make(map[PageID]*Page),
		index:       nil,
	}

	pool.index, err = pool.readPageAt(0)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

// Obtain a page by id
func (pool *BufferPool) FetchPage(id PageID) (*Page, error) {
	// first check the memory cache
	page, ok := pool.memory[id]
	if ok {
		return page, nil
	}

	// read from the disk
	page, err := pool.readPage(id)
	if err != nil {
		return nil, err
	}

	pool.putInPool(id, page)
	return page, nil
}

// Allocate a new page on the disk, this only changes the metadata
func (pool *BufferPool) AllocatePage() (PageID, error) {
	// Check metadata for free pages to reuse
	for id := 0; id < len(pool.index.data); id++ {
		if !pool.isPageAllocated(PageID(id)) {
			// page 0 is reserved for metadata
			pool.markAllocated(PageID(id))
			return PageID(id), nil
		}
	}

	return InvalidPageID, ErrNoFreePages
}

// Mark page as deallocated, this only changes the metadata
func (pool *BufferPool) DeallocatePage(id PageID) {
	// remove from memory
	delete(pool.memory, id)

	// mark as deallocated
	pool.markDeallocated(id)
}

// Flush page to disk by id
func (pool *BufferPool) SyncPageByID(id PageID) error {
	page, ok := pool.memory[id]
	if !ok {
		// page is not cached, nothing to sync
		return nil
	}

	return pool.SyncPage(id, page)
}

// Flush page to disk
func (pool *BufferPool) SyncPage(id PageID, page *Page) error {
	if !page.IsDirty() {
		// no changes in page, nothing to sync
		return nil
	}

	err := pool.writePage(id, page)
	if err != nil {
		return err
	}

	page.markClean()
	return nil
}

// Flush all metadata pages to the disk
func (pool *BufferPool) SyncMetadata() error {
	page := pool.index
	if !page.IsDirty() {
		return nil
	}

	err := pool.writePageAt(0, page)
	if err != nil {
		return err
	}

	page.markClean()
	return nil
}

// Flush all the pages to the disk
func (pool *BufferPool) SyncAll() error {
	err := pool.SyncMetadata()
	if err != nil {
		return err
	}

	for id, page := range pool.memory {
		err = pool.SyncPage(id, page)
		if err != nil {
			return err
		}
	}

	return nil
}

// Get ID of the first page. Returns InvalidPageID if db is empty
func (pool *BufferPool) FirstPage() PageID {
	id := PageID(^uint32(0)) // uint32(-1)
	return pool.NextPage(id)
}

func (pool *BufferPool) NextPage(pid PageID) PageID {
	for id := int(pid + 1); id < len(pool.index.data); id++ {
		if pool.isPageAllocated(PageID(id)) {
			return PageID(id)
		}
	}
	return InvalidPageID
}

// Check whether or not page is allocated according to the metadata
func (pool *BufferPool) isPageAllocated(id PageID) bool {
	slotOffset := int(id)
	if slotOffset >= len(pool.index.data) {
		// Out of bounds
		return false
	}
	return pool.index.data[slotOffset] == 1
}

// Mark page as allocated
func (pool *BufferPool) markAllocated(id PageID) {
	if !pool.isPageAllocated(id) {
		pool.index.data[int(id)] = 1
		pool.index.MarkDirty()
	}
}

// Mark page as free
func (pool *BufferPool) markDeallocated(id PageID) {
	if pool.isPageAllocated(id) {
		pool.index.data[int(id)] = 0
		pool.index.MarkDirty()
	}
}

// Put a page in memory pool, evicting if neccesarry
func (pool *BufferPool) putInPool(id PageID, page *Page) {
	// TODO: add upper limit on number of pages in memory
	pool.memory[id] = page
}

// Map page id to file offset
func (pool *BufferPool) offsetFromID(id PageID) (int64, error) {
	if !pool.isPageAllocated(id) {
		return 0, ErrPageNotAllocated
	}

	// TODO: store offsets in the metadata page
	return (int64(id) + 1) * int64(PageSize), nil
}

// Allocate empty page in memory
func (pool *BufferPool) allocateMemoryPage() *Page {
	// TODO: use a fixed pool of N (fixed) + K (allocation request limit) pages
	return &Page{}
}

// Read page at offset
func (pool *BufferPool) readPageAt(offset int64) (*Page, error) {
	if offset >= pool.storageSize {
		newSize := offset + int64(PageSize)
		err := pool.storage.Truncate(newSize)
		if err != nil {
			return nil, err
		}
		pool.storageSize = newSize
	}

	page := pool.allocateMemoryPage()
	_, err := pool.storage.ReadAt(page.data[:], offset)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// Read page from the disk
func (pool *BufferPool) readPage(id PageID) (*Page, error) {
	offset, err := pool.offsetFromID(id)
	if err != nil {
		return nil, err
	}

	return pool.readPageAt(offset)
}

// Write page at offset
func (pool *BufferPool) writePageAt(offset int64, page *Page) error {
	_, err := pool.storage.WriteAt(page.data[:], offset)
	return err
}

// Write page to the disk
func (pool *BufferPool) writePage(id PageID, page *Page) error {
	offset, err := pool.offsetFromID(id)
	if err != nil {
		return err
	}

	return pool.writePageAt(offset, page)
}
