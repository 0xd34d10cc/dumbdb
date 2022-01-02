package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"sync"
)

type Storage interface {
	io.ReaderAt
	io.WriterAt
	io.Seeker

	Truncate(size int64) error
}

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

var pageLocks []sync.Mutex = make([]sync.Mutex, 1024)

func fnv32(val uint32) uint32 {
	hasher := fnv.New32()
	var bytes [4]byte
	binary.LittleEndian.PutUint32(bytes[:], val)
	hasher.Write(bytes[:])
	return hasher.Sum32()
}

func pageIDMutex(id PageID) *sync.Mutex {
	idx := fnv32(uint32(id)) % uint32(len(pageLocks))
	return &pageLocks[idx]
}

func LockPageID(id PageID) {
	pageIDMutex(id).Lock()
}

func UnlockPageID(id PageID) {
	pageIDMutex(id).Unlock()
}

type Page struct {
	m     sync.Mutex
	dirty bool
	data  [PageSize]byte
}

func (page *Page) Lock() {
	page.m.Lock()
}

func (page *Page) Unlock() {
	page.m.Unlock()
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

func (page *Page) MarkClean() {
	page.dirty = false
}

// NOTE: all cache methods have to be thread-safe
type PageCache interface {
	// get page from cache by id
	Get(id PageID) *Page

	// try put page into cache
	// returns:
	//    (InvalidPageID, nil) if cache is not full
	//    (evictedID, evictedPage) otherwise
	//
	Put(id PageID, page *Page) (PageID, *Page)

	// remove page from cache
	// returns removed page, if any
	Remove(id PageID) *Page

	// run function f for each page in cache
	// and stop if f() returns false
	ForEach(f func(PageID, *Page) bool)
}

// Manages pool of pages in memory abstracting away details of file storage
type Pager struct {
	storage     Storage
	storageSize int64

	cache PageCache
	index *Page
}

// Create a new pager backed by storage
func NewPager(maxPages int, storage Storage) (*Pager, error) {
	storageSize, err := storage.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	if storageSize%int64(PageSize) != 0 {
		return nil, ErrInvalidStorageSize
	}

	cache := NewLRUCache(maxPages)
	pager := &Pager{
		storage:     storage,
		storageSize: storageSize,
		cache:       &cache,
		index:       nil,
	}

	err = pager.ensureSize(int64(PageSize))
	if err != nil {
		return nil, err
	}

	pager.index, err = pager.readPageAt(0)
	if err != nil {
		return nil, err
	}

	return pager, nil
}

// Obtain a page by id
func (pager *Pager) FetchPage(id PageID) (*Page, error) {
	LockPageID(id)
	defer UnlockPageID(id)

	// first check the memory cache
	page := pager.cache.Get(id)
	if page != nil {
		return page, nil
	}

	// read from the disk
	page, err := pager.readPage(id)
	if err != nil {
		return nil, err
	}

	// put page in cache
	evictedID, evictedPage := pager.cache.Put(id, page)
	if evictedID != InvalidPageID {
		if evictedID == id {
			// SyncPage() will deadlock in this case
			panic("duplicate page id")
		}
		err = pager.SyncPage(evictedID, evictedPage)
	}

	return page, err
}

// Allocate a new page on the disk, this only changes the metadata
func (pager *Pager) AllocatePage() (PageID, error) {
	index := pager.index
	index.Lock()
	defer index.Unlock()

	// Check metadata for free pages to reuse
	nEntries := len(index.Data())
	for id := 1; id < nEntries; id++ {
		if !pager.isPageAllocated(index, PageID(id)) {
			pager.markAllocated(index, PageID(id))

			offset, err := pager.offsetFromID(index, PageID(id))
			if err != nil {
				// should only fail if page is not allocated
				panic(err)
			}

			err = pager.ensureSize(offset + int64(PageSize))
			if err != nil {
				pager.markDeallocated(index, PageID(id))
				continue
			}
			return PageID(id), nil
		}
	}

	return InvalidPageID, ErrNoFreePages
}

// Mark page as deallocated, this only changes the metadata
func (pager *Pager) DeallocatePage(id PageID) {
	// remove from memory
	pager.cache.Remove(id)

	index := pager.index
	index.Lock()
	defer index.Unlock()
	// mark as deallocated
	pager.markDeallocated(index, id)
}

// Flush page to disk by id
func (pager *Pager) SyncPageByID(id PageID) error {
	page := pager.cache.Get(id)
	if page == nil {
		// page is not cached, nothing to sync
		return nil
	}

	page.Lock()
	defer page.Unlock()
	return pager.SyncPage(id, page)
}

// Flush page to disk, page have to be locked
func (pager *Pager) SyncPage(id PageID, page *Page) error {
	if !page.IsDirty() {
		// no changes in page, nothing to sync
		return nil
	}

	LockPageID(id)
	defer UnlockPageID(id)

	err := pager.writePage(id, page)
	if err != nil {
		return err
	}

	page.MarkClean()
	return nil
}

// Flush all metadata pages to the disk
func (pager *Pager) SyncMetadata() error {
	index := pager.index
	index.Lock()
	defer index.Unlock()

	if !index.IsDirty() {
		return nil
	}

	err := pager.writePageAt(0, index)
	if err != nil {
		return err
	}

	index.MarkClean()
	return nil
}

// Flush all the pages to the disk
func (pager *Pager) SyncAll() error {
	err := pager.SyncMetadata()
	if err != nil {
		return err
	}

	pager.cache.ForEach(func(id PageID, page *Page) bool {
		page.Lock()
		err = pager.SyncPage(id, page)
		page.Unlock()
		return err == nil
	})

	return err
}

// Get ID of the first page. Returns InvalidPageID if db is empty
func (pager *Pager) FirstPage() PageID {
	id := PageID(^uint32(0)) // uint32(-1)
	return pager.NextPage(id)
}

func (pager *Pager) NextPage(pid PageID) PageID {
	index := pager.index
	index.Lock()
	defer index.Unlock()
	nEntries := len(index.Data())
	for id := int(pid + 1); id < nEntries; id++ {
		if pager.isPageAllocated(index, PageID(id)) {
			return PageID(id)
		}
	}
	return InvalidPageID
}

// Check whether or not page is allocated according to the metadata
func (pager *Pager) isPageAllocated(index *Page, id PageID) bool {
	slotOffset := int(id)
	nEntries := len(index.Data())

	if slotOffset >= nEntries {
		// Out of bounds
		return false
	}
	return index.Data()[slotOffset] == 1
}

// Mark page as allocated
func (pager *Pager) markAllocated(index *Page, id PageID) {
	if !pager.isPageAllocated(index, id) {
		index.Data()[int(id)] = 1
		index.MarkDirty()
	}
}

// Mark page as free
func (pager *Pager) markDeallocated(index *Page, id PageID) {
	if pager.isPageAllocated(index, id) {
		index.Data()[int(id)] = 0
		index.MarkDirty()
	}
}

// Map page id to file offset
func (pager *Pager) offsetFromID(index *Page, id PageID) (int64, error) {
	if id == PageID(0) {
		// special case for root metadata page
		return 0, nil
	}

	if !pager.isPageAllocated(index, id) {
		return 0, ErrPageNotAllocated
	}

	return int64(id) * int64(PageSize), nil
}

func (pager *Pager) ensureSize(requiredSize int64) error {
	currentSize := pager.storageSize
	if currentSize >= requiredSize {
		return nil
	}

	err := pager.storage.Truncate(requiredSize)
	if err != nil {
		return err
	}

	pager.storageSize = requiredSize
	return nil
}

// Read page at offset
func (pager *Pager) readPageAt(offset int64) (*Page, error) {
	page := &Page{}
	_, err := pager.storage.ReadAt(page.Data(), offset)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// Read page from the disk
func (pager *Pager) readPage(id PageID) (*Page, error) {
	index := pager.index
	index.Lock()
	offset, err := pager.offsetFromID(index, id)
	if err != nil {
		index.Unlock()
		return nil, err
	}
	index.Unlock()
	return pager.readPageAt(offset)
}

// Write page at offset
func (pager *Pager) writePageAt(offset int64, page *Page) error {
	_, err := pager.storage.WriteAt(page.Data(), offset)
	return err
}

// Write page to the disk
func (pager *Pager) writePage(id PageID, page *Page) error {
	index := pager.index
	index.Lock()
	offset, err := pager.offsetFromID(index, id)
	if err != nil {
		index.Unlock()
		return err
	}
	index.Unlock()

	return pager.writePageAt(offset, page)
}
