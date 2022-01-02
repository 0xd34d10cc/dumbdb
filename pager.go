package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"sync"
)

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

const (
	IndexHeaderSize        = 4
	IndexMaxEntriesPerPage = (PageSize - IndexHeaderSize) * 8
)

type AllocationIndex struct {
	nEntires uint32
	root     *Page
}

func ReadAllocationIndex(storage Storage) (*AllocationIndex, error) {
	root := &Page{}
	_, err := storage.ReadAt(root.Data(), 0)
	if err != nil {
		return nil, err
	}

	nEntries := binary.LittleEndian.Uint32(root.Data())
	return &AllocationIndex{
		nEntires: nEntries,
		root:     root,
	}, nil
}

func (index *AllocationIndex) Lock() {
	index.root.Lock()
}

func (index *AllocationIndex) Unlock() {
	index.root.Unlock()
}

func (index *AllocationIndex) SyncPages(storage Storage) error {
	if !index.root.IsDirty() {
		return nil
	}

	binary.LittleEndian.PutUint32(index.root.Data(), index.nEntires)
	_, err := storage.WriteAt(index.root.Data(), 0)
	if err != nil {
		index.root.MarkClean()
	}
	return err
}

// returns number of pages allocated
func (index *AllocationIndex) NumEntries() uint32 {
	return index.nEntires
}

func (index *AllocationIndex) GetOffset(id PageID) int64 {
	if uint32(id) >= index.NumEntries() {
		return -1
	}

	return (1 + int64(id)) * int64(PageSize)
}

func (index *AllocationIndex) IsAllocated(id PageID) bool {
	idx := uint32(id)
	if idx >= index.NumEntries() {
		return false
	}

	nByte := idx / 8
	nBit := idx % 8
	return index.root.Data()[IndexHeaderSize:][nByte]&(1<<nBit) != 0
}

func (index *AllocationIndex) Allocate() PageID {
	idx := index.NumEntries()
	if idx >= IndexMaxEntriesPerPage {
		return InvalidPageID
	}

	nByte := idx / 8
	nBit := idx % 8
	index.root.Data()[IndexHeaderSize:][nByte] |= (1 << nBit)
	index.nEntires++
	index.root.MarkDirty()
	return PageID(idx)
}

type Storage interface {
	io.ReaderAt
	io.WriterAt
	io.Seeker

	Truncate(size int64) error
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
	index *AllocationIndex
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

	pager.index, err = ReadAllocationIndex(storage)
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
	id := index.Allocate()
	if id == InvalidPageID {
		return InvalidPageID, ErrNoFreePages
	}

	offset := index.GetOffset(id)
	err := pager.ensureSize(offset + int64(PageSize))
	return id, err
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
	return index.SyncPages(pager.storage)
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

func (pager *Pager) NextPage(id PageID) PageID {
	index := pager.index
	index.Lock()
	defer index.Unlock()
	next := PageID(uint32(id) + 1)
	if index.IsAllocated(next) {
		return next
	}
	return InvalidPageID
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
	offset := index.GetOffset(id)
	if offset == -1 {
		index.Unlock()
		return nil, ErrPageNotAllocated
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
	offset := index.GetOffset(id)
	if offset == -1 {
		index.Unlock()
		return ErrPageNotAllocated
	}
	index.Unlock()

	return pager.writePageAt(offset, page)
}
