package main

import (
	"errors"
	"io"
	"testing"
)

var (
	ErrPartialRead  = errors.New("partial read")
	ErrPartialWrite = errors.New("partial write")
)

type MemoryStorage struct {
	data []byte
	off  int64
}

func (s *MemoryStorage) Truncate(size int64) error {
	if int64(len(s.data)) == size {
		return nil
	}

	newData := make([]byte, int(size))
	copy(newData, s.data)
	s.data = newData
	return nil
}

func (s *MemoryStorage) ReadAt(buf []byte, off int64) (n int, err error) {
	n = copy(buf, s.data[off:])
	if n != len(buf) {
		err = ErrPartialRead
	}
	return
}

func (s *MemoryStorage) WriteAt(buf []byte, off int64) (n int, err error) {
	n = copy(s.data[off:], buf)
	if n != len(buf) {
		err = ErrPartialWrite
	}
	return
}

func (s *MemoryStorage) Seek(diff int64, whence int) (newOff int64, err error) {
	switch whence {
	case io.SeekCurrent:
		s.off = s.off + diff
	case io.SeekStart:
		s.off = diff
	case io.SeekEnd:
		s.off = int64(len(s.data)) + diff
	}
	newOff = s.off
	return
}

func TestInsert(t *testing.T) {
	storage := &MemoryStorage{
		data: make([]byte, 0),
	}
	pager, err := NewPager(10, storage)
	if err != nil {
		t.Error(err)
	}

	rootID, err := pager.AllocatePage()
	if err != nil {
		t.Error(err)
	}

	root, err := pager.FetchPage(rootID)
	if err != nil {
		t.Error(err)
	}

	const nEntries = 500
	const searchFrom = nEntries / 2

	tree := NewBTree(root, pager)
	for key := 0; key < nEntries; key++ {
		val := key * 2
		err = tree.Insert(BTreeKey(key), BTreeValue(val))
		if err != nil {
			t.Error(err)
		}
	}

	c := tree.Search(searchFrom)
	for i := searchFrom; i < nEntries; i++ {
		if c.Err() != nil {
			t.Error(c.Err())
		}

		key, val := c.Get()
		if key != BTreeKey(i) {
			t.Errorf("Unexpected key at %v: %v %v\n", i, key, val)
		}

		if !c.Forward() {
			if i+1 != nEntries {
				t.Errorf("Unexpected end of cursor: %v at %v\n", c.Err(), i)
			}
		}
	}
}
