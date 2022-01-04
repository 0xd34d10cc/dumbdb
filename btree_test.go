package main

import (
	"errors"
	"fmt"
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

func debugTree(t *testing.T, tree *BTree) {
	fmt.Println(tree.root.len())
	for i := 0; i < tree.root.len(); i++ {
		key, id := tree.root.getBranch(i)
		fmt.Printf("%v) %v -> %v", i, key, id)

		page, err := tree.pager.FetchPage(id)
		if err != nil {
			t.Fatal(err)
		}

		node := readNode(page)
		if !node.isLeaf {
			t.Fatal("not leaf")
		}

		fmt.Printf(" (prev: %v, next: %v): ", node.prev, node.next)

		n := node.len()
		if n > 10 {
			n = 5
		}
		for idx := 0; idx < n; idx++ {
			k, v := node.getLeaf(idx)
			fmt.Printf("(%v, %v), ", k, v)
		}

		fmt.Printf(" ... ")
		for idx := node.len() - 5; idx < node.len(); idx++ {
			k, v := node.getLeaf(idx)
			fmt.Printf(", (%v, %v)", k, v)
		}
		fmt.Println()
	}
}

func TestInsert(t *testing.T) {
	storage := &MemoryStorage{
		data: make([]byte, 0),
	}
	pager, err := NewPager(10, storage)
	if err != nil {
		t.Fatal(err)
	}

	rootID, err := pager.AllocatePage()
	if err != nil {
		t.Fatal(err)
	}

	root, err := pager.FetchPage(rootID)
	if err != nil {
		t.Fatal(err)
	}

	const nEntries = (LeafNodeCap - 1) * 6

	tree := NewBTree(root, pager)
	// insert high keys
	for key := nEntries - 1; key >= nEntries/2; key-- {
		val := key * 2
		err = tree.Insert(BTreeKey(key), BTreeValue(val))
		if err != nil {
			t.Fatal(err)
		}
	}

	debugTree(t, tree)

	fmt.Println("----------------------------------------")

	// insert low keys
	for key := 0; key < nEntries/2; key++ {
		val := key * 2
		err = tree.Insert(BTreeKey(key), BTreeValue(val))
		if err != nil {
			t.Fatal(err)
		}
	}

	debugTree(t, tree)

	const searchFrom = 0
	c := tree.Search(BTreeKey(searchFrom))
	for i := searchFrom; i < nEntries; i++ {
		if c.Err() != nil {
			t.Fatal(c.Err())
		}

		key, val := c.Get()
		if key != BTreeKey(i) {
			t.Fatalf("Unexpected key at %v: %v %v\n", i, key, val)
		}

		if !c.Forward() {
			if i+1 != nEntries {
				t.Fatalf("Unexpected end of cursor: %v at %v\n", c.Err(), i)
			}
		}
	}
}
