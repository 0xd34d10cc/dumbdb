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

func debugLeaf(node *BTreeNode) {
	if node.len() > 10 {
		for idx := 0; idx < 5; idx++ {
			k, v := node.getLeaf(idx)
			fmt.Printf("(%v, %v), ", k, v)
		}

		fmt.Printf(" ... ")
		for idx := node.len() - 5; idx < node.len(); idx++ {
			k, v := node.getLeaf(idx)
			fmt.Printf(", (%v, %v)", k, v)
		}
	} else {
		for idx := 0; idx < node.len(); idx++ {
			k, v := node.getLeaf(idx)
			fmt.Printf("(%v, %v), ", k, v)
		}
	}
}

func debugTree(t *testing.T, root *BTreeNode, pager *Pager) {
	for i := 0; i < root.len(); i++ {
		key, id := root.getBranch(i)
		fmt.Printf("%v) %v -> %v: ", i, key, id)

		page, err := pager.FetchPage(id)
		if err != nil {
			t.Fatal(err)
		}

		node := readNode(page)
		if !node.isLeaf {
			fmt.Println("subtree:")
			debugTree(t, &node, pager)
		} else {
			fmt.Printf("(prev: %v, next: %v): ", node.prev, node.next)

			debugLeaf(&node)
			fmt.Println()
		}
	}

	if root.next != InvalidPageID {
		page, err := pager.FetchPage(root.next)
		if err != nil {
			t.Fatal(err)
		}

		node := readNode(page)
		if !node.isLeaf {
			fmt.Println("rightmost subtree:")
			debugTree(t, &node, pager)
		} else {
			fmt.Printf("%v) above          (prev: %v, next: %v): ", root.len(), node.prev, node.next)
			debugLeaf(&node)
			fmt.Println()
		}
	}
}

func TestInsert(t *testing.T) {
	storage := &MemoryStorage{
		data: make([]byte, 0),
	}
	pager, err := NewPager(5, storage)
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

	const nEntries = 4096

	tree := NewBTree(root, pager)
	// insert high keys
	for key := nEntries - 1; key >= nEntries/2; key-- {
		val := key * 2
		err = tree.Insert(BTreeKey(key), BTreeValue(val))
		if err != nil {
			t.Fatal(err)
		}
	}

	debugTree(t, &tree.root, tree.pager)

	// insert low keys
	for key := 0; key < nEntries/2; key++ {
		val := key * 2
		err = tree.Insert(BTreeKey(key), BTreeValue(val))
		if err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("---------------------------------")
	debugTree(t, &tree.root, tree.pager)

	const searchFrom = 0
	c := tree.Search(BTreeKey(searchFrom))
	defer c.Close()
	for i := searchFrom; i < nEntries; i++ {
		if c.Err() != nil {
			t.Fatal(c.Err())
		}

		key, val := c.Get()
		if key != BTreeKey(i) {
			t.Fatalf("Unexpected key at %v: %v %v\n", i, key, val)
		}

		isNotLast := i+1 != nEntries
		movedForward := c.Forward()
		if isNotLast != movedForward {
			if !movedForward {
				t.Fatalf("Unexpected end of cursor: %v at %v\n", c.Err(), i)
			} else {
				t.Fatalf("Cursor moved past the end: %v at %v\n", c.Err(), i)
			}
		}
	}
}
