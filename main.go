package main

import (
	"fmt"
	"log"
	"os"

	"github.com/chzyer/readline"
)

func main() {
	dbFile, err := os.OpenFile("data.bin", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal("Failed to create db file:", err)
	}

	pool, err := NewBufferPool(dbFile)
	if err != nil {
		log.Fatal("Faield to create buffer pool:", err)
	}
	defer func() {
		err := pool.SyncAll()
		if err != nil {
			log.Println("Failed to sync pages:", err)
		}
	}()

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

		fmt.Println("got it:", line)
	}

	// id, err := pool.AllocatePage()
	// if err != nil {
	// 	log.Fatal("Failed to allocate page:", err)
	// }

	// log.Println("Allocated page at:", id)
	// for id = pool.FirstPage(); id != InvalidPageID; id = pool.NextPage(id) {
	// 	log.Println("Allocated page:", id)
	// }
}
