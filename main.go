package main

import (
	"log"
	"os"

	"github.com/chzyer/readline"
)

func main() {
	dbFile, err := os.OpenFile("data.bin", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal("Failed to create db file:", err)
	}
	defer dbFile.Close()

	pager, err := NewPager(dbFile)
	if err != nil {
		log.Fatal("Faield to create buffer pool:", err)
	}
	defer func() {
		err := pager.SyncAll()
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

		query, err := Parse(line)
		if err != nil {
			log.Println("Failed to parse query:", err)
			continue
		}

		log.Println("Parsed:", query)
	}
}
