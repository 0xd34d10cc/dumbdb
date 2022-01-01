package main

import (
	"fmt"
	"os"

	"github.com/chzyer/readline"
)

func main() {
	dataDir := "."
	if len(os.Args) > 2 {
		fmt.Println("Usage: db <data dir>")
		return
	}

	if len(os.Args) == 2 {
		dataDir = os.Args[1]
	}

	db, err := NewDatabase(dataDir)
	if err != nil {
		fmt.Println("Failed to initialize database:", err)
		return
	}
	defer func() {
		err := db.Close()
		if err != nil {
			fmt.Println("Failed to close db:", err)
		}
	}()

	rl, err := readline.New("> ")
	if err != nil {
		fmt.Println("Failed to initialize readline", err)
		return
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}

		query, err := ParseQuery(line)
		if err != nil {
			fmt.Println("Failed to parse query:", err)
			continue
		}

		result, err := db.Execute(query)
		if err != nil {
			fmt.Println("Failed to process query:", err)
			continue
		}

		if result != nil {
			result.FormatTable(os.Stdout)
		}
	}
}
