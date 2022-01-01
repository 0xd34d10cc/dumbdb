package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
)

func RunCLI(history string, db *Database) {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:      "> ",
		HistoryFile: history,
	})
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

		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
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

func main() {
	dataDir, err := os.Getwd()
	if len(os.Args) > 2 || len(os.Args) != 2 && err != nil {
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

	history := filepath.Join(dataDir, "history.txt")
	RunCLI(history, db)
}
