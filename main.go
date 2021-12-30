package main

import (
	"fmt"
	"log"

	"github.com/chzyer/readline"
)

func main() {
	db := NewDatabase()
	defer db.Close()

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

		fmt.Println(result)
	}
}
