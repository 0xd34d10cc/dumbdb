package main

import (
	"dumbdb"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/olekukonko/tablewriter"
)

func formatTable(result *dumbdb.Result, w io.Writer) {
	writer := tablewriter.NewWriter(w)
	writer.SetHeader(result.Schema.ColumnNames())

	text := make([]string, 0, 3)
	for _, row := range result.Rows {
		for _, field := range row {
			text = append(text, field.String())
		}

		writer.Append(text)
		text = text[:0]
	}
	writer.Render()
}

func receiveResponse(conn net.Conn) (*dumbdb.Result, error) {
	response, err := dumbdb.RecvMessage(conn)
	if err != nil {
		return nil, err
	}

	if len(response) == 0 {
		return nil, nil
	}

	var result dumbdb.Result
	err = json.Unmarshal(response, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func runCLI(history string, conn net.Conn) {
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
		query, err := rl.Readline()
		if err != nil {
			break
		}

		query = strings.TrimSpace(query)
		if len(query) == 0 {
			continue
		}

		err = dumbdb.SendMessage(conn, []byte(query))
		if err != nil {
			log.Fatal("Failed to send query:", err)
		}

		response, err := receiveResponse(conn)
		if err != nil {
			log.Fatal("Failed to receive resposne:", err)
		}

		if response != nil {
			formatTable(response, os.Stdout)
		}
	}
}

func main() {
	addr := flag.String("addr", "localhost:1337", "address of the server")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatal("Failed to connect to server", err)
	}
	defer conn.Close()

	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	history := filepath.Join(currentDir, "history.txt")
	runCLI(history, conn)
}
