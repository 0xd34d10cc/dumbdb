package main

import (
	"context"
	"dumbdb"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
)

func readQuery(conn net.Conn) (string, error) {
	message, err := dumbdb.RecvMessage(conn)
	if err != nil {
		return "", err
	}
	return string(message), err
}

func handleClient(db *dumbdb.Database, conn net.Conn) {
	defer conn.Close()
	for {
		query, err := readQuery(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("[%v] Connection closed\n", conn.RemoteAddr())
				break
			}

			log.Printf("[%v] Failed to receive query: %v\n", conn.RemoteAddr(), err)
			break
		}

		q, err := dumbdb.ParseQuery(query)
		if err != nil {
			// TODO: send error in response instead
			log.Printf("[%v] Failed to parse query: %v\n", conn.RemoteAddr(), err)
			break
		}

		log.Printf("[%v] Running \"%v\"\n", conn.RemoteAddr(), query)

		result, err := db.Execute(q)
		if err != nil {
			// TODO: send error in response instead
			log.Printf("[%v] Failed to process query: %v\n", conn.RemoteAddr(), err)
			break
		}

		var data []byte
		if result != nil {
			data, err = json.Marshal(result)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			data = []byte("")
		}

		err = dumbdb.SendMessage(conn, data)
		if err != nil {
			log.Printf("[%v] Failed to send response: %v\n", conn.RemoteAddr(), err)
			break
		}
	}
}

func runServer(ctx context.Context, db *dumbdb.Database, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		// close listener to stop the loop below
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}

			return err
		}

		log.Printf("[%v] Connected\n", conn.RemoteAddr())

		// TODO: pass ctx to handleClient()
		go handleClient(db, conn)
	}
}

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal("Failed to get cwd:", err)
	}

	dataDir := flag.String("data", cwd, "data directory")
	addr := flag.String("addr", "localhost:1337", "address to bind to")
	flag.Parse()

	db, err := dumbdb.NewDatabase(*dataDir)
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

	log.Println("Starting on", *addr)

	ctx, cancel := context.WithCancel(context.Background())

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
	}()

	err = runServer(ctx, db, *addr)
	if err != nil {
		log.Fatal("Server error:", err)
	}

	log.Println("Closed successfully")
}
