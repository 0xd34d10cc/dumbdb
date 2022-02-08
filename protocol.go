package dumbdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
)

func SendMessage(conn net.Conn, message []byte) error {
	var lenbuf [4]byte
	binary.LittleEndian.PutUint32(lenbuf[:], uint32(len(message)))
	n, err := conn.Write(lenbuf[:])
	if err != nil {
		return err
	}

	if n != len(lenbuf) {
		return errors.New("partial write (len)")
	}

	sent := 0
	for sent < len(message) {
		n, err = conn.Write(message[sent:])
		if err != nil {
			return err
		}

		if n == 0 {
			return errors.New("connection closed")
		}

		sent += n
	}

	return nil

}

func RecvMessage(conn net.Conn) ([]byte, error) {
	var lenbuf [4]byte
	_, err := io.ReadFull(conn, lenbuf[:])
	if err != nil {
		return nil, err
	}

	responseLen := binary.LittleEndian.Uint32(lenbuf[:])
	if responseLen == 0 {
		// success, but no data
		return nil, nil
	}

	response := make([]byte, responseLen)
	_, err = io.ReadFull(conn, response)
	return response, err
}

type ResponseChunk struct {
	Schema Schema
	Rows   []Row
}

type Response struct {
	Result *ResponseChunk `json:",omitempty"`
	Error  string         `json:",omitempty"`
}

func SendResponse(conn net.Conn, response *Response) error {
	message, err := json.Marshal(response)
	if err != nil {
		return err
	}

	return SendMessage(conn, message)
}

func ReceiveResponse(conn net.Conn) (*Response, error) {
	response, err := RecvMessage(conn)
	if err != nil {
		return nil, err
	}

	if len(response) == 0 {
		return nil, nil
	}

	var result Response
	err = json.Unmarshal(response, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}
