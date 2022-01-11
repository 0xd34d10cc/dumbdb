package dumbdb

import (
	"encoding/binary"
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
