package tcp

import (
	"VNFS/pkg/communication"
	"encoding/json"
	"fmt"
	"net"
)

// TODO:: use encoding and decoding to decrease the payload size

// Serialize Seralizer for the message
func Serialize(m *communication.Message) ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// DeSerializeMessage DeSeralizer for the message
func DeSerializeMessage(conn net.Conn) (*communication.Message, error) {
	buffer := make([]byte, 5*1024*1024+22)
	n, err := conn.Read(buffer)
	if err != nil {
		return nil, err
	}
	var msg communication.Message
	err = json.Unmarshal(buffer[:n], &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// SendMessage Writes the message to conn
func SendMessage(conn net.Conn, msg *communication.Message) error {
	data, err := Serialize(msg)
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

// ReceiveMessage Receives the message
func ReceiveMessage(conn net.Conn) (*communication.Message, error) {
	msg, err := DeSerializeMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("error in receving message: %v", err)
	}
	return msg, err
}
