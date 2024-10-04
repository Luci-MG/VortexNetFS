package client

import (
	"VNFS/pkg/communication"
	"VNFS/pkg/communication/tcp"
	"fmt"
	"net"
)

// TCPClient this client will connect to remote tcp server in another node.
type TCPClient struct {
	ServerAddress string
	NodeId        uint64
	Conn          net.Conn
}

// NewTCPClient creates new client instance.
func NewTCPClient(serverAdress string, nodeId uint64) (*TCPClient, error) {
	conn, err := net.Dial("tcp", serverAdress)
	if err != nil {
		return nil, fmt.Errorf("error connecting to %s: %v", serverAdress, err)
	}
	return &TCPClient{
		ServerAddress: serverAdress,
		NodeId:        nodeId,
		Conn:          conn,
	}, nil
}

// SendMesasge sends message
func (c *TCPClient) SendMesasge(msg communication.Message) error {
	if err := tcp.SendMessage(c.Conn, &msg); err != nil {
		return err
	}
	return nil
}

// ReceiveMessage receives message
func (c *TCPClient) ReceiveMessage() (*communication.Message, error) {
	msg, err := tcp.ReceiveMessage(c.Conn)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// Close closes the client
func (c *TCPClient) Close() {
	if c.Conn != nil {
		c.Conn.Close()
	}
}
