package node

import (
	"VNFS/pkg/dht"
	"VNFS/pkg/managers/heartbeat"
	"VNFS/pkg/server"
	"VNFS/pkg/storage"
	"VNFS/pkg/wal"
	"fmt"
	"log"
	"os"
	"time"
)

type NodeConfig struct {
	NodeId             uint64            `json:"nodeId"`             // Unique node identifier
	Address            string            `json:"address"`            // Node's address
	Peers              map[uint64]string `json:"peers"`              // List of peer nodes (NodeId -> Address)
	StoragePath        string            `json:"storagePath"`        // Path for local storage
	WALFilePath        string            `json:"walFilePath"`        // Path for WAL file
	LogFilePath        string            `json:"logFilePath"`        // Path for log file
	IsRingTopology     bool              `json:"isRingTopology"`     // Whether to use consistent hash ring (true) or all-to-all (false)
	ReplicationFactor  int               `json:"replicationFactor"`  // Number of replicas for chunks
	HeartbeatInterval  time.Duration     `json:"heartbeatInterval"`  // Interval between heartbeats
	HeartbeatThreshold time.Duration     `json:"heartbeatThreshold"` // Timeout before a node is considered failed
}

// Node represents a running node in the DFS
type Node struct {
	Config    *NodeConfig
	Server    *server.Server
	Storage   *storage.LocalStorageEngine
	DHT       *dht.DHT
	WAL       *wal.WAL
	Heartbeat *heartbeat.HeartBeatManager
	logger    *log.Logger
	logFile   *os.File
}

// NewNode creates a new node instance based on the provided config
func NewNode(config *NodeConfig) (*Node, error) {
	// Initialize logger
	var logFile *os.File
	var logger *log.Logger

	if config.LogFilePath != "" {
		var err error
		logFile, err = os.OpenFile(config.LogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %v", err)
		}
		logger = log.New(logFile, "", log.LstdFlags)
	} else {
		logger = log.New(os.Stdout, "", log.LstdFlags)
	}
	logger.Printf("Initializing Node %d at %s...\n", config.NodeId, config.Address)

	// Initialize the local storage engine
	localStorage := storage.NewLocalStorageEngine(config.StoragePath)
	logger.Printf("Local storage engine initialized at %s\n", config.StoragePath)

	// Initialize the DHT
	dhtInstance := dht.NewDHT()
	logger.Println("DHT initialized.")

	// Initialize the WAL (Write Ahead Log) // 64mb (small) 246mb (large)
	walInstance, err := wal.NewWAL(config.WALFilePath, 64*1024*1024)
	if err != nil {
		logger.Printf("Error initializing WAL: %v\n", err)
		return nil, fmt.Errorf("failed to initialize WAL: %v", err)
	}
	logger.Printf("WAL initialized at %s\n", config.WALFilePath)

	// Initialize Heartbeat Manager TODO:: server does this or node
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 30 * time.Second
	}
	if config.HeartbeatThreshold == 0 {
		config.HeartbeatThreshold = 45 * time.Second
	}
	heartbeatManager := heartbeat.NewHeartBeatManager(config.HeartbeatInterval, config.HeartbeatThreshold, logger)
	logger.Println("Heartbeat Manager initialized.")

	// Create the Server with the necessary components
	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = 0
	}
	server := server.NewServer(
		config.Address,
		config.NodeId,
		localStorage,
		dhtInstance,
		walInstance,
		heartbeatManager,
		config.Peers,
		config.IsRingTopology,
		config.ReplicationFactor,
		logger,
	)
	logger.Println("TCP Server initialized.")

	return &Node{
		Config:    config,
		Server:    server,
		Storage:   localStorage,
		DHT:       dhtInstance,
		WAL:       walInstance,
		Heartbeat: heartbeatManager,
		logger:    logger,
		logFile:   logFile,
	}, nil
}

// Start starts the node and its server
func (n *Node) Start() error {
	n.logger.Printf("Starting Node %d on %s...\n", n.Config.NodeId, n.Config.Address)
	err := n.Server.Start()
	if err != nil {
		n.logger.Printf("Error starting server: %v\n", err)
		return fmt.Errorf("failed to start server: %v", err)
	}
	n.logger.Printf("Node %d is up and running on %s\n", n.Config.NodeId, n.Config.Address)
	return nil
}

// Shutdown gracefully shuts down the node
func (n *Node) Shutdown() {
	fmt.Printf("Shutting down node %d with %s...\n", n.Config.NodeId, n.Config.Address)

	n.logger.Println("Stopping server...")
	n.logger.Println("Closing WAL...")
	if err := n.WAL.Close(); err != nil {
		n.logger.Printf("Error closing WAL: %v\n", err)
	} else {
		n.logger.Println("WAL closed successfully.")
	}

	n.logger.Println("Closing storage engine...")
	// Need to close storage engine

	n.logger.Println("Stopping Heartbeat Manager...")
	// Need to stop HBM

	if n.logFile != nil {
		n.logger.Println("Closing log file...")
		if err := n.logFile.Close(); err != nil {
			fmt.Printf("Error closing log file: %v\n", err)
		} else {
			n.logger.Println("Log file closed successfully.")
		}
	}

	// Need to add more logic ....

	n.logger.Printf("Node %d shut down successfully.\n", n.Config.NodeId)
}
