package main

import (
	"VNFS/internal/node"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func main() {
	// Parse command-line arguments for the config file path.
	configFilePath := flag.String("config", "configs/config.json", "Path to the node config file")
	flag.Parse()

	// Load the node configuration.
	NodeConfig := loadConfig(*configFilePath)
	nodeInstance, err := node.NewNode(NodeConfig)
	if err != nil {
		fmt.Printf("Error creating node: %v\n", err)
		os.Exit(1)
	}

	// Letssssss Start the node :)
	fmt.Printf("Starting Node %d at address %s...\n", NodeConfig.NodeId, NodeConfig.Address)
	err = nodeInstance.Start()
	if err != nil {
		fmt.Printf("Error starting node: %v\n", err)
		os.Exit(1)
	}

	// Graceful shutdown :)
	defer func() {
		fmt.Println("Shutting down node...")
		nodeInstance.Shutdown()
	}()
}

// Helper function to load config file
func loadConfig(configFile string) *node.NodeConfig {
	file, err := os.Open(configFile)
	if err != nil {
		fmt.Printf("Failed to open config file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	nodeConfig := &node.NodeConfig{}
	if err := decoder.Decode(nodeConfig); err != nil {
		fmt.Printf("Error decoding config file: %v\n", err)
		os.Exit(1)
	}
	return nodeConfig
}
