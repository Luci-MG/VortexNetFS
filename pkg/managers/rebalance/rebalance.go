package rebalance

// import (
// 	"VNFS/pkg/communication"
// 	"VNFS/pkg/server"
// 	"fmt"
// 	"sync"
// 	"time"
// )

// type RebalanceManager struct {
// 	server *server.TCPServer
// 	mu     sync.Mutex
// }

// // NewRebalanceManager creates a new Rebalance Manager
// func NewRebalanceManager(server *server.TCPServer) *RebalanceManager {
// 	return &RebalanceManager{server: server}
// }

// // Triggers rebalancing when a new node joins the network
// func (rm *RebalanceManager) rebalanceOnNodeJoin(newNodeId uint64) {
// 	rm.mu.Lock()
// 	defer rm.mu.Unlock()

// 	fmt.Printf("Rebalance Manager: Rebalancing chunks after Node %d joined...\n", newNodeId)

// 	// Iterate through all files and their metadata
// 	for _, fileMetaData := range rm.server.DHT.FileMetaDataDHT {
// 		for _, chunkId := range fileMetaData.Chunks {
// 			primaryNode, _ := rm.server.DeterminedNodesForChunk(chunkId)

// 			if primaryNode == newNodeId {
// 				// This chunk should now be owned by the new node, move it
// 				fmt.Printf("Rebalance Manager: Moving chunk %s to new Node %d\n", chunkId, newNodeId)
// 				err := rm.server.CreateTCPClientAndSendRequest(rm.server.Peers[newNodeId], uint8(communication.MessageTypeSaveChunk), []byte(chunkId))
// 				if err != nil {
// 					fmt.Printf("Rebalance Manager: Failed to move chunk %s to Node %d: %v\n", chunkId, newNodeId, err)
// 				}
// 			}
// 		}
// 	}
// 	fmt.Printf("Rebalance Manager: Rebalancing complete for Node %d join.\n", newNodeId)
// }

// // Triggers rebalancing when a node leaves the network
// func (rm *RebalanceManager) rebalanceOnNodeLeave(leavingNodeId uint64) {
// 	rm.mu.Lock()
// 	defer rm.mu.Unlock()

// 	fmt.Printf("Rebalance Manager: Rebalancing chunks after Node %d left...\n", leavingNodeId)

// 	// Iterate through all files and their metadata
// 	for _, fileMetaData := range rm.server.DHT.FileMetaDataDHT {
// 		for _, chunkId := range fileMetaData.Chunks {
// 			nodes := rm.server.DHT.ChunkToNodeDHT[chunkId]

// 			if contains(nodes, leavingNodeId) {
// 				// Redistribute the chunk to other nodes
// 				newPrimaryNode, _ := rm.server.DeterminedNodesForChunk(chunkId)
// 				fmt.Printf("Rebalance Manager: Redistributing chunk %s to new Node %d\n", chunkId, newPrimaryNode)

// 				// Move the chunk to the new primary node
// 				err := rm.server.CreateTCPClientAndSendRequest(rm.server.Peers[newPrimaryNode], uint8(communication.MessageTypeSaveChunk), []byte(chunkId))
// 				if err != nil {
// 					fmt.Printf("Rebalance Manager: Failed to redistribute chunk %s to Node %d: %v\n", chunkId, newPrimaryNode, err)
// 				}

// 				// Update DHT to remove leaving node's ownership of the chunk
// 				rm.server.DHT.RemoveChunkMapping(chunkId, leavingNodeId)
// 			}
// 		}
// 	}

// 	fmt.Printf("Rebalance Manager: Rebalancing complete for Node %d leave.\n", leavingNodeId)
// }

// // Helper function to check if a node exists in the list
// func contains(nodes []uint64, nodeId uint64) bool {
// 	for _, n := range nodes {
// 		if n == nodeId {
// 			return true
// 		}
// 	}
// 	return false
// }

// // Background rebalance manager that runs periodically
// func (rm *RebalanceManager) RunPeriodicRebalance(interval time.Duration) {
// 	go func() {
// 		for {
// 			time.Sleep(interval)
// 			fmt.Println("Rebalance Manager: Running periodic rebalance...")

// 			// Logic for running periodic rebalancing (e.g., checking if chunks need redistribution)
// 			rm.checkForImbalances()

// 			fmt.Println("Rebalance Manager: Periodic rebalance complete.")
// 		}
// 	}()
// }

// // Check for imbalances in chunk distribution and trigger rebalancing if necessary
// func (rm *RebalanceManager) checkForImbalances() {
// 	rm.mu.Lock()
// 	defer rm.mu.Unlock()

// 	chunkDistribution := make(map[uint64]int)

// 	// Count the number of chunks each node holds
// 	for _, chunkOwners := range rm.server.DHT.ChunkToNodeDHT {
// 		for _, nodeId := range chunkOwners {
// 			chunkDistribution[nodeId]++
// 		}
// 	}

// 	// Find the average number of chunks per node
// 	totalChunks := 0
// 	for _, count := range chunkDistribution {
// 		totalChunks += count
// 	}
// 	avgChunks := totalChunks / len(chunkDistribution)

// 	// If a node has significantly more or fewer chunks than the average, trigger rebalancing
// 	for nodeId, count := range chunkDistribution {
// 		if count > avgChunks*2 || count < avgChunks/2 {
// 			fmt.Printf("Rebalance Manager: Node %d has imbalanced chunks (%d vs avg %d). Triggering rebalance.\n", nodeId, count, avgChunks)

// 			// Rebalance node
// 			rm.rebalanceNodeChunks(nodeId)
// 		}
// 	}
// }

// // Rebalances chunks for a specific node if it has too many or too few chunks
// func (rm *RebalanceManager) rebalanceNodeChunks(nodeId uint64) {
// 	fmt.Printf("Rebalance Manager: Rebalancing chunks for Node %d due to imbalance...\n", nodeId)

// 	// Iterate through all chunks and redistribute them if necessary
// 	for chunkId, chunkOwners := range rm.server.DHT.ChunkToNodeDHT {
// 		if contains(chunkOwners, nodeId) {
// 			primaryNode, _ := rm.server.DeterminedNodesForChunk(chunkId)

// 			if primaryNode != nodeId {
// 				fmt.Printf("Rebalance Manager: Redistributing chunk %s from Node %d to Node %d\n", chunkId, nodeId, primaryNode)
// 				err := rm.server.CreateTCPClientAndSendRequest(rm.server.Peers[primaryNode], uint8(communication.MessageTypeSaveChunk), []byte(chunkId))
// 				if err != nil {
// 					fmt.Printf("Rebalance Manager: Failed to redistribute chunk %s from Node %d to Node %d: %v\n", chunkId, nodeId, primaryNode, err)
// 				}

// 				// Remove the chunk from the node
// 				rm.server.DHT.RemoveChunkMapping(chunkId, nodeId)
// 			}
// 		}
// 	}
// 	fmt.Printf("Rebalance Manager: Completed rebalancing for Node %d.\n", nodeId)
// }
