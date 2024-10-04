package replication

// import (
// 	"VNFS/pkg/communication"
// 	"VNFS/pkg/communication/client"
// 	"VNFS/pkg/dht"
// 	"VNFS/pkg/storage"
// 	"fmt"
// 	"time"
// )

// // replicaManager is responsible for managing replication of chunks to other nodes.
// type ReplicaManager struct {
// 	NodeId    uint64
// 	DHT       *dht.DHT
// 	Storage   *storage.LocalStorageEngine
// 	Peers     map[uint64]string
// 	retries   int
// 	retryWait time.Duration
// }

// // NewReplicaManager creates a new ReplicaManager instance.
// func NewReplicaManager(nodeId uint64, dht *dht.DHT, storage *storage.LocalStorageEngine, peers map[uint64]string) *ReplicaManager {
// 	return &ReplicaManager{
// 		NodeId:    nodeId,
// 		DHT:       dht,
// 		Storage:   storage,
// 		Peers:     peers,
// 		retries:   3,
// 		retryWait: 2 * time.Second,
// 	}
// }

// // ReplicateChunk replicates the chunk to the given replica nodes asynchronously.
// func (r *ReplicaManager) ReplicateChunk(chunkId string, chunkData []byte, replicaNodes []uint64) {
// 	go func() {
// 		for _, replicaNode := range replicaNodes {
// 			peerAddress := r.Peers[replicaNode]
// 			err := r.sendChunkToNode(chunkId, chunkData, peerAddress)
// 			if err != nil {
// 				fmt.Printf("ReplicaManager: Failed to send chunk %s to node %d, retrying...\n", chunkId, replicaNode)
// 				r.retryReplication(chunkId, chunkData, peerAddress)
// 			}
// 		}
// 	}()
// }

// // sendChunkToNode sends a chunk to a specific peer node.
// func (r *ReplicaManager) sendChunkToNode(chunkId string, chunkData []byte, peerAddress string) error {
// 	client := client.NewTCPClient(peerAddress, r.NodeId)
// 	if client == nil {
// 		return fmt.Errorf("ReplicaManager: failed to connect to peer at address %s", peerAddress)
// 	}
// 	defer client.Close()

// 	msg := &communication.Message{
// 		Type:    uint8(communication.MessageTypeSaveChunk),
// 		NodeId:  r.NodeId,
// 		Payload: append([]byte(chunkId), chunkData...),
// 	}

// 	err := client.SendMesasge(*msg)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// // retryReplication handles retry logic for failed replications.
// func (r *ReplicaManager) retryReplication(chunkId string, chunkData []byte, peerAddress string) {
// 	for attempt := 1; attempt <= r.retries; attempt++ {
// 		fmt.Printf("ReplicaManager: Retry %d for chunk %s to node at address %s\n", attempt, chunkId, peerAddress)
// 		err := r.sendChunkToNode(chunkId, chunkData, peerAddress)
// 		if err == nil {
// 			fmt.Printf("ReplicaManager: Successfully replicated chunk %s on retry %d\n", chunkId, attempt)
// 			return
// 		}
// 		time.Sleep(r.retryWait)
// 	}
// 	fmt.Printf("ReplicaManager: Failed to replicate chunk %s after %d retries\n", chunkId, r.retries)
// }
