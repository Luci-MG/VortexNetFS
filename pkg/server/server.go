package server

import (
	"VNFS/pkg/communication"
	"VNFS/pkg/communication/client"
	"VNFS/pkg/communication/tcp"
	"VNFS/pkg/consistenthash"
	"VNFS/pkg/dht"
	"VNFS/pkg/managers/heartbeat"
	"VNFS/pkg/storage"
	"VNFS/pkg/storage/chunker"
	"VNFS/pkg/storage/file"
	"VNFS/pkg/wal"
	"fmt"
	"log"
	"net"
	"slices"
	"strings"
	"sync"
	"time"
)

// TODO:: handle the ring for all the cases
// TODO:: rebalancemanager integration
// TODO:: replicationmanager integration
// TODO:: disastermanager integration

// Server => this is a node tcp server.
type Server struct {
	Address        string
	NodeId         uint64
	Storage        *storage.LocalStorageEngine
	DHT            *dht.DHT
	WAL            *wal.WAL
	HeartBeat      *heartbeat.HeartBeatManager
	Peers          map[uint64]string
	ConnectedPeers map[uint64]string
	Ring           *consistenthash.HashRing
	IsRing         bool
	FileIdGen      *file.FileIdGenerator
	mutex          sync.Mutex
	logger         *log.Logger
}

// NewServer creates a new tcp server instance.
func NewServer(
	address string,
	nodeId uint64,
	storage *storage.LocalStorageEngine,
	dht *dht.DHT,
	wal *wal.WAL,
	heartbeat *heartbeat.HeartBeatManager,
	peers map[uint64]string,
	isring bool,
	replicationFactor int,
	logger *log.Logger) *Server {
	server := &Server{
		Address:        address,
		NodeId:         nodeId,
		Storage:        storage,
		DHT:            dht,
		WAL:            wal,
		HeartBeat:      heartbeat,
		Peers:          peers,
		ConnectedPeers: map[uint64]string{},
		Ring:           consistenthash.NewHashRing(replicationFactor),
		IsRing:         isring,
		FileIdGen:      file.NewFileIdGenerator(nodeId),
		logger:         logger,
	}
	// server.RebalanceManager = rebalance.NewRebalanceManager(server)
	server.logger.Println("Server initialized...")
	return server
}

// Listen to address
func (s *Server) Start() error {
	s.logger.Println("Starting server...")
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		s.logger.Fatalf("Error starting listener: %v\n", err)
		return err
	}
	defer listener.Close()
	s.logger.Printf("Listening on %s\n", s.Address)

	// Recover state from WAL
	// if err := s.recoverStateFromWAL(); err != nil {
	// 	s.logger.Fatalf("WAL recovery failed: %v\n", err)
	// 	return err
	// }

	// Channels for heartbeat and monitoring
	heartBeatReady := make(chan bool)
	monitorReady := make(chan bool)

	// Form the network
	go s.formNetwork(heartBeatReady)
	// Start sending heartbeats
	go s.sendHeartBeats(heartBeatReady, monitorReady)
	// Monitor the network nodes
	go s.monitorNodeHealth(monitorReady)

	// start the managers
	// s.RebalanceManager.RunPeriodicRebalance(10 * time.Second)

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.logger.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

// recoverStateFromWAL recovers the DHT, file metadata, and chunks from WAL.
func (s *Server) recoverStateFromWAL() error {
	s.logger.Println("Recovering state from WAL...")

	recoverFunc := func(entry wal.LogEntry) error {
		switch entry.OperationType {
		case "chunk_store":
			chunkId := entry.Details
			s.logger.Printf("Recovering chunk %s\n", chunkId)
			// TODO:: Recover chunk DHT mapping

		case "file_meta":
			fileId := entry.Details
			s.logger.Printf("Recovering file metadata %s\n", fileId)
			// TODO:: Recover file metadata DHT

		default:
			s.logger.Printf("Unknown log entry type: %s\n", entry.OperationType)
		}
		return nil
	}

	return s.WAL.RecoverWAL(recoverFunc)
}

// formNetwork sets up the node network by connecting with peers.
func (s *Server) formNetwork(heartBeatReady chan bool) {
	s.logger.Println("Forming the network...")
	for nodeId, nodeAddress := range s.Peers {
		s.Ring.AddNewNode(consistenthash.Node{NodeId: nodeId, Address: nodeAddress})
	}

	if s.IsRing {
		s.logger.Println("Using Ring topology with consistent hashing.")
		s.connectToRingNodes()
	} else {
		s.logger.Println("Using All-to-All topology.")
		s.connectToAllNodes()
	}

	heartBeatReady <- true
	err := s.WAL.WriteLogEntry(s.NodeId, "network_formation", "Network formed successfully")
	if err != nil {
		s.logger.Printf("Error writing WAL entry: %v\n", err)
	}
}

// monitorNodeHealth monitors the health of the nodes.
func (s *Server) monitorNodeHealth(monitorReady chan bool) {
	if <-monitorReady {
		s.logger.Println("Monitoring node health...")
		for {
			for nodeId := range s.ConnectedPeers {
				if s.HeartBeat.CheckFailedNode(nodeId) {
					s.logger.Printf("Node %d has failed. Starting recovery...\n", nodeId)
					s.handleNodeFailure(nodeId)
				}
			}
			time.Sleep(s.HeartBeat.HeartbeatInterval)
		}
	}
}

// sendHeartBeats sends periodic heartbeat messages to connected peers.
func (s *Server) sendHeartBeats(heartBeatReady chan bool, monitorReady chan bool) {
	if <-heartBeatReady {
		monitorReady <- true
		s.logger.Println("Sending heartbeats...")
		for {
			for peerId, peerAddress := range s.ConnectedPeers {
				if peerId != s.NodeId {
					client, err := client.NewTCPClient(peerAddress, s.NodeId)
					if err != nil {
						msg := &communication.Message{
							Type:    uint8(communication.MessageTypeHeartBeat),
							NodeId:  s.NodeId,
							Payload: []byte(fmt.Sprintf("heartbeat from node %d", s.NodeId)),
						}
						err = client.SendMesasge(*msg)
						if err != nil {
							s.logger.Printf("Failed to send heartbeat to node %d: %v\n", peerId, err)
						} else {
							s.logger.Printf("Sent heartbeat to node %d\n", peerId)
						}
					}
				}
			}
			time.Sleep(s.HeartBeat.HeartbeatInterval)
		}
	}
}

// handleConnection processes incoming TCP connections.
func (s *Server) handleConnection(conn net.Conn) error {
	defer conn.Close()

	for {
		msg, err := tcp.ReceiveMessage(conn)
		if err != nil {
			// s.logger.Printf("Error receiving message: %v\n", err)
			return err
		}

		// Routes messages based on their type
		switch msg.Type {
		case uint8(communication.MessageTypeJoin):
			s.handleJoin(msg)
		case uint8(communication.MessageTypeLeave):
			s.handleLeave(msg)
		case uint8(communication.MessageTypeHeartBeat):
			s.handleHeartBeat(msg)
		case uint8(communication.MessageTypeAck):
			s.handleACK(msg)
		case uint8(communication.MessageTypeSaveChunk):
			s.handleSaveChunk(msg)
		case uint8(communication.MessageTypeRetrieveChunk):
			s.handleRetrieveChunk(msg, conn)
		case uint8(communication.MessageTypeFileMetaData):
			s.handleFileMetaData(msg, conn)
		case uint8(communication.MessageTypeRetrieveFileMetaData):
			s.handleRetrieveFileMetaData(msg, conn)
		case uint8(communication.MessageTypeDHTSync):
			s.handleDHTSync(msg)
		case uint8(communication.MessageTypeRetrieveFile):
			s.handleRetrieveFile(msg, conn)
		default:
			s.logger.Printf("Unknown message type: %d\n", msg.Type)
		}
	}
}

// handleJoin processes the join request from other nodes.
func (s *Server) handleJoin(msg *communication.Message) error {
	s.logger.Printf("Received join request from Node %d\n", msg.NodeId)
	err := s.sendAck(msg.NodeId)
	if err != nil {
		return fmt.Errorf("failed to send acknowledgment to Node %d: %v", msg.NodeId, err)
	}

	if !containsNodeInPeers(s.Peers, msg.NodeId, string(msg.Payload)) {
		s.mutex.Lock()
		s.logger.Printf("Adding Node %d to the ring\n", msg.NodeId)
		s.Ring.AddNewNode(consistenthash.Node{NodeId: msg.NodeId, Address: s.Peers[msg.NodeId]})
		s.Peers[msg.NodeId] = string(msg.Payload)
		s.mutex.Unlock()
		// go s.RebalanceManager.rebalanceOnNodeJoin(msg.NodeId)
	}

	if s.IsRing {
		s.mutex.Lock()
		prevNode, nextNode := s.Ring.GetNeighbours(s.NodeId)
		s.ConnectedPeers = map[uint64]string{
			prevNode.NodeId: s.Peers[prevNode.NodeId],
			nextNode.NodeId: s.Peers[nextNode.NodeId],
		}
		s.mutex.Unlock()
	} else {
		s.mutex.Lock()
		s.ConnectedPeers[msg.NodeId] = string(s.Peers[msg.NodeId])
		s.mutex.Unlock()
	}

	s.logger.Printf("Added Node %d to ConnectedPeers Sent (ACK)\n", msg.NodeId)

	// we need to see about ring adding this node
	if err = s.WAL.WriteLogEntry(s.NodeId, "peer_joined", fmt.Sprintf("Node %d joined the network", msg.NodeId)); err != nil {
		return err
	}
	return nil
}

// sendAck sends an acknowledgment message.
func (s *Server) sendAck(nodeId uint64) error {
	peerAddress, exists := s.Peers[nodeId]
	if !exists {
		return fmt.Errorf("peer %d not found", nodeId)
	}

	err := s.CreateTCPClientAndSendRequest(peerAddress, uint8(communication.MessageTypeAck), []byte(fmt.Sprintf("ACK from Node %d", s.NodeId)))
	if err != nil {
		return err
	}

	s.logger.Printf("Sent acknowledgment to Node %d\n", nodeId)
	return nil
}

// handleLeave processes the leave request from other nodes.
func (s *Server) handleLeave(msg *communication.Message) error {
	s.logger.Printf("Node %d has left the network\n", msg.NodeId)
	if s.IsRing {
		s.mutex.Lock()
		s.Ring.RemoveNode(consistenthash.Node{NodeId: msg.NodeId})
		prevNode, nextNode := s.Ring.GetNeighbours(s.NodeId)
		s.ConnectedPeers = map[uint64]string{
			prevNode.NodeId: s.Peers[prevNode.NodeId],
			nextNode.NodeId: s.Peers[nextNode.NodeId],
		}
		s.mutex.Unlock()
	} else {
		s.mutex.Lock()
		s.Ring.RemoveNode(consistenthash.Node{NodeId: msg.NodeId})
		delete(s.ConnectedPeers, msg.NodeId)
		s.mutex.Unlock()
	}
	// go s.RebalanceManager.rebalanceOnNodeJoin(msg.NodeId)
	if err := s.WAL.WriteLogEntry(s.NodeId, "peer_left", fmt.Sprintf("Node %d left the network", msg.NodeId)); err != nil {
		return err
	}
	return nil
}

// handleHeartBeat processes the heartbeat messages.
func (s *Server) handleHeartBeat(msg *communication.Message) error {
	fmt.Printf("Node %d: Received heartbeat from the Node %d\n", s.NodeId, msg.NodeId)
	s.HeartBeat.UpdateLastSeen(msg.NodeId, s.NodeId)
	err := s.WAL.WriteLogEntry(s.NodeId, "heartbeat", fmt.Sprintf("Received heartbeat from Node %d", msg.NodeId))
	if err != nil {
		return err
	}
	return nil
}

// handleACK processes acknowledgment messages.
func (s *Server) handleACK(msg *communication.Message) error {
	s.logger.Printf("Received acknowledgment (ACK) from Node %d\n", msg.NodeId)
	s.mutex.Lock()
	s.ConnectedPeers[msg.NodeId] = string(s.Peers[msg.NodeId])
	s.mutex.Unlock()
	s.logger.Printf("Added Node %d to ConnectedPeers after ACK\n", msg.NodeId)

	if err := s.WAL.WriteLogEntry(s.NodeId, "ack_received", fmt.Sprintf("ACK received from Node %d", msg.NodeId)); err != nil {
		s.logger.Printf("Error writing WAL entry: %v\n", err)
	}
	return nil
}

// handleSaveChunk handles incoming requests to save file chunks.
func (s *Server) handleFileMetaData(msg *communication.Message, conn net.Conn) error {
	s.logger.Printf("Received file metadata from Node %d\n", msg.NodeId)

	fileMetaData := s.DHT.FileMetaDataFromBytes(msg.Payload)

	if msg.NodeId == s.NodeId {
		s.mutex.Lock()
		fileId := s.FileIdGen.GenerateFileId()
		fileMetaData.FileId = fileId
		s.mutex.Unlock()

		s.dhtUpdate(fileMetaData, fileId)
		err := s.WAL.WriteLogEntry(s.NodeId, "file_meta", fmt.Sprintf("File metadata for %s updated", fileId))
		if err != nil {
			s.logger.Printf("Error writing WAL entry for file metadata %s: %v\n", fileId, err)
		}

		for _, peerAddress := range s.ConnectedPeers {
			err := s.CreateTCPClientAndSendRequest(peerAddress, uint8(communication.MessageTypeFileMetaData),
				s.DHT.FileMetaDataToBytes(fileMetaData))
			if err != nil {
				return err
			}
		}

		filemsg := &communication.Message{
			Type:    uint8(communication.MessageTypeAck),
			NodeId:  s.NodeId,
			Payload: []byte(fileId),
		}
		tcp.SendMessage(conn, filemsg)
	} else {
		s.mutex.Lock()
		_, exists := s.DHT.GetFileMetaData(fileMetaData.FileId)
		s.mutex.Unlock()
		if exists {
			// if already exists then the request is only to update last chunkid
			s.logger.Printf("File metadata for %s exists, updating last chunk ID\n", fileMetaData.FileId)
			s.mutex.Lock()
			existingFileMetaData := s.DHT.FileMetaDataDHT[fileMetaData.FileId]
			existingFileMetaData.LastChunkId = fileMetaData.LastChunkId
			s.DHT.FileMetaDataDHT[fileMetaData.FileId] = existingFileMetaData
			s.mutex.Unlock()
		} else {
			// if not exists then to create the filemetadata and dht's
			s.logger.Printf("File metadata for %s does not exist, updating DHT\n", fileMetaData.FileId)
			go s.dhtUpdate(fileMetaData, fileMetaData.FileId)
		}
	}

	return nil
}

// handleRetrieveFileMetaData handles the retrieval of file metadata from nodes.
func (s *Server) handleRetrieveFileMetaData(msg *communication.Message, conn net.Conn) error {
	fileId := string(msg.Payload)
	s.logger.Printf("Received file metadata retrieval request for file %s from Node %d\n", fileId, msg.NodeId)

	// we need to process if its from client then we will send based on new connection or node connection
	s.mutex.Lock()
	fileMetaData, exists := s.DHT.FileMetaDataDHT[fileId]
	s.mutex.Unlock()

	if !exists {
		return fmt.Errorf("file metadata for file %s not found", fileId)
	}

	if msg.NodeId == s.NodeId {
		filemsg := &communication.Message{
			Type:    uint8(communication.MessageTypeAck),
			NodeId:  s.NodeId,
			Payload: s.DHT.FileMetaDataToBytes(fileMetaData),
		}
		err := tcp.SendMessage(conn, filemsg)
		if err != nil {
			s.logger.Printf("Error sending file metadata for file %s to Node %d: %v\n", fileId, msg.NodeId, err)
			return err
		}
	} else {
		// if requested by any other node to check for any miss match
		err := s.CreateTCPClientAndSendRequest(s.Peers[msg.NodeId], uint8(communication.MessageTypeFileMetaData),
			s.DHT.FileMetaDataToBytes(fileMetaData))
		if err != nil {
			return err
		}
	}

	if err := s.WAL.WriteLogEntry(s.NodeId, "file_meta_retrieved", fmt.Sprintf("File metadata for %s retrieved successfully", fileId)); err != nil {
		s.logger.Printf("Error writing WAL entry for file metadata retrieval %s: %v\n", fileId, err)
	}
	return nil
}

// dhtUpdate updates the DHT with the latest file metadata and logs to WAL.
func (s *Server) dhtUpdate(fileMetaData dht.FileMetaData, fileId string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	chunkIds := chunker.GenerateChunkIDs(fileId, fileMetaData.FileSize)
	primaryNodes := []uint64{}
	replicaNodes := []uint64{}

	for _, chunkId := range chunkIds {
		primaryNode, replicaNodeIds := s.DeterminedNodesForChunk(chunkId)

		if !slices.Contains(primaryNodes, primaryNode) {
			primaryNodes = append(primaryNodes, primaryNode)
		}

		for _, replica := range replicaNodeIds {
			if !slices.Contains(replicaNodes, replica) {
				replicaNodes = append(replicaNodes, replica)
			}
		}

		s.DHT.ChunkToNodeDHT[chunkId] = append([]uint64{primaryNode}, replicaNodeIds...)

		for _, node := range append([]uint64{primaryNode}, replicaNodeIds...) {
			s.DHT.NodeToChunkDHT[node] = append(s.DHT.NodeToChunkDHT[node], chunkId)
		}
	}

	fileMetaData.Chunks = chunkIds
	fileMetaData.PrimaryNodes = primaryNodes
	fileMetaData.ReplicaNodes = replicaNodes
	s.DHT.FileMetaDataDHT[fileId] = fileMetaData

	if err := s.WAL.WriteLogEntry(s.NodeId, "dht_update", fmt.Sprintf("DHT updated for file %s", fileId)); err != nil {
		s.logger.Printf("Error writing WAL entry for DHT update %s: %v\n", fileId, err)
	}
}

// handleSaveChunk processes incoming requests to save file chunks.
func (s *Server) handleSaveChunk(msg *communication.Message) error {
	s.logger.Printf("Received save chunk request from node %d\n", msg.NodeId)

	chunkId := string(msg.Payload[:24])
	fileId := extractFileIdFromChunkId(chunkId)

	s.mutex.Lock()
	fileMetaData, exists := s.DHT.FileMetaDataDHT[fileId]
	s.mutex.Unlock()

	if !exists {
		return fmt.Errorf("file metadata for file %s not found", fileId)
	}

	if _, exists := s.DHT.ChunkToNodeDHT[chunkId]; exists {
		chunkData := msg.Payload[24:]
		// TODO :: need to change this as our chunk to nodeid map would have already got created
		// TODO :: need to properly send if its ring structure
		primaryNode, replicaNodes := s.DeterminedNodesForChunk(chunkId)

		if primaryNode == s.NodeId {
			if err := s.Storage.SaveChunk(chunkId, chunkData); err != nil {
				s.logger.Printf("Error saving chunk %s: %v\n", chunkId, err)
				return err
			}

			for _, replicaNode := range replicaNodes {
				if err := s.CreateTCPClientAndSendRequest(s.ConnectedPeers[replicaNode],
					uint8(communication.MessageTypeSaveChunk),
					msg.Payload); err != nil {
					s.logger.Printf("Error sending chunk to replica node %d: %v\n", replicaNode, err)
				}
			}

			// s.ReplicaManager.ReplicateChunk(chunkId, chunkData, replicaNodes)

			fileMetaData.LastChunkId = chunkId
			s.mutex.Lock()
			s.DHT.FileMetaDataDHT[fileId] = fileMetaData
			s.mutex.Unlock()

			fileMetaData.Chunks = make([]string, 0) // just emptying the chunks to decrease payload size

			// send this dht to all of the peers
			for _, peerAddress := range s.ConnectedPeers {
				err := s.CreateTCPClientAndSendRequest(peerAddress, uint8(communication.MessageTypeFileMetaData),
					s.DHT.FileMetaDataToBytes(fileMetaData))
				if err != nil {
					s.logger.Printf("Error syncing file metadata with peer: %v\n", err)
				}
			}
		} else {
			// broadcast to the chunks which not belong to this node
			s.CreateTCPClientAndSendRequest(s.ConnectedPeers[primaryNode],
				uint8(communication.MessageTypeSaveChunk),
				msg.Payload)
		}

		if chunkId == fileMetaData.LastChunkId {
			s.logger.Printf("File %s upload completed successfully.\n", fileId)
		}

		err := s.WAL.WriteLogEntry(s.NodeId, "chunk_saved", fmt.Sprintf("Chunk %s successfully saved", chunkId))
		if err != nil {
			return err
		}
	} else {
		s.logger.Printf("Chunk %s not saved not a valid chunk\n", chunkId)
	}

	return nil
}

// handleRetrieveChunk processes requests for retrieving file chunks.
func (s *Server) handleRetrieveChunk(msg *communication.Message, conn net.Conn) error {
	chunkId := string(msg.Payload)
	s.logger.Printf("Received request for chunk %s from Node %d\n", chunkId, msg.NodeId)

	// TODO :: get this retries no from config
	chunkData, exists, err := s.retrieveChunkWithRetries(chunkId, 3)
	if exists {
		filemsg := &communication.Message{
			Type:    uint8(communication.MessageTypeRetrieveChunk),
			NodeId:  s.NodeId,
			Payload: chunkData,
		}

		err = tcp.SendMessage(conn, filemsg)
		if err != nil {
			s.logger.Printf("Error sending retrieved chunk %s to Node %d: %v\n", chunkId, msg.NodeId, err)
			return err
		}
		s.logger.Printf("Successfully sent chunk %s\n", chunkId)
	}
	return err
}

// retrieveChunk retrieves a chunk from local storage or requests from peers.
func (s *Server) retrieveChunk(chunkId string) ([]byte, bool, error) {
	// local check
	chunkData, exists, _ := s.Storage.RetrieveChunk(chunkId)
	if exists {
		s.logger.Printf("Chunk %s found locally on Node %d\n", chunkId, s.NodeId)
		return chunkData, true, nil
	}

	s.logger.Printf("Chunk %s not found locally, requesting from peers...\n", chunkId)

	// dht check to get nodes
	nodes, exists := s.DHT.GetNodeFromChunkDHT(chunkId)
	if !exists {
		return nil, false, fmt.Errorf("chunk %s not found in dht", chunkId)
	}

	// need to look out for circular requests
	chunkResponseChan := make(chan []byte)
	errChan := make(chan error)

	// need to handle case of if ring is selected we should contact neighnours
	for _, nodeId := range nodes {
		if nodeId != s.NodeId {
			go func(nodeId uint64) {
				peerAddress := s.Peers[nodeId]
				client, err := client.NewTCPClient(peerAddress, s.NodeId)
				if client == nil {
					errChan <- fmt.Errorf("failed to connect to node %d with: %v", nodeId, err)
					return
				}
				defer client.Close()
				requestMsg := &communication.Message{
					Type:    uint8(communication.MessageTypeRetrieveChunk),
					NodeId:  s.NodeId,
					Payload: []byte(chunkId),
				}

				err = client.SendMesasge(*requestMsg)
				if err != nil {
					errChan <- fmt.Errorf("error requesting chunk %s from node %d: %v", chunkId, nodeId, err)
					return
				}

				responseMsg, err := client.ReceiveMessage()
				if err != nil {
					errChan <- fmt.Errorf("error receiving chunk %s from node %d: %v", chunkId, nodeId, err)
					return
				}

				if responseMsg != nil && len(responseMsg.Payload) > 0 {
					chunkResponseChan <- responseMsg.Payload
				} else {
					errChan <- fmt.Errorf("received empty chunk data from node %d", nodeId)
				}
			}(nodeId)
		}
	}

	select {
	case chunkData = <-chunkResponseChan:
		s.logger.Printf("Successfully received chunk %s from a peer\n", chunkId)
		return chunkData, true, nil
	case err := <-errChan:
		s.logger.Printf("Error occurred while retrieving chunk: %v\n", err)
		return nil, false, err
	case <-time.After(5 * time.Second):
		return nil, false, fmt.Errorf("node %d: timeout waiting for chunk %s", s.NodeId, chunkId)
	}
}

// retrieveChunkWithRetries retries chunk retrieval a specified number of times.
func (s *Server) retrieveChunkWithRetries(chunkId string, maxRetries int) ([]byte, bool, error) {
	var chunkData []byte
	var exists bool
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		chunkData, exists, err = s.retrieveChunk(chunkId)
		if err == nil && exists {
			return chunkData, exists, nil
		}
		// TODO:: backoff retries must be done
		s.logger.Printf("Failed to retrieve chunk %s (Attempt %d/%d)\n", chunkId, attempt, maxRetries)
		time.Sleep(2 * time.Second)
	}

	return nil, false, fmt.Errorf("node %d: exhausted retries for chunk %s", s.NodeId, chunkId)
}

// handleRetrieveFile handles full file retrieval by streaming chunks to the requester.
func (s *Server) handleRetrieveFile(msg *communication.Message, conn net.Conn) error {
	fileId := string(msg.Payload[:18])
	s.logger.Printf("Received file retrieval request for file %s from Node %d\n", fileId, msg.NodeId)

	// TODO :: stream the chunks in sequence
	// Retrieve file metadata from DHT
	s.mutex.Lock()
	fileMetaData, exists := s.DHT.GetFileMetaData(fileId)
	s.mutex.Unlock()

	if !exists {
		return fmt.Errorf("file %s not found in DHT", fileId)
	}

	s.logger.Printf("Starting file transfer for file %s\n", fileId)

	chunkChan := make(chan []byte)
	errChan := make(chan error)

	// Stream each chunk of the file
	// TODO:: worker pool to handle the amount of chunks we can get to make it sequencing
	for _, chunkId := range fileMetaData.Chunks {
		go func(chunkId string) {
			chunkData, exists, err := s.retrieveChunkWithRetries(chunkId, 3)
			if !exists || err != nil {
				errChan <- fmt.Errorf("failed to retrieve chunk %s: %v", chunkId, err)
			}
			chunkChan <- chunkData
		}(chunkId)
	}

	for i := 0; i < len(fileMetaData.Chunks); i++ {
		select {
		case chunkData := <-chunkChan:
			_, err := conn.Write(chunkData)
			if err != nil {
				return fmt.Errorf("failed to send chunk %s: %v", fileMetaData.Chunks[i], err)
			}
			s.logger.Printf("Streamed chunk %s to client\n", fileMetaData.Chunks[i])
			time.Sleep(10 * time.Millisecond)
		case err := <-errChan:
			return fmt.Errorf("error retrieving chunk %s: %v", fileMetaData.Chunks[i], err)
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout while waiting for chunk retrieval")
		}
	}
	s.logger.Printf("Successfully streamed file %s\n", fileId)
	conn.Close()

	err := s.WAL.WriteLogEntry(s.NodeId, "file_retrieved", fmt.Sprintf("File %s retrieved successfully", fileId))
	if err != nil {
		s.logger.Printf("Error writing WAL entry for file retrieval %s: %v\n", fileId, err)
	}

	return nil
}

// (NOT IN USE) handleDHTSync processes DHT synchronization messages.
func (s *Server) handleDHTSync(msg *communication.Message) error {
	chunkId := string(msg.Payload)
	s.logger.Printf("Received DHT sync request for chunk %s from Node %d\n", chunkId, msg.NodeId)
	err := s.WAL.WriteLogEntry(s.NodeId, "dht_sync", fmt.Sprintf("DHT synced for chunk %s", chunkId))
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) CreateTCPClientAndSendRequest(address string,
	messageType uint8, payload []byte) error {
	client, err := client.NewTCPClient(address, s.NodeId)
	if client == nil {
		return fmt.Errorf("failed to connect to peer at address %s with: %v", address, err)
	}
	defer client.Close()
	msg := &communication.Message{
		Type:    messageType,
		NodeId:  s.NodeId,
		Payload: payload,
	}
	err = client.SendMesasge(*msg)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) DeterminedNodesForChunk(chunkId string) (primaryNode uint64, replicaNodes []uint64) {
	_, primaryNode = s.Ring.GetNodeForChunkKey(chunkId)
	replicaNodes = s.Ring.GetReplicaNodes(chunkId, s.Ring.ReplicationFactor)
	return primaryNode, replicaNodes
}

// connectToRingNodes connects the server to its immediate neighbors in the ring.
func (s *Server) connectToRingNodes() {
	retryCall := true
	neigh1, neigh2 := s.Ring.GetNeighbours(s.NodeId)
	for attempt := 0; attempt < 9; attempt++ {
		// Connect with neighbors
		retryCall = s.tryConnectToNode(neigh1) && s.tryConnectToNode(neigh2)
		if retryCall {
			time.Sleep(2 * time.Second)
		} else {
			break
		}
	}
}

// tryConnectToNode attempts to connect to a specific node.
func (s *Server) tryConnectToNode(node consistenthash.Node) bool {
	if node.NodeId != s.NodeId && !containsNodeInPeers(s.ConnectedPeers, node.NodeId, node.Address) {
		err := s.CreateTCPClientAndSendRequest(node.Address, uint8(communication.MessageTypeJoin), []byte(s.Address))
		if err != nil {
			s.logger.Printf("Failed to connect to node %d: %v\n", node.NodeId, err)
			return true
		} else {
			s.logger.Printf("Connected to node %d\n", node.NodeId)
			return false
		}
	}
	return false
}

// connectToAllNodes connects the server to all peers in the network.
func (s *Server) connectToAllNodes() {
	for attempt := 0; attempt < 9; attempt++ {
		for nodeId, nodeAddress := range s.Peers {
			if nodeId != s.NodeId {
				if !containsNodeInPeers(s.ConnectedPeers, nodeId, nodeAddress) {
					s.logger.Printf("Attempting to connect to node %d at %s\n", nodeId, nodeAddress)
					s.CreateTCPClientAndSendRequest(nodeAddress, uint8(communication.MessageTypeJoin), []byte(s.Address))
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
}

// handleNodeFailure manages the recovery process when a node fails.
func (s *Server) handleNodeFailure(nodeId uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// go s.DisasterManager.handleNodeFailure(nodeId)
	delete(s.ConnectedPeers, nodeId)
	s.Ring.RemoveNode(consistenthash.Node{NodeId: nodeId})

	err := s.WAL.WriteLogEntry(s.NodeId, "node_failure", fmt.Sprintf("Node %d failed", nodeId))
	if err != nil {
		s.logger.Printf("Error writing WAL entry: %v\n", err)
	}
}

// containsNodeInPeers returns if node contains in the given map.
func containsNodeInPeers(peers map[uint64]string, node uint64, address string) bool {
	for nodeId, nodeAddress := range peers {
		if nodeId == node && nodeAddress == address {
			return true
		}
	}
	return false
}

// extractFileIdFromChunkId extracts file from chunkId.
func extractFileIdFromChunkId(chunkId string) string {
	parts := strings.Split(chunkId, "::")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}
