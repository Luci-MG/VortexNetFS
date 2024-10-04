package dht

import (
	"encoding/json"
	"sync"
)

// TODO:: Check we can decrease DHT's

// DHT is a distrubuted hash table that stores storage data.
type DHT struct {
	mu              sync.RWMutex
	FileMetaDataDHT map[string]FileMetaData // maps files to metadata
	ChunkToNodeDHT  map[string][]uint64     // maps chunkids to nodes
	NodeToChunkDHT  map[uint64][]string     // maps nodes to chunkids
}

// FileMetaData the meta data of the file we store.
type FileMetaData struct {
	FileId       string
	FileName     string
	FileType     string
	FileSize     int64
	LastChunkId  string
	PrimaryNodes []uint64
	ReplicaNodes []uint64
	Chunks       []string
}

// NewDHT creates dht instances.
func NewDHT() *DHT {
	return &DHT{
		FileMetaDataDHT: make(map[string]FileMetaData),
		ChunkToNodeDHT:  make(map[string][]uint64),
		NodeToChunkDHT:  make(map[uint64][]string),
	}
}

// NewFileMetaData creates the new file meta data.
func (d *DHT) NewFileMetaData(fileName string, fileType string, fileSize int64) *FileMetaData {
	return &FileMetaData{
		FileName: fileName,
		FileType: fileType,
		FileSize: fileSize,
	}
}

// FileMetaDataToBytes converts filemetadata to bytes.
func (d *DHT) FileMetaDataToBytes(fm FileMetaData) []byte {
	bytes, _ := json.Marshal(fm)
	return bytes
}

// FileMetaDataFromBytes converts bytes to filemetadata.
func (d *DHT) FileMetaDataFromBytes(data []byte) FileMetaData {
	var metaData FileMetaData
	_ = json.Unmarshal(data, &metaData)
	return metaData
}

// AddFileMetaData adds file metadata to the FileMetaDataDHT.
func (d *DHT) AddFileMetaData(fileId string, metaData FileMetaData) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.FileMetaDataDHT[fileId] = metaData
}

// AddChunkToNode adds chunk to the ChunkToNodeDHT.
func (d *DHT) AddChunkToNode(chunkId string, nodes []uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ChunkToNodeDHT[chunkId] = nodes
}

// AddNodeToChunk adds node to the NodeToChunkDHT.
func (d *DHT) AddNodeToChunk(nodeId uint64, chunks []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.NodeToChunkDHT[nodeId] = chunks
}

// AppendChunksInFileMetaData appends chunks in the FileMetaDataDHT.
func (d *DHT) AppendChunksInFileMetaData(fileId string, chunks []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if fileMeta, exists := d.FileMetaDataDHT[fileId]; exists {
		for _, chunk := range chunks {
			if !containsChunk(fileMeta.Chunks, chunk) {
				fileMeta.Chunks = append(fileMeta.Chunks, chunk)
			}
		}
		d.FileMetaDataDHT[fileId] = fileMeta
	}
}

// UpdateFileMetaData updates metadata in the FileMetaDataDHT.
func (d *DHT) UpdateFileMetaData(fileId string, metaData FileMetaData) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.FileMetaDataDHT[fileId]; exists {
		d.FileMetaDataDHT[fileId] = metaData
	}
}

// UpdateChunkToNode updates the ChunkToNodeDHT.
func (d *DHT) UpdateChunkToNode(chunkId string, nodes []uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// d.ChunkToNodeDHT[chunkId] = append(d.ChunkToNodeDHT[chunkId], nodes...)
	for _, node := range nodes {
		if !containsNode(d.ChunkToNodeDHT[chunkId], node) {
			d.ChunkToNodeDHT[chunkId] = append(d.ChunkToNodeDHT[chunkId], node)
		}
	}
}

// UpdateNodeToChunk updates the NodeToChunkDHT.
func (d *DHT) UpdateNodeToChunk(nodeId uint64, chunks []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// d.NodeToChunkDHT[nodeId] = append(d.NodeToChunkDHT[nodeId], chunks...)
	for _, chunk := range chunks {
		if !containsChunk(d.NodeToChunkDHT[nodeId], chunk) {
			d.NodeToChunkDHT[nodeId] = append(d.NodeToChunkDHT[nodeId], chunk)
		}
	}
}

// GetFileMetaData gets the file meta data for fileId.
func (d *DHT) GetFileMetaData(fileId string) (FileMetaData, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	metadata, exists := d.FileMetaDataDHT[fileId]
	return metadata, exists
}

// GetChunkFromNodeDHT gets the chunks for a node.
func (d *DHT) GetChunkFromNodeDHT(nodeId uint64) ([]string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	chunks, exists := d.NodeToChunkDHT[nodeId]
	return chunks, exists
}

// GetNodeFromChunkDHT gets the nodes for the chunk.
func (d *DHT) GetNodeFromChunkDHT(chunkId string) ([]uint64, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	nodes, exists := d.ChunkToNodeDHT[chunkId]
	return nodes, exists
}

// containsNode helper func to check if node exists in the list.
func containsNode(nodeList []uint64, node uint64) bool {
	for _, n := range nodeList {
		if n == node {
			return true
		}
	}
	return false
}

// containsChunk helper func to check if chunk exists in the list.
func containsChunk(chunkList []string, chunk string) bool {
	for _, c := range chunkList {
		if c == chunk {
			return true
		}
	}
	return false
}
