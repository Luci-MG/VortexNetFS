package file

import (
	"fmt"
	"sync"
	"time"
)

// FileIdGenerator
// Our file Id composes of 64-bit which we will do using bitwise operations
// the file id is structured as:
// 41 bits > time since custom epoch (in milliseconds) in our case 5th Nov 2021
// 10 bits > NodeId (max 1023 nodes)  0000000001 to 1111111111
// 12 bits > sequence number (to handle multiple requests within the same millisecond)
// 64 bits > 41 + 10 + 12
// so we will get 2^12 = 4096 unique ids per millisecond per node
// and if we conisder max nodes of 2^10 = 1024
// then per maxnodes network 2^22 = 4194304 we can generate unique ids per millisecond
// so highly scalable
// and it can generate this upto 2^41-1 = so approx 69.7 years from 5th nov 2021

// FileIdGenerator structure
type FileIdGenerator struct {
	lastTimestamp int64
	nodeId        uint64
	sequence      uint64
	mutex         sync.Mutex
}

const (
	timeBits     uint  = 41
	nodeBits     uint  = 10
	sequenceBits uint  = 12
	maxNodeId          = -1 ^ (-1 << nodeBits)
	maxSequence        = -1 ^ (-1 << sequenceBits)
	customEpoch  int64 = 1636070400000
)

// NewFileIdGenerator creates file id generator instance.
func NewFileIdGenerator(nodeId uint64) *FileIdGenerator {
	if nodeId > maxNodeId {
		panic("Node ID exceeds the maximum allowed value")
	}
	return &FileIdGenerator{
		nodeId: nodeId,
	}
}

// GenerateFileId generates a proper global id unique across cluster.
func (f *FileIdGenerator) GenerateFileId() string {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	timestamp := time.Now().UnixMilli() - customEpoch
	if timestamp == f.lastTimestamp {
		f.sequence = (f.sequence + 1) & maxSequence
		if f.sequence == 0 {
			for timestamp <= f.lastTimestamp {
				timestamp = time.Now().UnixMilli() - customEpoch
			}
		}
	} else {
		f.sequence = 0
	}

	f.lastTimestamp = timestamp
	fileId := (uint64(timestamp) << (nodeBits + sequenceBits)) | (f.nodeId << sequenceBits) | f.sequence
	return fmt.Sprintf("%d", fileId)
}
