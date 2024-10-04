package heartbeat

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// TODO:: Improve efficency of HBM

// HeartBeatManager structure.
type HeartBeatManager struct {
	Heartbeats         map[uint64]int64
	HeartbeatInterval  time.Duration
	HeartbeatThreshold time.Duration
	mu                 sync.Mutex
	logger             *log.Logger
}

// NewHeartBeatManager
func NewHeartBeatManager(heartbeatInterval time.Duration,
	heartBeatThreshold time.Duration, logger *log.Logger) *HeartBeatManager {
	HBM := &HeartBeatManager{
		Heartbeats:         make(map[uint64]int64),
		HeartbeatInterval:  heartbeatInterval,
		HeartbeatThreshold: heartBeatThreshold,
		logger:             logger,
	}
	HBM.logger.Println("Heart Beat Manager initialized...")
	return HBM
}

// UpdateLastSeen updates last seen of the node
func (h *HeartBeatManager) UpdateLastSeen(nodeId uint64, ourNodeId uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Heartbeats[nodeId] = time.Now().Unix()
	fmt.Printf("Node %d: Updated last seen time for node %d\n", ourNodeId, nodeId)
}

// CheckFailedNode checks node for health checks to see if failed
func (h *HeartBeatManager) CheckFailedNode(nodeId uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	lastSeen, exists := h.Heartbeats[nodeId]
	if !exists {
		return false
	}

	if time.Now().Unix()-lastSeen > int64(h.HeartbeatThreshold) {
		fmt.Printf("Node %d is considered failed last seen at %d\n", nodeId, lastSeen)
		return true
	}
	return false
}
