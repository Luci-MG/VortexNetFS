package consistenthash

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// TODO:: Add virtual nodes
// TODO:: Better Hashing function

// Node in the ring
type Node struct {
	NodeId  uint64
	Address string
}

// The Hashring
type HashRing struct {
	Nodes             []Node
	ReplicationFactor int
	mu                sync.RWMutex
}

// NewHashRing creates a hash ring.
func NewHashRing(replicationFactor int) *HashRing {
	return &HashRing{
		ReplicationFactor: replicationFactor,
		Nodes:             []Node{},
	}
}

// AddNewNode adds a node in the ring.
func (r *HashRing) AddNewNode(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	//inset node in sorted order
	i := sort.Search(len(r.Nodes), func(i int) bool {
		return r.hashNode(r.Nodes[i].NodeId) < r.hashNode(node.NodeId)
	})
	r.Nodes = append(r.Nodes[:i], append([]Node{node}, r.Nodes[i:]...)...)
}

// AddMultipleNodes adds multiple nodes in the ring.
func (r *HashRing) AddMultipleNodes(nodes []Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, node := range nodes {
		r.AddNewNode(node)
	}
}

// RemoveNode removes node from hash ring.
func (r *HashRing) RemoveNode(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	i := sort.Search(len(r.Nodes), func(i int) bool {
		return r.Nodes[i].NodeId == node.NodeId
	})

	if i < len(r.Nodes) && r.Nodes[i].NodeId == node.NodeId {
		r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)
	}
}

// GetNodeForChunkKey gets node for a chunkkey.
func (r *HashRing) GetNodeForChunkKey(chunkKey string) (int, uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.Nodes) == 0 {
		return 0, 0
	}

	hash := r.hashKey(chunkKey)

	i := sort.Search(len(r.Nodes), func(i int) bool {
		return r.hashNode(r.Nodes[i].NodeId) >= hash
	})

	if i == len(r.Nodes) {
		return 0, r.Nodes[0].NodeId
	}

	return i, r.Nodes[i].NodeId
}

// GetReplicaNodes gets replicas for a nodeId.
func (r *HashRing) GetReplicaNodes(chunkKey string, count int) []uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.Nodes) == 0 {
		return []uint64{}
	}

	i, primaryNode := r.GetNodeForChunkKey(chunkKey)
	replicas := []uint64{}

	for j := 1; len(replicas) < count && j < len(r.Nodes); j++ {
		nextNode := r.Nodes[(i+j)%len(r.Nodes)]
		if nextNode.NodeId != primaryNode && !containsNode(replicas, nextNode.NodeId) {
			replicas = append(replicas, nextNode.NodeId)
		}
	}

	return replicas
}

// GetNeighbours get neighbours of the particular nodeId
func (r *HashRing) GetNeighbours(nodeId uint64) (Node, Node) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.Nodes) == 0 {
		return Node{}, Node{}
	}

	i := sort.Search(len(r.Nodes), func(i int) bool {
		return r.Nodes[i].NodeId >= nodeId
	})

	prevNode := r.Nodes[(i-1+len(r.Nodes))%len(r.Nodes)]
	nextNode := r.Nodes[(i+1)%len(r.Nodes)]

	return prevNode, nextNode
}

// containsNode helper function for checking if node contains in the list.
func containsNode(nodes []uint64, nodeId uint64) bool {
	for _, n := range nodes {
		if n == nodeId {
			return true
		}
	}
	return false
}

// hashNode generates the hash for a node based on its nodeid.
func (r *HashRing) hashNode(nodeId uint64) uint32 {
	return crc32.ChecksumIEEE([]byte(fmt.Sprintf("%d", nodeId)))
}

// hashKey generates the hash for a given chunkId.
func (r *HashRing) hashKey(chunkKey string) uint32 {
	return crc32.ChecksumIEEE([]byte(chunkKey))
}
