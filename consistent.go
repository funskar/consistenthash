package consistenthash

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type uints []uint32

// Len returns the length of the uints array.
func (u uints) Len() int { return len(u) }

// Swap exchanges elements i and j
func (u uints) Swap(i, j int) { u[i], u[j] = u[j], u[i] }

// Less returns true if element at i is less than j
func (u uints) Less(i, j int) bool { return u[i] < u[j] }

type Hash func(data []byte) uint32

type HashRing struct {
	hash             Hash
	sortedNodeHashes uints
	nodeHashMap      map[uint32]string
	members          map[string]bool
	sync.RWMutex
}

func (h *HashRing) IsEmpty() bool {
	return len(h.sortedNodeHashes) == 0
}

func NewRing(fn Hash) *HashRing {
	h := &HashRing{
		hash:        fn,
		nodeHashMap: make(map[uint32]string),
		members:     make(map[string]bool),
	}
	if h.hash == nil {
		h.hash = crc32.ChecksumIEEE
	}
	return h
}

func (h *HashRing) AddNode(node string, weight int) error {
	h.RWMutex.Lock()
	defer h.RWMutex.Unlock()
	if _, exists := h.members[node]; exists {
		return fmt.Errorf("node with name %s already exists", node)
	}
	for i := 0; i < weight; i++ {
		hashKey := h.hashKey(i, node)
		h.sortedNodeHashes = append(h.sortedNodeHashes, hashKey)
		h.nodeHashMap[hashKey] = node
	}
	sort.Sort(h.sortedNodeHashes)
	return nil
}

func (h *HashRing) hashKey(i int, node string) uint32 {
	strKey := []byte(strconv.Itoa(i) + node)
	return h.hash(strKey)
}

func (h *HashRing) Get(key string) (string, error) {
	h.RLock()
	defer h.RUnlock()
	reqHash := h.hash([]byte(key))
	nodeHash := h.search(reqHash)
	return h.nodeHashMap[nodeHash], nil
}

func (h *HashRing) DeleteNode(node string) error {
	if _, exists := h.members[node]; !exists {
		return fmt.Errorf("node with name %s not found", node)
	}
	//TODO: code for node deletion
	return nil
}

func (h *HashRing) search(reqHash uint32) uint32 {
	//Binary search for nearest node after the hash of current request
	fn := func(mid int) bool {
		return h.sortedNodeHashes[mid] >= reqHash
	}
	hashKey := sort.Search(len(h.sortedNodeHashes), fn)

	// Means we have cycled back to the first replica
	if hashKey == len(h.sortedNodeHashes) {
		hashKey = 0
	}
	return h.sortedNodeHashes[hashKey]
}
