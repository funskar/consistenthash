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

type entry interface {
	GetWeight() int
	GetKey() string
}

type node struct {
	weight int
	key    string
}

func (e *node) GetWeight() int {
	return e.weight
}

func (e *node) GetKey() string {
	return e.key
}

type StringValue struct {
	value string
}

type HashRing struct {
	hash         Hash
	sortedHashes uints
	nodeHashMap  map[uint32]string
	members      map[string]entry
	sync.RWMutex
}

func (h *HashRing) IsEmpty() bool {
	return len(h.sortedHashes) == 0
}

func NewRing(fn Hash) *HashRing {
	h := &HashRing{
		hash:        fn,
		nodeHashMap: make(map[uint32]string),
		members:     make(map[string]entry),
	}
	if h.hash == nil {
		h.hash = crc32.ChecksumIEEE
	}
	return h
}

func (h *HashRing) AddNode(key string, weight int) error {
	h.Lock()
	defer h.Unlock()
	if _, exists := h.members[key]; exists {
		return fmt.Errorf("key with name %s already exists", key)
	}
	h.members[key] = &node{weight, key}
	for i := 0; i < weight; i++ {
		hashKey := h.hashKey(i, key)
		h.sortedHashes = append(h.sortedHashes, hashKey)
		h.nodeHashMap[hashKey] = key
	}
	sort.Sort(h.sortedHashes)
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
	index := h.search(reqHash)
	return h.nodeHashMap[h.sortedHashes[index]], nil
}

func (h *HashRing) DeleteNode(key string) error {
	h.Lock()
	defer h.Unlock()
	if _, exists := h.members[key]; !exists {
		return fmt.Errorf("key with name %s not found", key)
	}
	for i := 0; i < h.members[key].GetWeight(); i++ {
		hashKey := h.hashKey(i, key)
		delete(h.nodeHashMap, hashKey)
		index := h.search(hashKey)
		h.sortedHashes = removeIndex(h.sortedHashes, index)
	}
	sort.Sort(h.sortedHashes)
	delete(h.members, key)
	return nil
}

func removeIndex(slice uints, i int) uints {
	return append(slice[:i], slice[i+1:]...)
}

func (h *HashRing) search(reqHash uint32) int {
	//Binary search for nearest node after the hash of current request
	fn := func(mid int) bool {
		return h.sortedHashes[mid] >= reqHash
	}
	nodeIndex := sort.Search(len(h.sortedHashes), fn)

	// Means we have cycled back to the first replica
	if nodeIndex == len(h.sortedHashes) {
		nodeIndex = 0
	}
	return nodeIndex
}
