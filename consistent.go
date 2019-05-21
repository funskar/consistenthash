package consistenthash

import (
	"hash/crc32"
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
	sync.RWMutex
}

func (hr *HashRing) IsEmpty() bool {
	return len(hr.sortedNodeHashes) == 0
}

func NewRing(fn Hash) *HashRing {
	hr := &HashRing{
		hash:        fn,
		nodeHashMap: make(map[uint32]string),
	}
	if hr.hash == nil {
		hr.hash = crc32.ChecksumIEEE
	}
	return hr
}

func (h *HashRing) AddNode(node string) {

}

func (h *HashRing) Get(key string) string {
	return ""
}
