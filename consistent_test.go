package consistenthash

import (
	"hash/crc32"
	"strconv"
	"testing"
)

func checkCount(num, expected int, t *testing.T) {
	if num != expected {
		t.Errorf("got %d, expected %d", num, expected)
	}
}

// Simple hash function to return easier to understand values
// Assumes the keys can be converted to integer
var hashFunction = func(data []byte) uint32 {
	hash, err := strconv.Atoi(string(data))
	if err != nil {
		panic(err)
	}
	return uint32(hash)
}

func TestNewRing(t *testing.T) {
	h := NewRing(crc32.ChecksumIEEE)
	if h == nil {
		t.Errorf("expected obj")
	}
}

// Given the above simple hashFunction,
// it will create virtual nodes - {1 4 7 11 14 17 21 24 27}
var nodes = []struct {
	Key    string
	Weight int
}{
	{"1", 3},
	{"4", 3},
	{"7", 3},
}

var requests = []struct {
	Key     string
	expNode string
}{
	{"3", "4"},
	{"0", "1"},
	{"15", "7"},
	{"6", "7"},
}

func TestHashRing_AddNode(t *testing.T) {
	h := NewRing(hashFunction)
	count := 0
	for _, node := range nodes {
		h.AddNode(node.Key, node.Weight)
		count += node.Weight
	}
	checkCount(len(h.sortedNodeHashes), count, t)
}

func TestHashRing_IsEmpty(t *testing.T) {
	h := NewRing(hashFunction)
	if !h.IsEmpty() {
		t.Error("Hash ring found to be non-empty")
	}
	h.AddNode("1", 5)
	if h.IsEmpty() {
		t.Error("Hash ring found to be empty")
	}
}

func TestHashRing_Get(t *testing.T) {
	h := NewRing(hashFunction)
	for _, node := range nodes {
		h.AddNode(node.Key, node.Weight)
	}
	for _, req := range requests {
		node, err := h.Get(req.Key)
		if err != nil {
			t.Errorf("Error while trying to fetch Node for Request - %s", req.Key)
		}
		if node != req.expNode {
			t.Errorf("Request for %s, yielded node %s, but expected node %s", req.Key, node, req.expNode)
		}
	}
}
