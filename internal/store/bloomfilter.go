package store

import (
	"godb/internal/encoding"
	"math"
)

type BloomFilter struct {
	numBits   uint64
	bitField  []byte
	numHashes int
}

func NewBloomFilter(size uint64, numHashes int) *BloomFilter {
	numBytes := (size + 7) / 8
	return &BloomFilter{
		numBits:   uint64(size),
		bitField:  make([]byte, numBytes),
		numHashes: numHashes,
	}
}

func (bf *BloomFilter) Add(key uint64) {
	for i := 0; i < bf.numHashes; i++ {
		h := encoding.MurmurHash64(key, uint64(i))
		bitPos := h % bf.numBits

		// set bit
		byteIndex := bitPos / 8
		bitIndex := bitPos % 8

		bf.bitField[byteIndex] |= (1 << bitIndex)
	}
}

func (bf *BloomFilter) MayContain(key uint64) bool {
	for i := 0; i < bf.numHashes; i++ {
		hash := encoding.MurmurHash64(key, uint64(i))
		bitPos := hash % bf.numBits

		// Check bit using bitwise operations
		byteIndex := bitPos / 8
		bitIndex := bitPos % 8

		if bf.bitField[byteIndex]&(1<<bitIndex) == 0 {
			return false // Definitely not present
		}
	}
	return true // Maybe present
}

// OptimalBloomSize calculates the optimal number of bits and hash functions
// for a bloom filter given the expected number of keys and desired false positive rate
func OptimalBloomSize(numKeys uint, falsePositiveRate float64) (numBits uint64, numHashes int) {
	if numKeys == 0 {
		return 1024, 3 // Minimum sensible values
	}

	// m = -n * ln(p) / (ln(2)^2)
	// where m = bits, n = keys, p = false positive rate
	ln2 := math.Ln2
	numBits = uint64(-float64(numKeys) * math.Log(falsePositiveRate) / (ln2 * ln2))

	// k = m/n * ln(2)
	// where k = number of hash functions
	numHashes = int(float64(numBits) / float64(numKeys) * ln2)

	// Clamp to reasonable range
	if numHashes < 1 {
		numHashes = 1
	}
	if numHashes > 10 {
		numHashes = 10 // Diminishing returns beyond ~7-10
	}

	return numBits, numHashes
}
