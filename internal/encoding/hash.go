package encoding

import "encoding/binary"

func MurmurHash3(data []byte, seed uint32) uint32 {
	c1 := uint32(0xcc9e2d51)
	c2 := uint32(0x1b873593)

	length := len(data)
	h1 := uint32(seed)
	roundedEnd := length & 0xfffffffc // round down to 4 byte block

	// process 4-byte blocks
	for i := 0; i < roundedEnd; i += 4 {
		k1 := uint32(data[i]&0xff) |
			(uint32(data[i+1]&0xff) << 8) |
			(uint32(data[i+2]&0xff) << 16) |
			(uint32(data[i+3]) << 24)
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17) // ROTL32(k1, 15)
		k1 *= c2

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19) // ROTL32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}

	// tail (remaining 1-3 bytes)
	k1 := uint32(0)
	val := length & 0x03

	if val == 3 {
		k1 = uint32(data[roundedEnd+2]&0xff) << 16
	}
	if val >= 2 {
		k1 |= uint32(data[roundedEnd+1]&0xff) << 8
	}
	if val >= 1 {
		k1 |= uint32(data[roundedEnd] & 0xff)
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17) // ROTL(k1, 15)
		k1 *= c2
		h1 ^= k1
	}

	// finalization
	h1 ^= uint32(length)

	// fmix(h1)
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}

func MurmurHash64(key uint64, seed uint64) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], key)

	h1 := uint64(MurmurHash3(buf[:], uint32(seed)))
	h2 := uint64(MurmurHash3(buf[:], uint32(seed+1)))

	return (h1 << 32) | h2
}
