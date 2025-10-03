package store

import (
	"encoding/binary"
	"io"
	"os"
)

// --------------
type KVStore struct {
	file *os.File
}

func NewKVStore(filename string) (*KVStore, error) {
	file, err := os.OpenFile("test.db", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &KVStore{file: file}, nil
}

func (kv *KVStore) Put(key, value int32) error {
	_, err := kv.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(key))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(value))

	_, err = kv.file.Write(buf)
	if err != nil {
		return err
	}

	return kv.file.Sync()
}

func (kv *KVStore) Get(key int32) (int32, bool) {
	stat, err := kv.file.Stat()
	if err != nil {
		return 0, false
	}
	fileSize := stat.Size()

	buf := make([]byte, 8)

	for offset := fileSize - 8; offset >= 0; offset -= 8 {
		_, err = kv.file.Seek(offset, io.SeekStart)
		if err != nil {
			return 0, false
		}
		_, err := io.ReadFull(kv.file, buf)
		if err != nil {
			return 0, false
		}

		storedKey := int32(binary.LittleEndian.Uint32(buf[0:4]))
		storedValue := int32(binary.LittleEndian.Uint32(buf[4:8]))

		if storedKey == key {
			return storedValue, true
		}
	}
	return 0, false
}

func (kv *KVStore) Close() error {
	return kv.file.Close()
}

// --------------
