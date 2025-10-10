package pager

import (
	"encoding/binary"
	"fmt"
	"godb/internal/encoding"
	"io"
	"os"
)

type WALManager struct {
	file   *os.File
	buffer []WALRecord
}

type LSN uint64

type WalAction uint8
type WalKey uint64

const (
	INSERT WalAction = iota
	DELETE
	UPDATE
	VACUUM
	CHECKPOINT
)

type WALRecord struct {
	lsn          LSN
	action       WalAction
	key          WalKey
	recordLength uint32
	recordBytes  []byte
	rootPageID   uint32
	nextPageID   uint32
}

func NewWalManager(filename string) (*WALManager, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WALManager{
		file:   f,
		buffer: []WALRecord{},
	}, nil
}

func (wr *WALRecord) Serialize() ([]byte, error) {
	switch wr.action {
	case INSERT:
		return SerializeInsert(wr)
	case DELETE:
		return SerializeDelete(wr)
	case UPDATE:
		return SerializeUpdate(wr)
	case VACUUM:
		return SerializeVacuum(wr)
	case CHECKPOINT:
		return SerializeCheckpoint(wr)
	default:
		return nil, fmt.Errorf("record type unsupported: %d", wr.action)
	}
}

func (wm *WALManager) Deserialize() (*WALRecord, error) {
	lsn, err := encoding.ReadInt64(wm.file)
	if err != nil {
		return nil, err
	}
	actionBytes := make([]byte, 1)
	_, err = wm.file.Read(actionBytes)
	if err != nil {
		return nil, err
	}
	action := WalAction(actionBytes[0])

	switch action {
	case INSERT:
		return DeserializeInsert(wm.file, lsn, action)
	case DELETE:
		return DeserializeDelete(wm.file, lsn, action)
	case UPDATE:
		return DeserializeUpdate(wm.file, lsn, action)
	case VACUUM:
		return DeserializeVacuum(wm.file, lsn, action)
	case CHECKPOINT:
		return DeserializeCheckpoint(wm.file, lsn, action)
	default:
		return nil, fmt.Errorf("record type unsupported: %d", action)
	}
}

func SerializeInsert(wr *WALRecord) ([]byte, error) {
	size := 8 + 1 + 8 + 4 + len(wr.recordBytes)
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(wr.lsn))
	buf[8] = byte(wr.action)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(wr.key))
	binary.LittleEndian.PutUint32(buf[17:21], wr.recordLength)
	copy(buf[21:], wr.recordBytes)

	return buf, nil
}

func DeserializeInsert(r io.Reader, lsn int64, action WalAction) (*WALRecord, error) {
	key, err := encoding.ReadInt64(r)
	if err != nil {
		return nil, err
	}
	record, err := encoding.ReadByteSlice(r)
	if err != nil {
		return nil, err
	}
	return &WALRecord{
		lsn:          LSN(lsn),
		action:       action,
		key:          WalKey(uint64(key)),
		recordLength: uint32(len(record)),
		recordBytes:  record,
	}, nil
}

func SerializeDelete(wr *WALRecord) ([]byte, error) {
	size := 8 + 1 + 8
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(wr.lsn))
	buf[8] = byte(wr.action)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(wr.key))

	return buf, nil
}

func DeserializeDelete(r io.Reader, lsn int64, action WalAction) (*WALRecord, error) {
	key, err := encoding.ReadInt64(r)
	if err != nil {
		return nil, err
	}

	return &WALRecord{
		lsn:    LSN(lsn),
		action: action,
		key:    WalKey(uint64(key)),
	}, nil
}

func SerializeUpdate(wr *WALRecord) ([]byte, error) {
	size := 8 + 1 + 8 + 4 + len(wr.recordBytes)
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(wr.lsn))
	buf[8] = byte(wr.action)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(wr.key))
	binary.LittleEndian.PutUint32(buf[17:21], wr.recordLength)
	copy(buf[21:], wr.recordBytes)

	return buf, nil
}

func DeserializeUpdate(r io.Reader, lsn int64, action WalAction) (*WALRecord, error) {
	key, err := encoding.ReadInt64(r)
	if err != nil {
		return nil, err
	}
	record, err := encoding.ReadByteSlice(r)
	if err != nil {
		return nil, err
	}
	return &WALRecord{
		lsn:          LSN(lsn),
		action:       action,
		key:          WalKey(uint64(key)),
		recordLength: uint32(len(record)),
		recordBytes:  record,
	}, nil
}

func SerializeCheckpoint(wr *WALRecord) ([]byte, error) {
	size := 8 + 1 + 4 + 4
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(wr.lsn))
	buf[8] = byte(wr.action)
	binary.LittleEndian.PutUint32(buf[9:13], wr.rootPageID)
	binary.LittleEndian.PutUint32(buf[13:17], wr.nextPageID)

	return buf, nil
}

func DeserializeCheckpoint(r io.Reader, lsn int64, action WalAction) (*WALRecord, error) {
	rootPageID, err := encoding.ReadUint32(r)
	if err != nil {
		return nil, err
	}
	nextPageID, err := encoding.ReadUint32(r)
	if err != nil {
		return nil, err
	}
	return &WALRecord{
		lsn:        LSN(lsn),
		action:     action,
		rootPageID: rootPageID,
		nextPageID: nextPageID,
	}, nil
}

func SerializeVacuum(wr *WALRecord) ([]byte, error) {
	size := 8 + 1 + 4 + 4
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(wr.lsn))
	buf[8] = byte(wr.action)
	binary.LittleEndian.PutUint32(buf[9:13], wr.rootPageID)
	binary.LittleEndian.PutUint32(buf[13:17], wr.nextPageID)

	return buf, nil
}

func DeserializeVacuum(r io.Reader, lsn int64, action WalAction) (*WALRecord, error) {
	rootPageID, err := encoding.ReadUint32(r)
	if err != nil {
		return nil, err
	}
	nextPageID, err := encoding.ReadUint32(r)
	if err != nil {
		return nil, err
	}
	return &WALRecord{
		lsn:        LSN(lsn),
		action:     action,
		rootPageID: rootPageID,
		nextPageID: nextPageID,
	}, nil
}

func (w *WALManager) LogInsert(key uint64, record []byte) error {

	wr := WALRecord{
		//lsn:          LSN(fileOffset), <-- handle this only on flushes
		action:       INSERT,
		key:          WalKey(key),
		recordLength: uint32(len(record)),
		recordBytes:  record,
	}
	w.buffer = append(w.buffer, wr)
	return nil
}

func (w *WALManager) LogDelete(key uint64) error {

	wr := WALRecord{
		//lsn:          LSN(fileOffset), <-- handle this only on flushes
		action: DELETE,
		key:    WalKey(key),
	}
	w.buffer = append(w.buffer, wr)
	return nil
}

func (w *WALManager) LogUpdate(key uint64, record []byte) error {

	wr := WALRecord{
		//lsn:          LSN(fileOffset), <-- handle this only on flushes
		action:       UPDATE,
		key:          WalKey(key),
		recordLength: uint32(len(record)),
		recordBytes:  record,
	}
	w.buffer = append(w.buffer, wr)
	return nil
}

func (w *WALManager) LogCheckpoint(rootPageID, nextPageID uint32) error {

	wr := WALRecord{
		//lsn:          LSN(fileOffset), <-- handle this only on flushes
		action:     CHECKPOINT,
		rootPageID: rootPageID,
		nextPageID: nextPageID,
	}
	w.buffer = append(w.buffer, wr)
	return nil
}

func (w *WALManager) LogVacuum(rootPageID, nextPageID uint32) error {

	wr := WALRecord{
		//lsn:          LSN(fileOffset), <-- handle this only on flushes
		action:     VACUUM,
		rootPageID: rootPageID,
		nextPageID: nextPageID,
	}
	w.buffer = append(w.buffer, wr)
	return nil
}

func (w *WALManager) getCurrentOffset() (uint64, error) {
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()), nil
}

func (w *WALManager) FlushWAL() error {
	for i := range w.buffer {
		fileOffset, err := w.getCurrentOffset()
		if err != nil {
			return err
		}
		w.buffer[i].lsn = LSN(fileOffset)
		data, err := w.buffer[i].Serialize()
		if err != nil {
			return err
		}
		_, err = w.file.Write(data)
		if err != nil {
			return err
		}
	}
	w.buffer = []WALRecord{}
	return w.file.Sync()
}
