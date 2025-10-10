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
	UPDATE // not implemented yet, UPDATEs are just DELETE then INSERT
	VACUUM
	CHECKPOINT
	CREATE_TABLE // not implemented yet
)

type WALRecord struct {
	Lsn          LSN
	Action       WalAction
	Key          WalKey
	RecordLength uint32
	RecordBytes  []byte
	RootPageID   uint32
	NextPageID   uint32
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

func (wm *WALManager) ReadAll() ([]WALRecord, error) {
	_, err := wm.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	records := []WALRecord{}

	for {
		record, err := wm.Deserialize()
		if err != nil && err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		records = append(records, *record)
	}
	return records, nil
}

func (wr *WALRecord) Serialize() ([]byte, error) {
	switch wr.Action {
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
		return nil, fmt.Errorf("record type unsupported: %d", wr.Action)
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
	size := 8 + 1 + 8 + 4 + len(wr.RecordBytes)
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(wr.Lsn))
	buf[8] = byte(wr.Action)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(wr.Key))
	binary.LittleEndian.PutUint32(buf[17:21], wr.RecordLength)
	copy(buf[21:], wr.RecordBytes)

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
		Lsn:          LSN(lsn),
		Action:       action,
		Key:          WalKey(uint64(key)),
		RecordLength: uint32(len(record)),
		RecordBytes:  record,
	}, nil
}

func SerializeDelete(wr *WALRecord) ([]byte, error) {
	size := 8 + 1 + 8
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(wr.Lsn))
	buf[8] = byte(wr.Action)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(wr.Key))

	return buf, nil
}

func DeserializeDelete(r io.Reader, lsn int64, action WalAction) (*WALRecord, error) {
	key, err := encoding.ReadInt64(r)
	if err != nil {
		return nil, err
	}

	return &WALRecord{
		Lsn:    LSN(lsn),
		Action: action,
		Key:    WalKey(uint64(key)),
	}, nil
}

func SerializeUpdate(wr *WALRecord) ([]byte, error) {
	size := 8 + 1 + 8 + 4 + len(wr.RecordBytes)
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(wr.Lsn))
	buf[8] = byte(wr.Action)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(wr.Key))
	binary.LittleEndian.PutUint32(buf[17:21], wr.RecordLength)
	copy(buf[21:], wr.RecordBytes)

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
		Lsn:          LSN(lsn),
		Action:       action,
		Key:          WalKey(uint64(key)),
		RecordLength: uint32(len(record)),
		RecordBytes:  record,
	}, nil
}

func SerializeCheckpoint(wr *WALRecord) ([]byte, error) {
	size := 8 + 1 + 4 + 4
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(wr.Lsn))
	buf[8] = byte(wr.Action)
	binary.LittleEndian.PutUint32(buf[9:13], wr.RootPageID)
	binary.LittleEndian.PutUint32(buf[13:17], wr.NextPageID)

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
		Lsn:        LSN(lsn),
		Action:     action,
		RootPageID: rootPageID,
		NextPageID: nextPageID,
	}, nil
}

func SerializeVacuum(wr *WALRecord) ([]byte, error) {
	size := 8 + 1 + 4 + 4
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(wr.Lsn))
	buf[8] = byte(wr.Action)
	binary.LittleEndian.PutUint32(buf[9:13], wr.RootPageID)
	binary.LittleEndian.PutUint32(buf[13:17], wr.NextPageID)

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
		Lsn:        LSN(lsn),
		Action:     action,
		RootPageID: rootPageID,
		NextPageID: nextPageID,
	}, nil
}

func (w *WALManager) LogInsert(key uint64, record []byte) error {

	wr := WALRecord{
		//lsn:          LSN(fileOffset), <-- handle this only on flushes
		Action:       INSERT,
		Key:          WalKey(key),
		RecordLength: uint32(len(record)),
		RecordBytes:  record,
	}
	w.buffer = append(w.buffer, wr)
	return nil
}

func (w *WALManager) LogDelete(key uint64) error {

	wr := WALRecord{
		//lsn:          LSN(fileOffset), <-- handle this only on flushes
		Action: DELETE,
		Key:    WalKey(key),
	}
	w.buffer = append(w.buffer, wr)
	return nil
}

func (w *WALManager) LogUpdate(key uint64, record []byte) error {

	wr := WALRecord{
		//lsn:          LSN(fileOffset), <-- handle this only on flushes
		Action:       UPDATE,
		Key:          WalKey(key),
		RecordLength: uint32(len(record)),
		RecordBytes:  record,
	}
	w.buffer = append(w.buffer, wr)
	return nil
}

func (w *WALManager) LogCheckpoint(rootPageID, nextPageID uint32) error {

	wr := WALRecord{
		//lsn:          LSN(fileOffset), <-- handle this only on flushes
		Action:     CHECKPOINT,
		RootPageID: rootPageID,
		NextPageID: nextPageID,
	}
	w.buffer = append(w.buffer, wr)
	return nil
}

func (w *WALManager) LogVacuum(rootPageID, nextPageID uint32) error {

	wr := WALRecord{
		//lsn:          LSN(fileOffset), <-- handle this only on flushes
		Action:     VACUUM,
		RootPageID: rootPageID,
		NextPageID: nextPageID,
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
		w.buffer[i].Lsn = LSN(fileOffset)
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

func (w *WALManager) Truncate() error {
	return w.file.Truncate(0)
}
