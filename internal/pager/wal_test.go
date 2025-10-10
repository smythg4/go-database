package pager

import (
	"bytes"
	"godb/internal/encoding"
	"testing"
)

func TestSerializeDeserializeInsert(t *testing.T) {
	// Create a WAL record
	original := &WALRecord{
		lsn:          LSN(12345),
		action:       INSERT,
		key:          WalKey(100),
		recordLength: 10,
		recordBytes:  []byte("test data!"),
	}

	// Serialize it
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("SerializeInsert failed: %v", err)
	}

	// Deserialize it
	reader := bytes.NewReader(data)
	lsn, _ := encoding.ReadInt64(reader)
	actionByte := make([]byte, 1)
	reader.Read(actionByte)
	action := WalAction(actionByte[0])

	result, err := DeserializeInsert(reader, lsn, action)
	if err != nil {
		t.Fatalf("DeserializeInsert failed: %v", err)
	}

	// Verify round-trip
	if result.lsn != original.lsn {
		t.Errorf("LSN mismatch: got %d, want %d", result.lsn, original.lsn)
	}
	if result.action != original.action {
		t.Errorf("Action mismatch: got %d, want %d", result.action, original.action)
	}
	if result.key != original.key {
		t.Errorf("Key mismatch: got %d, want %d", result.key, original.key)
	}
	if !bytes.Equal(result.recordBytes, original.recordBytes) {
		t.Errorf("RecordBytes mismatch: got %v, want %v", result.recordBytes, original.recordBytes)
	}
}

func TestSerializeDeserializeUpdate(t *testing.T) {
	// Create a WAL record
	original := &WALRecord{
		lsn:          LSN(12345),
		action:       UPDATE,
		key:          WalKey(100),
		recordLength: 10,
		recordBytes:  []byte("test data!"),
	}

	// Serialize it
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("SerializeUpdate failed: %v", err)
	}

	// Deserialize it
	reader := bytes.NewReader(data)
	lsn, _ := encoding.ReadInt64(reader)
	actionByte := make([]byte, 1)
	reader.Read(actionByte)
	action := WalAction(actionByte[0])

	result, err := DeserializeUpdate(reader, lsn, action)
	if err != nil {
		t.Fatalf("DeserializeUpdate failed: %v", err)
	}

	// Verify round-trip
	if result.lsn != original.lsn {
		t.Errorf("LSN mismatch: got %d, want %d", result.lsn, original.lsn)
	}
	if result.action != original.action {
		t.Errorf("Action mismatch: got %d, want %d", result.action, original.action)
	}
	if result.key != original.key {
		t.Errorf("Key mismatch: got %d, want %d", result.key, original.key)
	}
	if !bytes.Equal(result.recordBytes, original.recordBytes) {
		t.Errorf("RecordBytes mismatch: got %v, want %v", result.recordBytes, original.recordBytes)
	}
}

func TestSerializeDeserializeDelete(t *testing.T) {
	// Create a WAL record
	original := &WALRecord{
		lsn:    LSN(12345),
		action: DELETE,
		key:    WalKey(100),
	}

	// Serialize it
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("SerializeDelete failed: %v", err)
	}

	// Deserialize it
	reader := bytes.NewReader(data)
	lsn, _ := encoding.ReadInt64(reader)
	actionByte := make([]byte, 1)
	reader.Read(actionByte)
	action := WalAction(actionByte[0])

	result, err := DeserializeDelete(reader, lsn, action)
	if err != nil {
		t.Fatalf("DeserializeDelete failed: %v", err)
	}

	// Verify round-trip
	if result.lsn != original.lsn {
		t.Errorf("LSN mismatch: got %d, want %d", result.lsn, original.lsn)
	}
	if result.action != original.action {
		t.Errorf("Action mismatch: got %d, want %d", result.action, original.action)
	}
	if result.key != original.key {
		t.Errorf("Key mismatch: got %d, want %d", result.key, original.key)
	}
}

func TestSerializeDeserializeCheckpoint(t *testing.T) {
	// Create a WAL record
	original := &WALRecord{
		lsn:        LSN(12345),
		action:     CHECKPOINT,
		rootPageID: 1,
		nextPageID: 2,
	}

	// Serialize it
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("SerializeCheckpoint failed: %v", err)
	}

	// Deserialize it
	reader := bytes.NewReader(data)
	lsn, _ := encoding.ReadInt64(reader)
	actionByte := make([]byte, 1)
	reader.Read(actionByte)
	action := WalAction(actionByte[0])

	result, err := DeserializeCheckpoint(reader, lsn, action)
	if err != nil {
		t.Fatalf("DeserializeCheckpoint failed: %v", err)
	}

	// Verify round-trip
	if result.lsn != original.lsn {
		t.Errorf("LSN mismatch: got %d, want %d", result.lsn, original.lsn)
	}
	if result.action != original.action {
		t.Errorf("Action mismatch: got %d, want %d", result.action, original.action)
	}
	if result.rootPageID != original.rootPageID {
		t.Errorf("rootPageID mismatch: got %d, want %d", result.rootPageID, original.rootPageID)
	}
	if result.nextPageID != original.nextPageID {
		t.Errorf("nextPageID mismatch: got %d, want %d", result.nextPageID, original.nextPageID)
	}
}

func TestSerializeDeserializeVacuum(t *testing.T) {
	// Create a WAL record
	original := &WALRecord{
		lsn:        LSN(12345),
		action:     CHECKPOINT,
		rootPageID: 1,
		nextPageID: 2,
	}

	// Serialize it
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("SerializeVacuum failed: %v", err)
	}

	// Deserialize it
	reader := bytes.NewReader(data)
	lsn, _ := encoding.ReadInt64(reader)
	actionByte := make([]byte, 1)
	reader.Read(actionByte)
	action := WalAction(actionByte[0])

	result, err := DeserializeVacuum(reader, lsn, action)
	if err != nil {
		t.Fatalf("DeserializeVacuum failed: %v", err)
	}

	// Verify round-trip
	if result.lsn != original.lsn {
		t.Errorf("LSN mismatch: got %d, want %d", result.lsn, original.lsn)
	}
	if result.action != original.action {
		t.Errorf("Action mismatch: got %d, want %d", result.action, original.action)
	}
	if result.rootPageID != original.rootPageID {
		t.Errorf("rootPageID mismatch: got %d, want %d", result.rootPageID, original.rootPageID)
	}
	if result.nextPageID != original.nextPageID {
		t.Errorf("nextPageID mismatch: got %d, want %d", result.nextPageID, original.nextPageID)
	}
}
