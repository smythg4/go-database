package pager

import (
	"bytes"
	"godb/internal/encoding"
	"testing"
)

func TestSerializeDeserializeInsert(t *testing.T) {
	// Create a WAL record
	original := &WALRecord{
		Lsn:          LSN(12345),
		Action:       INSERT,
		Key:          WalKey(100),
		RecordLength: 10,
		RecordBytes:  []byte("test data!"),
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
	if result.Lsn != original.Lsn {
		t.Errorf("LSN mismatch: got %d, want %d", result.Lsn, original.Lsn)
	}
	if result.Action != original.Action {
		t.Errorf("Action mismatch: got %d, want %d", result.Action, original.Action)
	}
	if result.Key != original.Key {
		t.Errorf("Key mismatch: got %d, want %d", result.Key, original.Key)
	}
	if !bytes.Equal(result.RecordBytes, original.RecordBytes) {
		t.Errorf("RecordBytes mismatch: got %v, want %v", result.RecordBytes, original.RecordBytes)
	}
}

func TestSerializeDeserializeUpdate(t *testing.T) {
	// Create a WAL record
	original := &WALRecord{
		Lsn:          LSN(12345),
		Action:       UPDATE,
		Key:          WalKey(100),
		RecordLength: 10,
		RecordBytes:  []byte("test data!"),
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
	if result.Lsn != original.Lsn {
		t.Errorf("LSN mismatch: got %d, want %d", result.Lsn, original.Lsn)
	}
	if result.Action != original.Action {
		t.Errorf("Action mismatch: got %d, want %d", result.Action, original.Action)
	}
	if result.Key != original.Key {
		t.Errorf("Key mismatch: got %d, want %d", result.Key, original.Key)
	}
	if !bytes.Equal(result.RecordBytes, original.RecordBytes) {
		t.Errorf("RecordBytes mismatch: got %v, want %v", result.RecordBytes, original.RecordBytes)
	}
}

func TestSerializeDeserializeDelete(t *testing.T) {
	// Create a WAL record
	original := &WALRecord{
		Lsn:    LSN(12345),
		Action: DELETE,
		Key:    WalKey(100),
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
	if result.Lsn != original.Lsn {
		t.Errorf("LSN mismatch: got %d, want %d", result.Lsn, original.Lsn)
	}
	if result.Action != original.Action {
		t.Errorf("Action mismatch: got %d, want %d", result.Action, original.Action)
	}
	if result.Key != original.Key {
		t.Errorf("Key mismatch: got %d, want %d", result.Key, original.Key)
	}
}

func TestSerializeDeserializeCheckpoint(t *testing.T) {
	// Create a WAL record
	original := &WALRecord{
		Lsn:        LSN(12345),
		Action:     CHECKPOINT,
		RootPageID: 1,
		NextPageID: 2,
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
	if result.Lsn != original.Lsn {
		t.Errorf("LSN mismatch: got %d, want %d", result.Lsn, original.Lsn)
	}
	if result.Action != original.Action {
		t.Errorf("Action mismatch: got %d, want %d", result.Action, original.Action)
	}
	if result.RootPageID != original.RootPageID {
		t.Errorf("rootPageID mismatch: got %d, want %d", result.RootPageID, original.RootPageID)
	}
	if result.NextPageID != original.NextPageID {
		t.Errorf("nextPageID mismatch: got %d, want %d", result.NextPageID, original.NextPageID)
	}
}

func TestSerializeDeserializeVacuum(t *testing.T) {
	// Create a WAL record
	original := &WALRecord{
		Lsn:        LSN(12345),
		Action:     CHECKPOINT,
		RootPageID: 1,
		NextPageID: 2,
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
	if result.Lsn != original.Lsn {
		t.Errorf("LSN mismatch: got %d, want %d", result.Lsn, original.Lsn)
	}
	if result.Action != original.Action {
		t.Errorf("Action mismatch: got %d, want %d", result.Action, original.Action)
	}
	if result.RootPageID != original.RootPageID {
		t.Errorf("rootPageID mismatch: got %d, want %d", result.RootPageID, original.RootPageID)
	}
	if result.NextPageID != original.NextPageID {
		t.Errorf("nextPageID mismatch: got %d, want %d", result.NextPageID, original.NextPageID)
	}
}
