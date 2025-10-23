package store

import (
	"context"
	"errors"
	"fmt"
	"godb/internal/btree"
	"godb/internal/pager"
	"godb/internal/schema"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

type BTreeStore struct {
	bt         *btree.BTree
	wal        *pager.WALManager
	tableBloom *BloomFilter

	wg  *sync.WaitGroup
	ctx context.Context

	mu sync.RWMutex
}

func NewBTreeStore(filename string, ctx context.Context, wg *sync.WaitGroup) (*BTreeStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	walFileName := strings.TrimSuffix(filename, ".db") + ".wal"
	wm, err := pager.NewWalManager(walFileName, ctx, wg)
	if err != nil {
		return nil, err
	}

	dm := &pager.DiskManager{}
	dm.SetFile(file)
	stat, _ := file.Stat()
	if stat.Size() == 0 {
		sch := schema.Schema{
			TableName: "table",
			Fields: []schema.Field{
				{Name: "id", Type: schema.IntType},
				{Name: "name", Type: schema.StringType},
				{Name: "age", Type: schema.IntType},
			},
		}
		dm.SetHeader(pager.DefaultTableHeader(sch))
		dm.WriteHeader()
		rootPage := pager.NewSlottedPage(1, pager.LEAF)
		dm.WriteSlottedPage(rootPage)
	} else {
		err = dm.ReadHeader()
		if err != nil {
			return nil, err
		}
	}

	header := dm.GetHeader()
	bt := btree.NewBTree(dm, header)
	bts := &BTreeStore{bt: bt, wal: wm, ctx: ctx, wg: wg}

	// Replay WAL to recover any uncommitted operations
	if err := bts.Recover(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	if err := bts.rebuildBloomFilter(); err != nil {
		return nil, err
	}

	wg.Add(1)
	go bts.startCheckpointer()
	return bts, nil
}

func CreateBTreeStore(filename string, sch schema.Schema, ctx context.Context, wg *sync.WaitGroup) (*BTreeStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	walFileName := strings.TrimSuffix(filename, ".db") + ".wal"
	wm, err := pager.NewWalManager(walFileName, ctx, wg)
	if err != nil {
		return nil, err
	}

	dm := &pager.DiskManager{}
	dm.SetFile(file)
	stat, _ := file.Stat()

	if stat.Size() == 0 {
		dm.SetHeader(pager.DefaultTableHeader(sch))
		dm.WriteHeader()
		rootPage := pager.NewSlottedPage(1, pager.LEAF)
		dm.WriteSlottedPage(rootPage)
	} else {
		return nil, fmt.Errorf("file already exists: %s", filename)
	}

	header := dm.GetHeader()
	bt := btree.NewBTree(dm, header)
	bts := &BTreeStore{bt: bt, wal: wm, ctx: ctx, wg: wg}

	// Replay WAL to recover any uncommitted operations (if WAL exists)
	if err := bts.Recover(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	if err := bts.rebuildBloomFilter(); err != nil {
		return nil, err
	}

	wg.Add(1)
	go bts.startCheckpointer()
	return bts, nil
}

func (bts *BTreeStore) startCheckpointer() {
	defer bts.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := bts.Checkpoint(); err != nil {
				log.Printf("Backgound checkpoint failed: %v", err)
			}
			fmt.Printf("DEBUG: checkpoint hit at %v\n", time.Now().UTC())
		case <-bts.ctx.Done():
			if err := bts.Checkpoint(); err != nil {
				log.Printf("Backgound checkpoint failed: %v", err)
			}
			fmt.Printf("DEBUG: checkpoint hit at %v\n", time.Now().UTC())
			return
		}
	}
}

func (bts *BTreeStore) Insert(record schema.Record) error {
	bts.mu.Lock()
	defer bts.mu.Unlock()

	key, err := bts.bt.ExtractPrimaryKey(record)
	if err != nil {
		return fmt.Errorf("insert: failed to extract primary key from table '%s': %w", bts.Schema().TableName, err)
	}

	data, err := bts.bt.SerializeRecord(record)
	if err != nil {
		return fmt.Errorf("insert: failed to serialize record: %w", err)
	}

	if err := bts.LogInsert(key, data); err != nil {
		return fmt.Errorf("insert: failed to log WAL insert: %w", err)
	}

	if bts.tableBloom != nil {
		bts.tableBloom.Add(key)
	}

	return bts.bt.Insert(key, data)
}

func (bts *BTreeStore) Delete(key uint64) error {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	if err := bts.LogDelete(key); err != nil {
		return fmt.Errorf("delete: failed to log WAL delete: %w", err)
	}
	return bts.bt.Delete(key)
}

func (bts *BTreeStore) Find(key int) (schema.Record, error) {
	bts.mu.RLock()
	defer bts.mu.RUnlock()

	if bts.tableBloom != nil && !bts.tableBloom.MayContain(uint64(key)) {
		// key definitely not in table
		return nil, fmt.Errorf("record %d not found", key)
	}

	data, found, err := bts.bt.Search(uint64(key))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("record %d not found", key)
	}
	_, result, err := bts.bt.DeserializeRecord(data)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (bts *BTreeStore) RangeScan(startKey, endKey uint64) ([]schema.Record, error) {
	bts.mu.RLock()
	defer bts.mu.RUnlock()

	results, err := bts.bt.RangeScan(startKey, endKey)
	if err != nil {
		return nil, err
	}

	records := make([]schema.Record, 0, len(results))
	for _, data := range results {
		_, rec, err := bts.bt.DeserializeRecord(data)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, nil
}

func (bts *BTreeStore) Vacuum() error {
	if err := bts.LogVacuum(); err != nil {
		return fmt.Errorf("vacuum: failed to log WAL vacuum: %w", err)
	}
	if err := bts.bt.Vacuum(); err != nil {
		return err
	}

	// Rebuild bloom filter after vacuum to remove false positives from deletes
	return bts.rebuildBloomFilter()
}

func (bts *BTreeStore) ScanAll() ([]schema.Record, error) {
	return bts.RangeScan(0, math.MaxUint64)
}
func (bts *BTreeStore) Close() error {
	return bts.bt.Close()
}

func (bts *BTreeStore) Schema() schema.Schema {
	return bts.bt.GetSchema()
}

func (bts *BTreeStore) Stats() string {
	return bts.bt.Stats()
}

func (bts *BTreeStore) ExtractPrimaryKey(record schema.Record) (uint64, error) {
	return bts.bt.ExtractPrimaryKey(record)
}

func (bts *BTreeStore) Recover() error {
	records, err := bts.wal.ReadAll()
	if err != nil {
		// If WAL is empty or doesn't exist, nothing to recover
		if errors.Is(err, io.EOF) {
			log.Printf("WAL recovery: WAL file is empty or doesn't exist (EOF)")
			return nil
		}
		return fmt.Errorf("recovery: failed to read WAL: %w", err)
	}

	// No records to recover
	if len(records) == 0 {
		log.Printf("WAL recovery: No records found in WAL")
		return nil
	}

	log.Printf("WAL recovery: Found %d records to replay", len(records))
	for _, record := range records {
		switch record.Action {
		case pager.INSERT:
			if err := bts.bt.Insert(uint64(record.Key), record.RecordBytes); err != nil {
				return fmt.Errorf("recovery: failed to replay INSERT for key %d: %w", record.Key, err)
			}
		case pager.DELETE:
			if err := bts.bt.Delete(uint64(record.Key)); err != nil {
				return fmt.Errorf("recovery: failed to replay DELETE for key %d: %w", record.Key, err)
			}
		default:
			return fmt.Errorf("unsupported action: %v", record.Action)
		}
	}
	return nil
}

func (bts *BTreeStore) Checkpoint() error {
	bts.mu.Lock()
	defer bts.mu.Unlock()

	// Write checkpoint START marker
	if err := bts.LogCheckpoint(); err != nil {
		return fmt.Errorf("checkpoint: failed to log checkpoint in WAL: %w", err)
	}

	// Flush pages
	if err := bts.bt.Checkpoint(); err != nil {
		return fmt.Errorf("checkpoint: failed to flush pages: %w", err)
	}

	// Sync to ensure pages are durable
	// Now safe to truncate WAL
	return bts.wal.Truncate()
}

func (bts *BTreeStore) Commit(txnBuffer []pager.WALRecord) error {
	// 1. Log actions
	done := make(chan error, 1)
	bts.wal.RequestChan <- pager.WALRequest{
		Records: txnBuffer,
		Done:    done,
	}
	if err := <-done; err != nil {
		return fmt.Errorf("commit - received error from wal buffer: %w", err)
	}

	// 3. now apply all operations to the tree
	for _, record := range txnBuffer {
		switch record.Action {
		case pager.INSERT:
			if err := bts.bt.Insert(uint64(record.Key), record.RecordBytes); err != nil {
				return fmt.Errorf("commit: failed to INSERT key %d: %w", record.Key, err)
			}
		case pager.DELETE:
			if err := bts.bt.Delete(uint64(record.Key)); err != nil {
				return fmt.Errorf("commit: failed to DELETE key %d: %w", record.Key, err)
			}
		case pager.CHECKPOINT:
			if err := bts.bt.Checkpoint(); err != nil {
				return fmt.Errorf("commit: failed to log checkpoint in WAL: %w", err)
			}
		case pager.VACUUM:
			if err := bts.bt.Vacuum(); err != nil {
				return fmt.Errorf("commit: failed to VACUUM: %w", err)
			}
		default:
			return fmt.Errorf("unsupported action: %v", record.Action)
		}
	}
	return nil
}

func (bts *BTreeStore) PrepareInsert(record schema.Record) (pager.WALRecord, error) {
	key, err := bts.bt.ExtractPrimaryKey(record)
	if err != nil {
		return pager.WALRecord{}, fmt.Errorf("failed to extract primary key for table '%s': %w", bts.Schema().TableName, err)
	}

	data, err := bts.bt.SerializeRecord(record)
	if err != nil {
		return pager.WALRecord{}, fmt.Errorf("failed to serialized record: %w", err)
	}

	return pager.WALRecord{
		Action:       pager.INSERT,
		Key:          pager.WalKey(key),
		RecordBytes:  data,
		RecordLength: uint32(len(data)),
	}, nil
}

func (bts *BTreeStore) PrepareDelete(key uint64) (pager.WALRecord, error) {

	return pager.WALRecord{
		Action: pager.DELETE,
		Key:    pager.WalKey(key),
	}, nil
}

func (bts *BTreeStore) LogCheckpoint() error {
	rpi, npi := bts.bt.GetWalMetadata()
	if err := bts.wal.LogCheckpoint(rpi, npi); err != nil {
		return fmt.Errorf("failed to log WAL checkpoint: %w", err)
	}
	return nil
}

func (bts *BTreeStore) LogVacuum() error {
	rpi, npi := bts.bt.GetWalMetadata()
	if err := bts.wal.LogVacuum(rpi, npi); err != nil {
		return fmt.Errorf("failed to log WAL vacuum: %w", err)
	}
	return nil
}

func (bts *BTreeStore) LogInsert(key uint64, recordBytes []byte) error {
	if err := bts.wal.LogInsert(key, recordBytes); err != nil {
		return fmt.Errorf("failed to log WAL insert: %w", err)
	}
	return nil
}

func (bts *BTreeStore) LogDelete(key uint64) error {
	if err := bts.wal.LogDelete(key); err != nil {
		return fmt.Errorf("failed to log WAL delete: %w", err)
	}
	return nil
}

func (bts *BTreeStore) LogUpdate(key uint64, recordBytes []byte) error {
	if err := bts.wal.LogUpdate(key, recordBytes); err != nil {
		return fmt.Errorf("failed to log WAL update: %w", err)
	}
	return nil
}

func (bts *BTreeStore) rebuildBloomFilter() error {
	// Count existing records to size bloom filter appropriately
	// NOTE: caller must hold lock

	records, err := bts.ScanAll()
	if err != nil {
		return err
	}

	// Create bloom filter sized for current data + growth
	numKeys := len(records) * 2 // 2x for growth headroom
	if numKeys == 0 {
		numKeys = 1000
	}

	numKeys *= 2

	numBits, numHashes := OptimalBloomSize(uint(numKeys), 0.01)

	bts.tableBloom = NewBloomFilter(numBits, numHashes)

	// Add all existing keys
	for _, record := range records {
		key, err := bts.bt.ExtractPrimaryKey(record)
		if err != nil {
			return err
		}
		bts.tableBloom.Add(key)
	}

	return nil
}
