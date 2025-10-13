package store

import (
	"context"
	"fmt"
	"godb/internal/btree"
	"godb/internal/pager"
	"godb/internal/schema"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

type BTreeStore struct {
	bt  *btree.BTree
	wal *pager.WALManager

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
		return err
	}

	data, err := bts.bt.SerializeRecord(record)
	if err != nil {
		return err
	}

	if err := bts.LogInsert(key, data); err != nil {
		return err
	}

	// CRASH HERE: WAL written, but tree not modified
	// if key == 999 { // Special test key
	// 	fmt.Println("SIMULATING CRASH!")
	// 	os.Exit(1)
	// }

	return bts.bt.Insert(key, data)
}

func (bts *BTreeStore) Delete(key uint64) error {
	bts.mu.Lock()
	defer bts.mu.Unlock()
	if err := bts.LogDelete(key); err != nil {
		return err
	}
	return bts.bt.Delete(key)
}

func (bts *BTreeStore) Find(key int) (schema.Record, error) {
	bts.mu.RLock()
	defer bts.mu.RUnlock()

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
		return err
	}
	return bts.bt.Vacuum()
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
		return err
	}

	for _, record := range records {
		switch record.Action {
		case pager.INSERT:
			if err := bts.bt.Insert(uint64(record.Key), record.RecordBytes); err != nil {
				return err
			}
		case pager.DELETE:
			if err := bts.bt.Delete(uint64(record.Key)); err != nil {
				return err
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
		return err
	}

	// Flush pages
	if err := bts.bt.Checkpoint(); err != nil {
		return err
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
		return err
	}

	// 3. now apply all operations to the tree
	for _, record := range txnBuffer {
		switch record.Action {
		case pager.INSERT:
			if err := bts.bt.Insert(uint64(record.Key), record.RecordBytes); err != nil {
				return err
			}
		case pager.DELETE:
			if err := bts.bt.Delete(uint64(record.Key)); err != nil {
				return err
			}
		case pager.CHECKPOINT:
			if err := bts.bt.Checkpoint(); err != nil {
				return err
			}
		case pager.VACUUM:
			if err := bts.bt.Vacuum(); err != nil {
				return err
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
		return pager.WALRecord{}, err
	}

	data, err := bts.bt.SerializeRecord(record)
	if err != nil {
		return pager.WALRecord{}, err
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
		return err
	}
	return nil
}

func (bts *BTreeStore) LogVacuum() error {
	rpi, npi := bts.bt.GetWalMetadata()
	if err := bts.wal.LogVacuum(rpi, npi); err != nil {
		return err
	}
	return nil
}

func (bts *BTreeStore) LogInsert(key uint64, recordBytes []byte) error {
	if err := bts.wal.LogInsert(key, recordBytes); err != nil {
		return err
	}
	return nil
}

func (bts *BTreeStore) LogDelete(key uint64) error {
	if err := bts.wal.LogDelete(key); err != nil {
		return err
	}
	return nil
}

func (bts *BTreeStore) LogUpdate(key uint64, recordBytes []byte) error {
	if err := bts.wal.LogUpdate(key, recordBytes); err != nil {
		return err
	}
	return nil
}
