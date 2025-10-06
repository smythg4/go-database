package store

import (
	"fmt"
	"godb/internal/btree"
	"godb/internal/pager"
	"godb/internal/schema"
	"math"
	"os"
	"sync"
)

type BTreeStore struct {
	bt *btree.BTree
	mu sync.RWMutex
}

func NewBTreeStore(filename string) (*BTreeStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
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
	return &BTreeStore{bt: bt}, nil
}

func CreateBTreeStore(filename string, sch schema.Schema) (*BTreeStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
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
	return &BTreeStore{bt: bt}, nil
}

func (bts *BTreeStore) Insert(record schema.Record) error {
	bts.mu.Lock()
	defer bts.mu.Unlock()

	firstField := bts.bt.Header.Schema.Fields[0]
	id := record[firstField.Name].(int32)
	key := uint64(id)

	data, err := bts.bt.Header.Schema.SerializeRecord(record)
	if err != nil {
		return err
	}

	return bts.bt.Insert(key, data)
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
	_, result, err := bts.bt.Header.Schema.DeserializeRecord(data)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (bts *BTreeStore) ScanAll() ([]schema.Record, error) {
	bts.mu.RLock()
	defer bts.mu.RUnlock()

	// Get all records
	results, err := bts.bt.RangeScan(0, math.MaxUint64)
	if err != nil {
		return nil, err
	}

	// Deserialize each
	records := make([]schema.Record, 0, len(results))
	for _, data := range results {
		_, rec, err := bts.bt.Header.Schema.DeserializeRecord(data)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, nil
}
func (bts *BTreeStore) Close() error {
	// BTree doesn't need explicit closing, but we could sync here
	return nil
}

func (bts *BTreeStore) Schema() schema.Schema {
	return bts.bt.Header.Schema
}

func (bts *BTreeStore) Stats() string {
	return bts.bt.Stats()
}
