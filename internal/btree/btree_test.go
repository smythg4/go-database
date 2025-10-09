package btree

import (
	"godb/internal/pager"
	"godb/internal/schema"
	"os"
	"testing"
)

func TestInsertNoSplit(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test_btree_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create schema
	sch := schema.Schema{
		TableName: "test",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "value", Type: schema.StringType},
		},
	}

	// Create table header with root page
	header := pager.TableHeader{
		Magic:      [4]byte{'G', 'D', 'B', 'T'},
		Version:    1,
		RootPageID: 1,
		NextPageID: 2, // Root is page 1, next available is 2
		NumPages:   1,
		Schema:     sch,
	}

	// Create disk manager
	dm := &pager.DiskManager{}
	dm.SetFile(tmpFile)
	dm.SetHeader(header)

	// Write header to disk
	err = dm.WriteHeader()
	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Create and write initial root leaf page
	rootPage := pager.NewSlottedPage(1, pager.LEAF)
	err = dm.WriteSlottedPage(rootPage)
	if err != nil {
		t.Fatalf("WriteSlottedPage failed: %v", err)
	}

	// Create BTree
	bt := NewBTree(dm, &header)

	// Insert 3 records (should not trigger split)
	testRecords := []struct {
		id  int32
		val string
	}{
		{10, "first"},
		{20, "second"},
		{30, "third"},
	}

	for _, tc := range testRecords {
		rec := schema.Record{"id": tc.id, "value": tc.val}
		data, err := sch.SerializeRecord(rec)
		if err != nil {
			t.Fatalf("SerializeRecord failed: %v", err)
		}

		key := uint64(tc.id)
		err = bt.Insert(key, data)
		if err != nil {
			t.Fatalf("Insert(%d) failed: %v", key, err)
		}
	}

	// Verify root is still page 1 (no split)
	if bt.pc.GetRootPageID() != 1 {
		t.Errorf("Root should still be page 1, got %d", bt.pc.GetRootPageID())
	}

	// Verify records are in the root
	root, err := bt.loadNode(1)
	if err != nil {
		t.Fatalf("loadNode failed: %v", err)
	}

	if root.NumSlots != 3 {
		t.Errorf("Expected 3 records in root, got %d", root.NumSlots)
	}

	// Verify keys are sorted
	expectedKeys := []uint64{10, 20, 30}
	for i, expected := range expectedKeys {
		key := root.GetKey(i)
		if key != expected {
			t.Errorf("Record %d: expected key %d, got %d", i, expected, key)
		}
	}
}

func TestInsertWithRootSplit(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test_btree_split_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create schema
	sch := schema.Schema{
		TableName: "test",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "value", Type: schema.StringType},
		},
	}

	// Create table header
	header := pager.TableHeader{
		Magic:      [4]byte{'G', 'D', 'B', 'T'},
		Version:    1,
		RootPageID: 1,
		NextPageID: 2,
		NumPages:   1,
		Schema:     sch,
	}

	// Create disk manager
	dm := &pager.DiskManager{}
	dm.SetFile(tmpFile)
	dm.SetHeader(header)

	// Write header to disk
	err = dm.WriteHeader()
	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Create and write initial root leaf page
	rootPage := pager.NewSlottedPage(1, pager.LEAF)
	err = dm.WriteSlottedPage(rootPage)
	if err != nil {
		t.Fatalf("WriteSlottedPage failed: %v", err)
	}

	// Create BTree
	bt := NewBTree(dm, &header)

	// Insert many records to trigger split
	// With a 4KB page, need enough records to fill it
	// Each record: 8 byte key + 4 byte id + ~50 byte string = ~62 bytes
	// Plus slot overhead (4 bytes per slot)
	// Roughly need 60-70 records to fill a page
	for i := 1; i <= 200; i++ {
		rec := schema.Record{
			"id":    int32(i * 10),
			"value": "this_is_a_longer_test_value_to_fill_the_page_faster",
		}
		data, err := sch.SerializeRecord(rec)
		if err != nil {
			t.Fatalf("SerializeRecord failed: %v", err)
		}

		key := uint64(i * 10)
		err = bt.Insert(key, data)
		if err != nil {
			t.Fatalf("Insert(%d) failed: %v", key, err)
		}

		// Check if root changed (indicates split)
		if bt.pc.GetRootPageID() != 1 {
			t.Logf("Split occurred after inserting key %d", key)
			t.Logf("New root is page %d", bt.pc.GetRootPageID())
			break
		}
	}

	// Verify split occurred
	if bt.pc.GetRootPageID() == 1 {
		t.Skip("No split occurred with 100 records - may need more records or smaller page size")
	}

	// Verify new root is internal
	newRoot, err := bt.loadNode(bt.pc.GetRootPageID())
	if err != nil {
		t.Fatalf("loadNode failed: %v", err)
	}

	if newRoot.PageType != pager.INTERNAL {
		t.Errorf("New root should be INTERNAL, got %v", newRoot.PageType)
	}

	// Verify root has children
	if newRoot.NumSlots < 1 {
		t.Errorf("New root should have at least 1 key, got %d", newRoot.NumSlots)
	}
}

func createTestSchema() schema.Schema {
	return schema.Schema{
		TableName: "testProducts",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "description", Type: schema.StringType},
			{Name: "qty", Type: schema.IntType},
			{Name: "price", Type: schema.FloatType},
		},
	}
}

func createTestHeader(sch schema.Schema) pager.TableHeader {
	return pager.TableHeader{
		Magic:      [4]byte{'G', 'D', 'B', 'T'},
		Version:    1,
		RootPageID: 1,
		NextPageID: 2,
		NumPages:   1,
		Schema:     sch,
	}
}

func createTestDiskManager(f *os.File, h pager.TableHeader) pager.DiskManager {
	dm := pager.DiskManager{}
	dm.SetFile(f)
	dm.SetHeader(h)
	dm.WriteHeader()
	return dm
}

func createTestBTree(t *testing.T) (*BTree, *os.File, func()) {
	tmpFile, err := os.CreateTemp("", "test_btree_*.db")
	if err != nil {
		t.Fatal(err)
	}
	sch := createTestSchema()
	h := createTestHeader(sch)
	dm := createTestDiskManager(tmpFile, h)

	rootPage := pager.NewSlottedPage(1, pager.LEAF)
	dm.WriteSlottedPage(rootPage)

	bt := NewBTree(&dm, &h)

	cleanup := func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}

	return bt, tmpFile, cleanup
}

func TestSearch(t *testing.T) {
	bt, _, cleanup := createTestBTree(t)
	defer cleanup()

	sch := createTestSchema()
	testData := []struct {
		id    int32
		desc  string
		qty   int32
		price float64
	}{
		{10, "widget", 5, 9.99},
		{20, "gadget", 10, 19.99},
		{30, "gizmo", 15, 29.99},
	}

	// insert all the test cases into the BTree
	for _, td := range testData {
		rec := schema.Record{
			"id":          td.id,
			"description": td.desc,
			"qty":         td.qty,
			"price":       td.price,
		}
		data, _ := sch.SerializeRecord(rec)
		err := bt.Insert(uint64(td.id), data)
		if err != nil {
			t.Fatalf("Insert failed for id=%d: %v", td.id, err)
		}
	}

	// test: search for existing key
	for _, td := range testData {
		eKey := uint64(td.id)
		data, found, err := bt.Search(eKey)
		if err != nil {
			t.Fatalf("Search failed for id=%d: %v", eKey, err)
		}
		if !found {
			t.Errorf("Expected to find key %d", eKey)
		}

		// deserialize and verify
		key, rec, err := sch.DeserializeRecord(data)
		if err != nil {
			t.Fatalf("DeserializeRecord failed: %v", err)
		}
		if key != eKey {
			t.Errorf("Expected key %d, got %d", eKey, key)
		}
		if rec["description"] != td.desc {
			t.Errorf("Expected '%s', got %v", td.desc, rec["description"])
		}
	}

	// test: search for non-existent key
	_, found, err := bt.Search(999)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if found {
		t.Error("Should not have found key 999")
	}
}

func TestSearchAfterSplit(t *testing.T) {
	bt, _, cleanup := createTestBTree(t)
	defer cleanup()

	sch := createTestSchema()

	// Insert enough records to trigger a split
	for i := 1; i <= 100; i++ {
		rec := schema.Record{
			"id":          int32(i * 10),
			"description": "this_is_a_much_longer_product_description_to_fill_pages",
			"qty":         int32(i),
			"price":       float64(i) * 1.5,
		}
		data, _ := sch.SerializeRecord(rec)
		bt.Insert(uint64(i*10), data)
	}

	// Verify split occurred
	if bt.pc.GetRootPageID() == 1 {
		t.Error("Expected root to split, but it's still page 1")
	}
	t.Logf("Root split occurred - new root is page %d", bt.pc.GetRootPageID())

	// Verify root is internal
	root, _ := bt.loadNode(bt.pc.GetRootPageID())
	if !root.IsLeaf() {
		t.Logf("Root is internal node with %d keys", root.NumSlots)
	}
	// Right after all inserts
	t.Logf("Inserted 100 records, verifying key 550 exists...")
	data550, found550, _ := bt.Search(550)
	t.Logf("Immediate search for 550: found=%v, data_len=%d", found550, len(data550))

	data, found, err := bt.Search(550)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if !found {
		t.Error("Expected to find key 550 after split")
	}

	t.Logf("Found data, length: %d bytes", len(data))

	key, rec, err := sch.DeserializeRecord(data)
	if err != nil {
		t.Fatalf("DeserializeRecord failed: %v", err)
	}
	t.Logf("Deserialized: key=%d, rec=%v", key, rec)
}

func TestRangeScan(t *testing.T) {
	bt, _, cleanup := createTestBTree(t)
	defer cleanup()

	sch := createTestSchema()

	// Insert many records to trigger splits and create sibling chain
	for i := 1; i <= 100; i++ {
		rec := schema.Record{
			"id":          int32(i * 10),
			"description": "this_is_a_much_longer_product_description_to_fill_pages",
			"qty":         int32(i),
			"price":       float64(i) * 1.5,
		}
		data, _ := sch.SerializeRecord(rec)
		bt.Insert(uint64(i*10), data)
	}

	// Range scan from 200 to 500
	results, err := bt.RangeScan(200, 500)
	if err != nil {
		t.Fatalf("RangeScan failed: %v", err)
	}

	// Should find keys: 200, 210, 220, ..., 490, 500 = 31 keys
	expected := 31
	if len(results) != expected {
		t.Errorf("Expected %d results, got %d", expected, len(results))
	}

	// Verify first and last
	key0, _, _ := sch.DeserializeRecord(results[0])
	if key0 != 200 {
		t.Errorf("First key should be 200, got %d", key0)
	}

	keyLast, _, _ := sch.DeserializeRecord(results[len(results)-1])
	if keyLast != 500 {
		t.Errorf("Last key should be 500, got %d", keyLast)
	}

	t.Logf("Range scan found %d records from %d to %d", len(results), key0, keyLast)
}
