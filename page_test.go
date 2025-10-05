package godatabase

import (
	"godb/internal/schema"
	"os"
	"testing"
)

func TestPageRoundTrip(t *testing.T) {
	// Create schema
	sch := schema.Schema{
		TableName: "users",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "name", Type: schema.StringType},
			{Name: "age", Type: schema.IntType},
		},
	}

	// Create test records
	records := []schema.Record{
		{"id": int32(1), "name": "alice", "age": int32(30)},
		{"id": int32(2), "name": "bob", "age": int32(25)},
		{"id": int32(3), "name": "charlie", "age": int32(35)},
	}

	// Create slotted page
	page := NewSlottedPage(1, LEAF)

	// Serialize and insert records
	for _, rec := range records {
		data, err := sch.SerializeRecord(rec)
		if err != nil {
			t.Fatalf("SerializeRecord failed: %v", err)
		}

		_, err = page.InsertRecord(data)
		if err != nil {
			t.Fatalf("InsertRecord failed: %v", err)
		}
	}

	// Verify we have 3 records
	if page.NumSlots != 3 {
		t.Errorf("expected 3 slots, got %d", page.NumSlots)
	}

	// Serialize page to disk format
	diskPage := page.Serialize()

	// Deserialize back to SlottedPage
	page2, err := DeserializeSlottedPage(diskPage)
	if err != nil {
		t.Fatalf("DeserializeSlottedPage failed: %v", err)
	}

	// Verify metadata matches
	if page2.PageID != page.PageID {
		t.Errorf("PageID mismatch: expected %d, got %d", page.PageID, page2.PageID)
	}
	if page2.PageType != page.PageType {
		t.Errorf("PageType mismatch: expected %d, got %d", page.PageType, page2.PageType)
	}
	if page2.NumSlots != page.NumSlots {
		t.Errorf("NumSlots mismatch: expected %d, got %d", page.NumSlots, page2.NumSlots)
	}

	// Verify each record
	for i := 0; i < len(records); i++ {
		recordData, err := page2.GetRecord(i)
		if err != nil {
			t.Fatalf("GetRecord(%d) failed: %v", i, err)
		}

		key, rec, err := sch.DeserializeRecord(recordData)
		if err != nil {
			t.Fatalf("DeserializeRecord failed: %v", err)
		}

		// Check key matches ID
		expectedKey := uint64(records[i]["id"].(int32))
		if key != expectedKey {
			t.Errorf("record %d: expected key %d, got %d", i, expectedKey, key)
		}

		// Check all fields match
		for fieldName, expectedVal := range records[i] {
			actualVal, ok := rec[fieldName]
			if !ok {
				t.Errorf("record %d: missing field %s", i, fieldName)
				continue
			}
			if actualVal != expectedVal {
				t.Errorf("record %d field %s: expected %v, got %v", i, fieldName, expectedVal, actualVal)
			}
		}
	}
}

func TestPageWithDiskManager(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test_page_*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create schema
	sch := schema.Schema{
		TableName: "products",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "name", Type: schema.StringType},
			{Name: "price", Type: schema.FloatType},
			{Name: "in_stock", Type: schema.BoolType},
		},
	}

	// Create table header
	header := TableHeader{
		Magic:      [4]byte{'G', 'D', 'B', 'T'},
		Version:    1,
		RootPageID: 1,
		NumPages:   2,
		Schema:     sch,
	}

	// Create disk manager
	dm := &DiskManager{
		file:   tmpFile,
		header: header,
	}

	// Write header
	err = dm.WriteHeader()
	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Create slotted page with records
	page := NewSlottedPage(1, LEAF)

	records := []schema.Record{
		{"id": int32(10), "name": "widget", "price": float64(9.99), "in_stock": true},
		{"id": int32(20), "name": "gadget", "price": float64(19.99), "in_stock": false},
		{"id": int32(30), "name": "gizmo", "price": float64(21.50), "in_stock": true},
	}

	for _, rec := range records {
		data, err := sch.SerializeRecord(rec)
		if err != nil {
			t.Fatalf("SerializeRecord failed: %v", err)
		}
		_, err = page.InsertRecord(data)
		if err != nil {
			t.Fatalf("InsertRecord failed: %v", err)
		}
	}

	// Write page to disk
	err = dm.WriteSlottedPage(page)
	if err != nil {
		t.Fatalf("WriteSlottedPage failed: %v", err)
	}

	// Read header back
	err = dm.ReadHeader()
	if err != nil {
		t.Fatalf("ReadHeader failed: %v", err)
	}

	// Verify header
	if string(dm.header.Magic[:]) != "GDBT" {
		t.Errorf("Magic mismatch: %s", string(dm.header.Magic[:]))
	}
	if dm.header.Schema.TableName != "products" {
		t.Errorf("Schema table name mismatch: %s", dm.header.Schema.TableName)
	}

	// Read page back
	page2, err := dm.ReadSlottedPage(1)
	if err != nil {
		t.Fatalf("ReadSlottedPage failed: %v", err)
	}

	// Verify records
	for i, expectedRec := range records {
		recordData, err := page2.GetRecord(i)
		if err != nil {
			t.Fatalf("GetRecord(%d) failed: %v", i, err)
		}

		key, rec, err := sch.DeserializeRecord(recordData)
		if err != nil {
			t.Fatalf("DeserializeRecord failed: %v", err)
		}

		expectedKey := uint64(expectedRec["id"].(int32))
		if key != expectedKey {
			t.Errorf("record %d: key mismatch: expected %d, got %d", i, expectedKey, key)
		}

		// Verify all fields
		if rec["name"] != expectedRec["name"] {
			t.Errorf("record %d: name mismatch", i)
		}
		if rec["price"] != expectedRec["price"] {
			t.Errorf("record %d: price mismatch", i)
		}
		if rec["in_stock"] != expectedRec["in_stock"] {
			t.Errorf("record %d: in_stock mismatch", i)
		}
	}
}

func TestKeyExtraction(t *testing.T) {
	sch := schema.Schema{
		TableName: "test",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "value", Type: schema.StringType},
		},
	}

	page := NewSlottedPage(1, LEAF)

	// Insert records with specific keys
	testCases := []struct {
		id  int32
		val string
	}{
		{100, "first"},
		{200, "second"},
		{50, "third"}, // Out of order intentionally
		{150, "fourth"},
	}

	for _, tc := range testCases {
		rec := schema.Record{"id": tc.id, "value": tc.val}
		data, err := sch.SerializeRecord(rec)
		if err != nil {
			t.Fatalf("SerializeRecord failed: %v", err)
		}
		_, err = page.InsertRecord(data)
		if err != nil {
			t.Fatalf("InsertRecord failed: %v", err)
		}
	}

	// Verify getKey() extracts correct keys
	for i, tc := range testCases {
		key := page.GetKey(i)
		expectedKey := uint64(tc.id)
		if key != expectedKey {
			t.Errorf("slot %d: expected key %d, got %d", i, expectedKey, key)
		}
	}

	// Verify keys are currently NOT sorted (inserted in order given)
	keys := []uint64{}
	for i := 0; i < int(page.NumSlots); i++ {
		keys = append(keys, page.GetKey(i))
	}

	// Keys should be: 100, 200, 50, 150 (insertion order)
	expectedOrder := []uint64{100, 200, 50, 150}
	for i, expected := range expectedOrder {
		if keys[i] != expected {
			t.Errorf("key order mismatch at position %d: expected %d, got %d", i, expected, keys[i])
		}
	}

	t.Logf("Keys in insertion order: %v", keys)
}

func TestPageDelete(t *testing.T) {
	sch := schema.Schema{
		TableName: "test",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "value", Type: schema.StringType},
		},
	}

	page := NewSlottedPage(1, LEAF)

	// Insert 3 records
	records := []schema.Record{
		{"id": int32(1), "value": "first"},
		{"id": int32(2), "value": "second"},
		{"id": int32(3), "value": "third"},
	}

	for _, rec := range records {
		data, _ := sch.SerializeRecord(rec)
		page.InsertRecord(data)
	}

	// Delete middle record
	err := page.DeleteRecord(1)
	if err != nil {
		t.Fatalf("DeleteRecord failed: %v", err)
	}

	// Try to get deleted record
	_, err = page.GetRecord(1)
	if err == nil {
		t.Error("Expected error when getting deleted record")
	}

	// Compact page
	page.Compact()

	// After compaction, should have 2 records
	if page.NumSlots != 2 {
		t.Errorf("After compaction, expected 2 slots, got %d", page.NumSlots)
	}

	// Verify remaining records
	data0, _ := page.GetRecord(0)
	_, rec0, _ := sch.DeserializeRecord(data0)
	if rec0["value"] != "first" {
		t.Errorf("Record 0 mismatch after compaction")
	}

	data1, _ := page.GetRecord(1)
	_, rec1, _ := sch.DeserializeRecord(data1)
	if rec1["value"] != "third" {
		t.Errorf("Record 1 mismatch after compaction")
	}
}

func TestSortedInsert(t *testing.T) {
	sch := schema.Schema{
		TableName: "test",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "value", Type: schema.StringType},
		},
	}

	page := NewSlottedPage(1, LEAF)

	// Insert records with specific keys
	testCases := []struct {
		id  int32
		val string
	}{
		{100, "first"},
		{200, "second"},
		{50, "third"}, // Out of order intentionally
		{150, "fourth"},
	}

	for _, tc := range testCases {
		rec := schema.Record{"id": tc.id, "value": tc.val}
		data, err := sch.SerializeRecord(rec)
		if err != nil {
			t.Fatalf("SerializeRecord failed: %v", err)
		}
		_, err = page.InsertRecordSorted(data)
		if err != nil {
			t.Fatalf("InsertRecord failed: %v", err)
		}
	}

	// Verify keys are sorted
	keys := []uint64{}
	for i := 0; i < int(page.NumSlots); i++ {
		keys = append(keys, page.GetKey(i))
	}

	// Keys should be sorted, NOT in insertion order
	expectedOrder := []uint64{50, 100, 150, 200}
	for i, expected := range expectedOrder {
		if keys[i] != expected {
			t.Errorf("key order mismatch at position %d: expected %d, got %d", i, expected, keys[i])
		}
	}

	t.Logf("Keys in insertion order: %v", keys)
}

func TestLeafSplit(t *testing.T) {
	sch := schema.Schema{
		TableName: "test",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "value", Type: schema.StringType},
		},
	}

	page := NewSlottedPage(1, LEAF)

	// Insert records with specific keys
	testCases := []struct {
		id  int32
		val string
	}{
		{10, "first"},
		{20, "second"},
		{30, "third"},
		{40, "fourth"},
		{50, "fifth"},
		{60, "sixth"},
	}

	for _, tc := range testCases {
		rec := schema.Record{"id": tc.id, "value": tc.val}
		data, err := sch.SerializeRecord(rec)
		if err != nil {
			t.Fatalf("SerializeRecord failed: %v", err)
		}
		_, err = page.InsertRecordSorted(data)
		if err != nil {
			t.Fatalf("InsertRecord failed: %v", err)
		}
	}

	newPage, promotedKey, err := page.SplitLeaf(2)
	if err != nil {
		t.Fatalf("SplitLeaf failed: %v", err)
	}

	if promotedKey != 40 {
		t.Errorf("Wrong key promoted: expected 40; actual: %d", promotedKey)
	}

	if newPage.PageID != 2 {
		t.Errorf("Wrong new page id: expected 2; actual: %d", newPage.PageID)
	}

	if page.NumSlots != 3 {
		t.Errorf("Left page should have 3 slots, got %d", page.NumSlots)
	}
	leftKeys := []uint64{10, 20, 30}
	for i, expected := range leftKeys {
		if page.GetKey(i) != expected {
			t.Errorf("Left page key %d: expected %d, got %d", i, expected, page.GetKey(i))
		}
	}

	if newPage.NumSlots != 3 {
		t.Errorf("Right page should have 3 slots, got %d", newPage.NumSlots)
	}

	rightKeys := []uint64{40, 50, 60}
	for i, expected := range rightKeys {
		if newPage.GetKey(i) != expected {
			t.Errorf("Left page key %d: expected %d, got %d", i, expected, newPage.GetKey(i))
		}
	}
}

func TestInternalSplit(t *testing.T) {
	page := NewSlottedPage(1, INTERNAL)
	page.RightmostChild = PageID(100) // arbitrary

	testKeys := []uint64{10, 20, 30, 40, 50}
	childPageIDs := []PageID{10, 20, 30, 40, 50}

	for i, key := range testKeys {
		data := SerializeInternalRecord(key, childPageIDs[i])
		_, err := page.InsertRecordSorted(data)
		if err != nil {
			t.Fatalf("InsertRecord failed: %v", err)
		}
	}

	newPage, promotedKey, err := page.SplitInternal(2)
	if err != nil {
		t.Fatalf("SplitInternal failed: %v", err)
	}

	if promotedKey != 30 {
		t.Errorf("Wrong key promoted: expected 30; actual: %d", promotedKey)
	}

	if newPage.PageID != 2 {
		t.Errorf("Wrong new page id: expected 2; actual: %d", newPage.PageID)
	}

	if page.NumSlots != 2 {
		t.Errorf("Left page should have 2 slots, got %d", page.NumSlots)
	}

	leftKeys := []uint64{10, 20}
	for i, expected := range leftKeys {
		if page.GetKey(i) != expected {
			t.Errorf("Left page key %d: expected %d, got %d", i, expected, page.GetKey(i))
		}
	}

	if newPage.NumSlots != 2 {
		t.Errorf("Right page should have 2 slots, got %d", newPage.NumSlots)
	}

	rightKeys := []uint64{40, 50}
	for i, expected := range rightKeys {
		if newPage.GetKey(i) != expected {
			t.Errorf("Left page key %d: expected %d, got %d", i, expected, newPage.GetKey(i))
		}
	}

	if page.RightmostChild != PageID(30) {
		t.Errorf("Left page RightmostChild: expected 30, got %d", page.RightmostChild)
	}

	if newPage.RightmostChild != PageID(100) {
		t.Errorf("Right page RightmostChild: expected 100, got %d", newPage.RightmostChild)
	}
}
