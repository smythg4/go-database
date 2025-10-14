package pager

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

	// Delete middle record (index 1 = "second")
	err := page.DeleteRecord(1)
	if err != nil {
		t.Fatalf("DeleteRecord failed: %v", err)
	}

	// After delete+compact, should have 2 records
	// DeleteRecord calls Compact() automatically, so records shift immediately
	if page.NumSlots != 2 {
		t.Errorf("After delete, expected 2 slots, got %d", page.NumSlots)
	}

	// Verify remaining records (first and third)
	data0, err := page.GetRecord(0)
	if err != nil {
		t.Fatalf("GetRecord(0) failed: %v", err)
	}
	_, rec0, _ := sch.DeserializeRecord(data0)
	if rec0["value"] != "first" {
		t.Errorf("Record 0 should be 'first', got %v", rec0["value"])
	}

	data1, err := page.GetRecord(1)
	if err != nil {
		t.Fatalf("GetRecord(1) failed: %v", err)
	}
	_, rec1, _ := sch.DeserializeRecord(data1)
	if rec1["value"] != "third" {
		t.Errorf("Record 1 should be 'third' (shifted down after delete), got %v", rec1["value"])
	}

	// Trying to access index 2 should fail (out of range)
	_, err = page.GetRecord(2)
	if err == nil {
		t.Error("Expected error when accessing out of range index 2")
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

	newPage, promotedKey, err := page.SplitLeaf(2, false)
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

	newPage, promotedKey, err := page.SplitInternal(2, false)
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

func TestFragmentedMerge(t *testing.T) {
	sch := schema.Schema{
		TableName: "test",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "value", Type: schema.StringType},
		},
	}

	// Create two leaf pages
	leftPage := NewSlottedPage(1, LEAF)
	rightPage := NewSlottedPage(2, LEAF)

	// Insert 5 records into left page
	for i := 1; i <= 5; i++ {
		rec := schema.Record{"id": int32(i * 10), "value": "left"}
		data, _ := sch.SerializeRecord(rec)
		leftPage.InsertRecordSorted(data)
	}

	// Insert 3 records into right page
	for i := 6; i <= 8; i++ {
		rec := schema.Record{"id": int32(i * 10), "value": "right"}
		data, _ := sch.SerializeRecord(rec)
		rightPage.InsertRecordSorted(data)
	}

	// Delete some records to reduce left page size
	// DeleteRecord calls Compact() automatically, so no tombstones remain
	leftPage.DeleteRecord(1) // Delete id=20 (index 1)
	leftPage.DeleteRecord(1) // Delete id=30 (now at index 1 after previous delete shifted)

	// After DeleteRecord+Compact, NumSlots == len(Slots) == len(Records)
	if leftPage.NumSlots != 3 {
		t.Errorf("Expected 3 active slots after deletes, got %d", leftPage.NumSlots)
	}
	if len(leftPage.Slots) != 3 {
		t.Errorf("Expected 3 total slots (no tombstones after compact), got %d", len(leftPage.Slots))
	}

	// Check that pages can merge
	if !leftPage.CanMergeWith(rightPage) {
		t.Fatal("Pages should be able to merge")
	}

	// Record keys before merge: 10, 40, 50 from left (20,30 deleted) + 60, 70, 80 from right
	expectedKeys := []uint64{10, 40, 50, 60, 70, 80}

	// Merge right into left
	err := leftPage.MergeLeaf(rightPage)
	if err != nil {
		t.Fatalf("MergeLeaf failed: %v", err)
	}

	// After merge, should have 6 active records
	if leftPage.NumSlots != 6 {
		t.Errorf("Expected 6 active slots after merge, got %d", leftPage.NumSlots)
	}

	// Should have no tombstones
	if len(leftPage.Slots) != 6 {
		t.Errorf("Expected 6 total slots (no tombstones), got %d", len(leftPage.Slots))
	}

	// Verify all keys are present and in order
	for i, expectedKey := range expectedKeys {
		actualKey := leftPage.GetKey(i)
		if actualKey != expectedKey {
			t.Errorf("Key at index %d: expected %d, got %d", i, expectedKey, actualKey)
		}
	}

	// Verify all records are accessible
	for i := 0; i < int(leftPage.NumSlots); i++ {
		_, err := leftPage.GetRecord(i)
		if err != nil {
			t.Errorf("Failed to get record at index %d: %v", i, err)
		}
	}
}
