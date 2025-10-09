package pager

import (
	"godb/internal/schema"
	"os"
	"testing"
)

// Test helpers

func createTestSchema() schema.Schema {
	return schema.Schema{
		TableName: "test_table",
		Fields: []schema.Field{
			{Name: "id", Type: schema.IntType},
			{Name: "name", Type: schema.StringType},
		},
	}
}

func createTestHeader() TableHeader {
	return DefaultTableHeader(createTestSchema())
}

func createTestPage(id PageID, pageType PageType) *SlottedPage {
	sp := NewSlottedPage(id, pageType)
	// Add some dummy data to make it non-empty
	key := uint64(id)
	record := make([]byte, 16)
	// Write key into first 8 bytes
	for i := 0; i < 8; i++ {
		record[i] = byte(key >> (i * 8))
	}
	sp.InsertRecordSorted(record)
	return sp
}

func createTestPageCache(t *testing.T) (*PageCache, *DiskManager, string) {
	// Create temporary file
	tmpfile, err := os.CreateTemp("", "test_cache_*.db")
	if err != nil {
		t.Fatal(err)
	}

	dm := &DiskManager{}
	dm.SetFile(tmpfile)

	header := createTestHeader()
	dm.SetHeader(header)
	dm.WriteHeader()

	// Write some test pages to disk
	for i := 1; i <= 60; i++ {
		page := createTestPage(PageID(i), LEAF)
		dm.WriteSlottedPage(page)
	}

	pc := NewPageCache(dm, &header)

	return &pc, dm, tmpfile.Name()
}

func cleanupTestFile(filename string) {
	os.Remove(filename)
}

// fillCache loads pages into cache until it's full
func fillCache(t *testing.T, pc *PageCache, count int) []PageID {
	loaded := []PageID{}
	for i := 1; i <= count; i++ {
		_, err := pc.Fetch(PageID(i))
		if err != nil {
			t.Fatalf("Failed to fetch page %d: %v", i, err)
		}
		loaded = append(loaded, PageID(i))
	}
	return loaded
}

// Tests

func TestFetchPinsPage(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	// Fetch page 1
	page, err := pc.Fetch(1)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if page == nil {
		t.Fatal("Expected page, got nil")
	}

	// Check pin count
	cr := pc.cache[1]
	if cr.pinCount != 1 {
		t.Errorf("Expected pinCount=1, got %d", cr.pinCount)
	}
}

func TestFetchSamePageTwice(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	// Fetch page 1 twice
	pc.Fetch(1)
	pc.Fetch(1)

	cr := pc.cache[1]
	if cr.pinCount != 2 {
		t.Errorf("Expected pinCount=2 after two fetches, got %d", cr.pinCount)
	}
}

func TestUnPinDecrements(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	pc.Fetch(1)
	pc.Fetch(1)

	pc.UnPin(1)
	cr := pc.cache[1]
	if cr.pinCount != 1 {
		t.Errorf("Expected pinCount=1 after unpin, got %d", cr.pinCount)
	}

	pc.UnPin(1)
	if cr.pinCount != 0 {
		t.Errorf("Expected pinCount=0 after second unpin, got %d", cr.pinCount)
	}
}

func TestEvictionRespectsPins(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	// Fill cache to capacity (50 pages)
	pages := fillCache(t, pc, maxCacheSize)

	// unpin everything
	for _, id := range pages {
		pc.UnPin(id)
	}

	// Pin page 1 so it can't be evicted
	pc.Pin(1)

	// Fetch page 51, should trigger eviction
	_, err := pc.Fetch(51)
	if err != nil {
		t.Fatalf("Fetch of page 51 failed: %v", err)
	}

	// Page 1 should still be in cache (pinned)
	if _, exists := pc.cache[1]; !exists {
		t.Error("Page 1 was evicted despite being pinned")
	}

	// Page 51 should be in cache
	if _, exists := pc.cache[51]; !exists {
		t.Error("Page 51 not in cache after fetch")
	}
}

func TestFIFOEvictionOrder(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	// Fill cache to capacity (50 pages)
	pages := fillCache(t, pc, maxCacheSize)

	// unpin everything
	for _, id := range pages {
		pc.UnPin(id)
	}

	// Fetch one more page to trigger eviction
	pc.Fetch(PageID(maxCacheSize + 1))

	// Page 1 (first in) should have been evicted
	if _, exists := pc.cache[1]; exists {
		t.Error("Page 1 should have been evicted (FIFO)")
	}

	// Page 2 should still be there
	if _, exists := pc.cache[2]; !exists {
		t.Error("Page 2 should still be in cache")
	}
}

func TestMakeDirty(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	pc.Fetch(1)

	err := pc.MakeDirty(1)
	if err != nil {
		t.Fatalf("MakeDirty failed: %v", err)
	}

	cr := pc.cache[1]
	if !cr.isDirty {
		t.Error("Page should be marked dirty")
	}
}

func TestCacheHitVsMiss(t *testing.T) {
	pc, dm, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	// First fetch - cache miss, should read from disk
	page1, err := pc.Fetch(1)
	if err != nil {
		t.Fatalf("First fetch failed: %v", err)
	}

	// Second fetch - cache hit, should return same pointer
	page2, err := pc.Fetch(1)
	if err != nil {
		t.Fatalf("Second fetch failed: %v", err)
	}

	// Should be the exact same pointer (cache hit)
	if page1 != page2 {
		t.Error("Cache hit should return same page pointer")
	}

	// Verify it's actually from cache, not disk
	// (We can't easily verify disk reads without instrumenting DiskManager,
	//  but checking pointer equality is a good proxy)
	_ = dm // silence unused warning
}

func TestAllocatePageIncrementsNextPageID(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	initialNextPageID := pc.header.NextPageID

	id1 := pc.AllocatePage()
	if id1 != initialNextPageID {
		t.Errorf("Expected first allocation to be %d, got %d", initialNextPageID, id1)
	}

	id2 := pc.AllocatePage()
	if id2 != initialNextPageID+1 {
		t.Errorf("Expected second allocation to be %d, got %d", initialNextPageID+1, id2)
	}

	if pc.header.NextPageID != initialNextPageID+2 {
		t.Errorf("Expected NextPageID to be %d, got %d", initialNextPageID+2, pc.header.NextPageID)
	}
}

func TestFreePageReuse(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	// Allocate a page
	id1 := pc.AllocatePage()

	// Free it
	pc.FreePage(id1)

	// Allocate again - should get the freed page back
	id2 := pc.AllocatePage()
	if id2 != id1 {
		t.Errorf("Expected to reuse freed page %d, got %d", id1, id2)
	}

	// Free list should be empty now
	if len(pc.header.FreePageIDs) != 0 {
		t.Errorf("Expected empty free list, got %d pages", len(pc.header.FreePageIDs))
	}
}

func TestAddNewPageCachesAndMarksDirty(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	pageID := pc.AllocatePage()
	sp := NewSlottedPage(pageID, LEAF)

	err := pc.AddNewPage(sp)
	if err != nil {
		t.Fatalf("AddNewPage failed: %v", err)
	}

	// Should be in cache
	cr, exists := pc.cache[pageID]
	if !exists {
		t.Fatal("Page should be in cache after AddNewPage")
	}

	// Should be marked dirty
	if !cr.isDirty {
		t.Error("New page should be marked dirty")
	}

	// Should be pinned (from the implicit CachePage call)
	if cr.pinCount != 0 {
		t.Logf("Note: pinCount is %d (AddNewPage doesn't pin)", cr.pinCount)
	}
}

func TestAddNewPageRejectsDuplicate(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	// Fetch existing page
	pc.Fetch(1)

	// Try to add it as new
	sp := NewSlottedPage(1, LEAF)
	err := pc.AddNewPage(sp)

	if err == nil {
		t.Error("Expected error when adding duplicate page, got nil")
	}
}

func TestRootPageIDGetSet(t *testing.T) {
	pc, _, filename := createTestPageCache(t)
	defer cleanupTestFile(filename)

	initialRoot := pc.GetRootPageID()

	newRoot := PageID(42)
	pc.SetRootPageID(newRoot)

	if pc.GetRootPageID() != newRoot {
		t.Errorf("Expected root %d, got %d", newRoot, pc.GetRootPageID())
	}

	// Verify it actually updated the header
	if pc.header.RootPageID != newRoot {
		t.Errorf("Header not updated: expected %d, got %d", newRoot, pc.header.RootPageID)
	}

	_ = initialRoot // silence unused warning
}
