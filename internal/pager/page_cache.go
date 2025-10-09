package pager

import (
	"errors"
	"fmt"
	"godb/internal/schema"
	"os"
	"sync"
	"time"
)

// background flusher for future use
func (pc *PageCache) backgroundFlusher(pagesToWrite <-chan PageID) error {
	// will need a terminate signal

	batch := make([]PageID, 0, 10)
	ticker := time.NewTicker(100 * time.Millisecond) // you can play with this timinig

	for {
		select {
		case pg := <-pagesToWrite:
			batch = append(batch, pg)
			if len(batch) >= 10 {
				if err := pc.flushBatch(batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if err := pc.flushBatch(batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}

	return nil
}

const maxCacheSize = 50

type CacheRecord struct {
	id       PageID
	data     *SlottedPage
	isDirty  bool
	pinCount int
}

func NewCacheRecord(sp *SlottedPage) CacheRecord {
	return CacheRecord{
		id:       sp.PageID,
		data:     sp,
		isDirty:  false,
		pinCount: 0,
	}
}

type PageCache struct {
	fifoQueue []PageID
	cache     map[PageID]*CacheRecord
	header    *TableHeader
	dm        *DiskManager
	mu        sync.Mutex
}

func NewPageCache(dm *DiskManager, th *TableHeader) PageCache {
	return PageCache{
		fifoQueue: []PageID{},
		cache:     make(map[PageID]*CacheRecord, maxCacheSize),
		header:    th,
		dm:        dm,
	}
}

func (pc *PageCache) AllocatePage() PageID {
	if len(pc.header.FreePageIDs) > 0 {
		pageID := pc.header.FreePageIDs[len(pc.header.FreePageIDs)-1]
		pc.header.FreePageIDs = pc.header.FreePageIDs[:len(pc.header.FreePageIDs)-1]
		return pageID
	}

	pageID := pc.header.NextPageID
	pc.header.NextPageID++
	return pageID
}

func (pc *PageCache) AddNewPage(sp *SlottedPage) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if _, exists := pc.cache[sp.PageID]; exists {
		return fmt.Errorf("page %d already cached", sp.PageID)
	}

	if err := pc.CachePage(sp); err != nil {
		return err
	}

	// mark new page dirty and pin it
	pc.cache[sp.PageID].isDirty = true
	pc.cache[sp.PageID].pinCount++

	return nil
}

func (pc *PageCache) FreePage(id PageID) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// remove the page from the cache -- forcefully
	delete(pc.cache, id)

	// remove from the fifoQueue if present
	for i, qid := range pc.fifoQueue {
		if qid == id {
			pc.fifoQueue = append(pc.fifoQueue[:i], pc.fifoQueue[i+1:]...)
			// keep it going just in case there's duplicates
		}
	}
	pc.header.FreePageIDs = append(pc.header.FreePageIDs, id)
}

func (pc *PageCache) GetRootPageID() PageID {
	return pc.header.RootPageID
}

func (pc *PageCache) SetRootPageID(id PageID) {
	pc.header.RootPageID = id
}

func (pc *PageCache) Fetch(id PageID) (sp *SlottedPage, err error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	cr, exists := pc.cache[id]
	if !exists {
		// retrieve page from disk
		sp, err = pc.dm.ReadSlottedPage(id)
		if err != nil {
			return nil, err
		}

		// cache the page
		if err = pc.CachePage(sp); err != nil {
			return nil, err
		}

		cr = pc.cache[id]
	}

	// Slotted Page found in cache
	cr.pinCount++
	return cr.data, nil
}

func (pc *PageCache) CachePage(sp *SlottedPage) error {
	// check the cache to see if it's already there
	_, exists := pc.cache[sp.PageID]
	if exists {
		return nil
	}

	// if cache is full, initiate an eviction
	if len(pc.cache) >= maxCacheSize {
		fmt.Printf("DEBUG: Cache FULL (%d pages), evicting to make room for page %d\n", len(pc.cache), sp.PageID)
		if err := pc.Evict(); err != nil {
			return err
		}
		if len(pc.cache) >= maxCacheSize {
			return errors.New("unable to evict cache to make room")
		}
	}

	// cache the page and update the fifoQueue
	ncr := NewCacheRecord(sp)
	pc.cache[sp.PageID] = &ncr
	pc.fifoQueue = append(pc.fifoQueue, sp.PageID)
	return nil
}

func (pc *PageCache) MakeDirty(id PageID) error {
	cr, exists := pc.cache[id]
	if !exists {
		return errors.New("attempting to dirty a page that isn't cached")
	}
	cr.isDirty = true
	return nil
}

func (pc *PageCache) Evict() error {
	pinnedSkips := 0
	for len(pc.fifoQueue) > 0 {
		id := pc.fifoQueue[0]
		cr, exists := pc.cache[id]

		if !exists {
			// stale entry, skip it
			pc.fifoQueue = pc.fifoQueue[1:]
			continue
		}

		if cr.pinCount > 0 {
			// pinned, skip for now
			pc.fifoQueue = pc.fifoQueue[1:]
			pc.fifoQueue = append(pc.fifoQueue, cr.id)
			pinnedSkips++
			continue
		}

		if cr.isDirty {
			if err := pc.flushRecord(cr); err != nil {
				return err
			}
			fmt.Printf("DEBUG: Evicted dirty page %d (pinned skips: %d, cache size: %d)\n", id, pinnedSkips, len(pc.cache))
		} else {
			fmt.Printf("DEBUG: Evicted clean page %d (pinned skips: %d, cache size: %d)\n", id, pinnedSkips, len(pc.cache))
		}

		pc.fifoQueue = pc.fifoQueue[1:]
		delete(pc.cache, id)
		return nil
	}
	fmt.Printf("DEBUG: Evict FAILED - all %d pages pinned\n", len(pc.cache))
	return errors.New("all pages pinned")
}

func (pc *PageCache) writeRecord(cr *CacheRecord) error {
	return pc.dm.WriteSlottedPage(cr.data)
}

func (pc *PageCache) flushRecord(cr *CacheRecord) error {
	if err := pc.writeRecord(cr); err != nil {
		return err
	}
	if err := pc.dm.Sync(); err != nil {
		return err
	}
	cr.isDirty = false
	return nil
}

func (pc *PageCache) flushBatch(ids []PageID) error {
	// for use with backgroundFlusher goroutine
	for _, id := range ids {
		cr, exists := pc.cache[id]
		if !exists {
			continue
		}
		if err := pc.writeRecord(cr); err != nil {
			return err
		}
	}
	if err := pc.dm.Sync(); err != nil {
		return err
	}
	for _, id := range ids {
		cr, exists := pc.cache[id]
		if !exists {
			continue
		}
		cr.isDirty = false
	}
	return nil
}

func (pc *PageCache) flushAll() error {
	for _, cr := range pc.cache {
		if err := pc.writeRecord(cr); err != nil {
			return err
		}
	}
	if err := pc.dm.Sync(); err != nil {
		return err
	}
	// also flush the header to disk
	if err := pc.FlushHeader(); err != nil {
		return err
	}
	for _, cr := range pc.cache {
		cr.isDirty = false
	}
	return nil
}

func (pc *PageCache) Pin(id PageID) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	cr, exists := pc.cache[id]
	if exists {
		cr.pinCount++
	}
}

func (pc *PageCache) UnPin(id PageID) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	cr, exists := pc.cache[id]
	if exists {
		cr.pinCount--
	}
}

func (pc *PageCache) GetHeader() *TableHeader {
	return pc.header
}

func (pc *PageCache) FlushHeader() error {
	pc.header.NumPages = uint32(pc.header.NextPageID - 1)
	pc.dm.SetHeader(*pc.header)
	return pc.dm.WriteHeader()
}

func (pc *PageCache) GetSchema() schema.Schema {
	return pc.header.Schema
}

func (pc *PageCache) Close() error {
	// flush everything to the disk first
	if err := pc.flushAll(); err != nil {
		return err
	}
	return pc.dm.Close()
}

func (pc *PageCache) UpdateFile(file *os.File) error {
	// switch to new file (after vacuum rename)
	pc.dm.SetFile(file)
	if err := pc.dm.ReadHeader(); err != nil {
		return err
	}

	// update header pointer
	newHeader := pc.dm.GetHeader()
	pc.header = newHeader

	// clear cache (pages are from old file)
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.cache = make(map[PageID]*CacheRecord, maxCacheSize)
	pc.fifoQueue = []PageID{}
	return nil
}

func (pc *PageCache) Contains(id PageID) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	_, exists := pc.cache[id]
	return exists
}
