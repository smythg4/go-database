package pager

import (
	"errors"
	"fmt"
	"godb/internal/schema"
	"os"
	"sync"
)

const maxCacheSize = 250

type CacheRecord struct {
	id       PageID
	data     *SlottedPage
	isDirty  bool
	pinCount int
	refBit   bool
}

func NewCacheRecord(sp *SlottedPage) CacheRecord {
	return CacheRecord{
		id:       sp.PageID,
		data:     sp,
		isDirty:  false,
		pinCount: 0,
		refBit:   false,
	}
}

type PageCache struct {
	//dirtyPageChan chan PageID
	clockQueue []PageID
	clockHand  int
	cache      map[PageID]*CacheRecord
	header     *TableHeader
	dm         *DiskManager
	mu         sync.Mutex
}

func NewPageCache(dm *DiskManager, th *TableHeader) *PageCache {
	pc := PageCache{
		//dirtyPageChan: make(chan PageID, 100),
		clockQueue: make([]PageID, maxCacheSize),
		clockHand:  0,
		cache:      make(map[PageID]*CacheRecord, maxCacheSize),
		header:     th,
		dm:         dm,
	}
	//go pc.backgroundFlusher()
	return &pc
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

	for i, qid := range pc.clockQueue {
		if qid == id {
			pc.clockQueue[i] = 0
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
	cr.refBit = true
	cr.pinCount++
	return cr.data, nil
}

func (pc *PageCache) CachePage(sp *SlottedPage) error {
	// find empty slot or evict
	for pc.clockQueue[pc.clockHand] != 0 {
		fmt.Printf("DEBUG: Clock sweeping (cache %d/%d), need room for page %d\n",
			len(pc.cache), maxCacheSize, sp.PageID)
		if err := pc.Evict(); err != nil {
			fmt.Printf("ERROR: flushRecord failed for page %d: %v\n", sp.PageID, err)
			return err
		}
	}

	// insert at current position
	ncr := NewCacheRecord(sp)
	ncr.refBit = true
	pc.cache[sp.PageID] = &ncr
	pc.clockQueue[pc.clockHand] = sp.PageID
	pc.advanceClock()
	return nil
}

func (pc *PageCache) MakeDirty(id PageID) error {
	cr, exists := pc.cache[id]
	if !exists {
		return errors.New("attempting to dirty a page that isn't cached")
	}
	cr.isDirty = true
	// select {
	// case pc.dirtyPageChan <- id:
	// default:
	// }
	return nil
}

func (pc *PageCache) Evict() error {
	startPos := pc.clockHand

	for {
		// get page at clock hand
		id := pc.clockQueue[pc.clockHand]
		cr := pc.cache[id]

		if cr == nil {
			// stale entry, skip
			pc.advanceClock()
			continue
		}

		if cr.pinCount > 0 {
			// pinned, skip (but don't clear refBit)
			pc.advanceClock()

			// detect full rotation through all pinned pages
			if pc.clockHand == startPos {
				return errors.New("all pages pinned")
			}
			continue
		}

		if cr.refBit {
			// give second chance
			cr.refBit = false
			pc.advanceClock()
			continue
		}

		// found victim (unpinned, refBit=0)
		fmt.Printf("DEBUG: Evicting page %d (dirty=%v) at position %d\n", id, cr.isDirty,
			pc.clockHand)
		if cr.isDirty {
			if err := pc.flushRecord(cr); err != nil {
				fmt.Printf("ERROR: flushRecord failed for page %d: %v\n", id, err)
				return err
			}
		}

		delete(pc.cache, id)
		pc.clockQueue[pc.clockHand] = 0
		fmt.Printf("DEBUG: Successfully evicted page %d, cache now %d/%d\n", id,
			len(pc.cache), maxCacheSize)
		return nil
	}

}

func (pc *PageCache) advanceClock() {
	pc.clockHand = (pc.clockHand + 1) % len(pc.clockQueue)
}

func (pc *PageCache) writeRecord(cr *CacheRecord) error {
	return pc.dm.WriteSlottedPage(cr.data)
}

func (pc *PageCache) flushRecord(cr *CacheRecord) error {
	if err := pc.writeRecord(cr); err != nil {
		return err
	}
	// shouldn't need to fsync on every flush now that we have WAL
	// if err := pc.dm.Sync(); err != nil {
	// 	return err
	// }
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

func (pc *PageCache) FlushAll() error {
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
	if err := pc.FlushAll(); err != nil {
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
	pc.clockQueue = make([]PageID, maxCacheSize)
	pc.clockHand = 0
	return nil
}

func (pc *PageCache) Contains(id PageID) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	_, exists := pc.cache[id]
	return exists
}

func (pc *PageCache) ReplaceTreeFromPages(pages []*SlottedPage, rootID PageID) error {
	// phase 3: write all pages to the new file and update header
	tempFile := pc.GetSchema().TableName + ".db.tmp"
	f, err := os.Create(tempFile)
	if err != nil {
		return err
	}

	// Cleanup temp file on error
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tempFile) // Clean up if we return early with error
		}
	}()

	tempDM := NewDiskManager(f)

	// create header pointing to root
	freshHeader := DefaultTableHeader(pc.GetSchema())
	freshHeader.RootPageID = rootID
	freshHeader.NextPageID = PageID(len(pages) + 1)
	freshHeader.NumPages = uint32(len(pages))
	tempDM.SetHeader(freshHeader)
	if err := tempDM.WriteHeader(); err != nil {
		return err
	}

	// write all pages
	for _, page := range pages {
		if err := tempDM.WriteSlottedPage(page); err != nil {
			return err
		}
	}

	// sync and close temp file
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// close old file, rename, reopen
	if err := pc.Close(); err != nil {
		return err
	}

	origFile := pc.GetSchema().TableName + ".db"
	if err := os.Rename(tempFile, origFile); err != nil {
		return err
	}

	f, err = os.OpenFile(origFile, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	return pc.UpdateFile(f)
}
