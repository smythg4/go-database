# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A handrolled database implementation in Go with multi-client TCP server support, custom schema system, and persistent binary storage. Built as a learning project following "Database Internals" by Alex Petrov, with the goal of eventually implementing B-tree page-based storage.

## Architecture

### Active Storage Layer (`internal/store`)

**btree_store.go** - Primary storage implementation using B+ tree (current backend):
- `BTreeStore`: Thin wrapper around B+ tree with schema-aware operations
- Implements full CRUD interface (Insert, Find, Delete, ScanAll)
- `sync.RWMutex` for concurrent access protection (Insert/Delete use Lock, Find/ScanAll use RLock)
- Key methods:
  - `Insert(record)`: Extracts first field as uint64 key, serializes record, calls bt.Insert
  - `Delete(key uint64)`: Deletes record from tree, may trigger merges
  - `Find(id)`: Searches B+ tree via bt.Search, deserializes result
  - `ScanAll()`: Full table scan via bt.RangeScan(0, MaxUint64), deserializes all records
  - `Schema()`: Returns read-only schema from bt.Header.Schema
  - `Stats()`: Exposes tree structure for debugging (root page, type, NextPageID)
- Constructor helpers: `NewBTreeStore(filename)` opens existing, `CreateBTreeStore(filename, schema)` creates new
- **Critical pattern**: Primary key (first field) must be int32, cast to uint64 for tree operations

**table.go** - Legacy append-only log storage (replaced by BTreeStore):
- `TableStore`: Schema-based table storage with binary serialization
- File format: Schema header (table name, field definitions) + variable-length records
- Deduplication on read: `ScanAll()` returns latest record per ID
- Helper functions: `writeValue/readValue` for type-aware serialization
- **Status**: No longer used by CLI, kept for reference

**kv.go** - Simple key-value storage (legacy, still used for testing):
- Append-only int32 key-value pairs as 8-byte records
- Linear backward search for latest value

### Schema System (`internal/schema`)

**schema.go** - Type definitions and parsing:
- `Schema`: Table name + field definitions
- `Field`: Name and FieldType (IntType=0, StringType=1, BoolType=2, FloatType=3)
- `Record`: map[string]any for row data
- `ParseFieldType(string)`: Converts "int", "string", "bool", "float" to FieldType enum
- `ParseValue(string, FieldType)`: Parses user input to typed values

### CLI/Network Layer (`cmd/main.go` + `internal/cli`)

**main.go** - Entry point with dual interface:
- Local REPL: Interactive prompt showing current table name
- TCP Server: Listens on port 42069, handles concurrent connections
- `ProcessCommand`: Routes commands through `io.Writer` abstraction (works for stdout or net.Conn)
- Per-session config: Each TCP client gets isolated `DatabaseConfig` but shares TableStore instances via cache

**commands.go** - Command implementations:
- Table cache: `GetOrOpenTable(filename)` ensures single BTreeStore instance per file (critical for mutex to work)
- Commands write to `io.Writer` parameter, return errors
- SQL-like: `create`, `use`, `show`, `describe`, `insert`, `select [id] [start end]`, `update <values>`, `delete <id>`, `count [id] [start end]`, `drop <table>`, `stats`
- Dynamic schema: INSERT/SELECT/UPDATE adapt to current table's field definitions
- Formatted output: Column-aligned tables with field names as headers
- **Range queries**: `select <start> <end>` and `count <start> <end>` use RangeScan for efficient range operations
- **Schema introspection**: `describe` shows table structure with field types and primary key
- `delete <id>`: Finds record by key, displays it, then calls Delete (triggers merges, updates free list)
- `update <values>`: Naive implementation using DELETE + INSERT (shows record being updated)
- `drop <table>`: Removes .db file (includes graceful handling for non-existent files)
- `stats` command: Shows tree structure (root page ID, type, NextPageID allocation, tree depth)

### B+ Tree Storage Engine (`internal/pager` + `internal/btree`)

**Page Cache Layer (`internal/pager/page_cache.go`)** - Buffer pool for page caching:
- `PageCache`: In-memory cache with Clock eviction policy (maxCacheSize = 200 pages)
- `CacheRecord`: Tracks page data, dirty flag, pin count, and reference bit for each cached page
- Pin/unpin semantics prevent eviction of in-use pages during tree operations
- Key methods:
  - `Fetch(id)`: Returns page from cache or loads from disk, auto-pins on fetch, sets refBit
  - `AddNewPage(sp)`: Adds new page to cache, marks dirty, and pins it
  - `UnPin(id)`: Decrements pin count, allowing eviction when count reaches 0
  - `Evict()`: Clock eviction with second-chance algorithm, flushes dirty pages before eviction
  - `MakeDirty(id)`: Marks cached page as dirty (needs flush on eviction)
  - `AllocatePage()`: Allocates new PageID, checks FreePageIDs first
  - `FreePage(id)`: Adds PageID to free list for reuse
  - `FlushHeader()`: Writes table header to disk, auto-calculates NumPages
  - `Close()`: Flushes all dirty pages and closes underlying file
  - `UpdateFile(file)`: Switches to new file after VACUUM rename, clears cache
- **Critical pattern**: BTree operations must pin pages during use, unpin via defer when done
- **Eviction strategy**: Clock algorithm gives "second chance" to recently accessed pages via refBit before eviction
- **Header management**: PageCache owns header pointer, BTree accesses via Get/Set methods
- **Concurrency**: PageCache.mu protects cache map and clock queue, BTreeStore.mu serializes BTree access
- **Cache size**: 200 pages (800KB with 4KB pages), sized to handle deep tree operations without exhausting cache

**Page Layer (`internal/pager/page.go`)** - Slotted page implementation:
- `SlottedPage`: In-memory representation with 13-byte header, slot array, and records
  - Header: PageType (LEAF/INTERNAL), NumSlots, FreeSpacePtr, RightmostChild, NextLeaf
  - NextLeaf forms sibling chain for efficient range scans (B+ tree feature)
  - Slot array grows down from byte 13, records grow up from end
  - 4KB fixed page size (PAGE_SIZE = 4096)
- Key methods:
  - `InsertRecordSorted`: Binary search insertion maintaining key order
  - `DeleteRecord`: Tombstone approach (mark Offset=0, Length=0, Records[i]=nil, NumSlots--), bounds check uses `len(Records)` not NumSlots
  - `Search`: **Linear scan** for exact key match in leaf nodes (binary search breaks with tombstones), skips nil Records, early exit when key > target
  - `SearchInternal`: Routes search to correct child page in internal nodes, binary search with tombstone skip (moves right when mid is tombstone)
  - `findInsertionPosition`: Binary search for insertion position, skips tombstones during search
  - `SplitLeaf/SplitInternal`: Node splitting with promoted key handling and child pointer updates, **calls Compact() first** to ensure NumSlots == len(Records)
  - `MergeLeaf/MergeInternals`: Combine two nodes (updates NextLeaf chain for leaves), iterates over `len(sibling.Records)` not NumSlots, calls Compact first
  - `CanMergeWith`: Check if two pages fit together (combined size + slot overhead ≤ PAGE_SIZE)
  - `IsUnderfull`: Returns true if GetUsedSpace() < PAGE_SIZE/2
  - `GetUsedSpace()`: Calculates space used by active records only (skips tombstones where Offset=0)
  - `Compact`: Removes tombstone gaps by repacking active records, called before splits/merges
  - `Serialize/Deserialize`: Convert between in-memory (13-byte header) and disk ([4096]byte array)
- Record formats:
  - Leaf: [key: 8 bytes][full record data: variable]
  - Internal: [key: 8 bytes][child PageID: 4 bytes]
- First field in schema is always the key
- **Tombstone model**: DeleteRecord doesn't compact immediately, allows tombstones to accumulate, Compact called only before splits/merges
- **Critical tombstone bug pattern**: `NumSlots` = active record count (decrements on delete), `len(Records)` = array size with tombstones. ALL iteration must use `len(Records)`, ALL bounds checks must use `len(Records)`. Using NumSlots causes records at high indices to be skipped.

**Table Metadata (`internal/pager/header.go`)**:
- `TableHeader`: Magic ("GDBT"), version, RootPageID, NextPageID, NumPages, Schema, FreePageIDs
- Page 0 reserved for header (padded to 4KB), B-tree nodes start at page 1
- NextPageID tracks next available page for allocation during splits
- **FreePageIDs**: Slice of PageIDs freed during merges, reused before allocating new pages
- `DefaultTableHeader(schema)`: Helper for creating initial headers with schema
- **Critical durability pattern**: BTree modifies its own header copy (RootPageID/NextPageID/FreePageIDs), must sync back to DiskManager before WriteHeader
- **Serialization**: FreePageIDs serialized as uint32 count + array of PageIDs (4 bytes each)

**Disk I/O (`internal/pager/disk_manager.go`)**:
- `ReadPage/WritePage`: Load/store raw Page at offset (pageID * PAGE_SIZE)
- `ReadSlottedPage/WriteSlottedPage`: High-level wrappers with serialization
- `ReadHeader/WriteHeader`: Table metadata at offset 0 (WriteHeader calls Sync())
- `Sync()`: Flushes OS buffers to disk (critical for durability)
- **WritePage now calls Sync()**: Each page write is immediately flushed (performance cost, but ensures durability)

**B-Tree Orchestration (`internal/btree/btree.go`)**:
- `BTree`: Manages tree structure via PageCache (which wraps DiskManager)
- `BNode`: Thin wrapper around SlottedPage for tree-specific operations
- `NewBTree(dm, header)`: Constructor creates PageCache, returns BTree with pc field
- **Integration with PageCache**:
  - `loadNode(id)`: Calls `pc.Fetch(id)`, wraps in BNode (auto-pins page)
  - `writeNode(node)`: Checks if page cached via `pc.Contains()`, adds via `AddNewPage()` if not, then calls `MakeDirty()`
  - All loadNode calls followed by `defer bt.pc.UnPin(node.PageID)` to allow eviction
  - New pages from splits immediately pinned via AddNewPage, unpinned at end of operation
- **Free Page Management**:
  - `allocatePage()`: Calls `pc.AllocatePage()` which checks FreePageIDs first
  - Merge operations call `pc.FreePage(orphanedPageID)` to add to free list
  - **No mutex protection**: BTreeStore's RWMutex serializes all BTree access, so FreePageIDs is implicitly protected
- Key algorithms:
  - `Insert`: Uses breadcrumb stack to track descent path, handles splits bottom-up
    - **Critical**: Defer pattern flushes header at end: `defer bt.pc.FlushHeader()`
    - Pins pages during descent, unpins via defer when done
  - `Delete`: Traverses to leaf, deletes record, writes leaf BEFORE checking underflow, handles merges
    - **Critical**: Same defer pattern as Insert for header flush
    - Writes modified leaf before underflow check to ensure durability
    - Pins pages during descent, unpins via defer when done
  - `propagateSplit`: Inserts promoted keys into parents, cascades splits up tree
  - `handleRootSplit`: Creates new internal root when root overflows (tree height growth)
  - `handleUnderflow`: Detects underflow, finds sibling, attempts merge, recurses on parent underflow
    - **Critical**: Writes parent BEFORE checking parent underflow (prevents stale parent pointers)
  - `handleRootUnderflow`: Collapses root when internal root has NumSlots=0 (only RightmostChild remains)
  - `mergeLeafNodes/mergeInternalNodes`: Combines siblings, updates parent pointers with tombstone awareness
    - Internal merge demotes separator key: `SerializeInternalRecord(separatorKey, leftNode.RightmostChild)`
    - **Tombstone handling**: After DeleteRecord, finds next non-tombstone record to update pointer (records don't shift)
  - `findLeftSibling/findRightSibling`: Navigates parent to locate merge partners
  - `Search`: Descends tree using SearchInternal/Search, max depth check prevents infinite loops
  - `RangeScan`: Follows NextLeaf sibling chain with cycle detection, iterates over `len(leaf.Records)` not NumSlots to include all records
  - `Stats()`: Debug helper showing root page ID, type, NextPageID allocation
  - `Close()`: Calls `pc.Close()` which flushes all dirty pages and closes file
  - `findLeftSibling/findRightSibling`: Skip tombstones when navigating parent records to find siblings (backward/forward scan)
  - `Vacuum`: Triggers bulk loading rebuild via `BulkLoad()`
  - `BulkLoad`: O(n) tree reconstruction - scans leaves sequentially, builds dense leaf layer, constructs internal layers bottom-up, writes to temp file, renames atomically
- Breadcrumb stack pattern (from Petrov): Tracks PageID and child index during descent for split/merge propagation
- Child pointer management: After inserting promoted key at position i, updates record[i+1] or RightmostChild

**Critical Implementation Details:**
- Split propagation updates child pointers: When `[key, leftPageID]` inserted at index i, the next record (i+1) must point to rightPageID, or RightmostChild if last
- Sibling chain maintenance: During SplitLeaf, `newPage.NextLeaf = oldPage.NextLeaf` then `oldPage.NextLeaf = newPageID`
- **Header durability**: BTree modifies header via PageCache methods (SetRootPageID, AllocatePage, FreePage), FlushHeader syncs to disk. Implemented as defer at start of Insert/Delete.
- **Pin/unpin discipline**: Every loadNode must have corresponding UnPin, typically via defer. New pages from AddNewPage also pinned.
- **Tombstone-aware pointer updates**: After DeleteRecord in merge operations, must find next non-tombstone record to update child pointer (records don't shift with tombstone model)
- Search safety: Max depth 100 prevents infinite loops from corrupted page structures
- Primary key constraint: First field in schema must be int32 (cast to uint64 for key operations)

**Test Coverage (`internal/btree/btree_test.go` + `internal/pager/page_test.go`)**:
- Insert without split (single leaf)
- Insert with root split (tree height 1 → 2)
- Search in single-level and multi-level trees
- Range scan across multiple leaves via sibling pointers
- Page serialization round-trips
- Split mechanics (leaf/internal with child pointer validation)
- **Integration testing**: Stress test with 150 inserts verified multi-level tree growth (root split from page 1 → page 3)

## Development Commands

```bash
# Build and run with server logs to stderr
go build -o godb cmd/main.go && ./godb 2>server.log

# Run the database (logs to stderr by default)
./godb

# Connect via TCP
nc localhost 42069

# Format and vet
go fmt ./...
go vet ./...

# Run all tests
go test ./...

# Run B-tree tests
go test -v ./internal/btree/

# Run specific test
go test -v -run TestInsertWithRootSplit ./internal/btree/

# Run page-related tests
go test -v ./internal/pager/

# Benchmark B+Tree vs legacy storage
go test -bench=. -benchmem ./internal/store/

# Stress tests (require running server)
./test_concurrent.sh   # 5000 concurrent inserts across 5 clients
./test_chaos.sh        # Concurrent inserts + deletes (tests merges and free list)
./test_freelist.sh     # Verifies free page reuse after chaos test
./stress_test.sh       # 10000 sequential inserts
./stress_delete.sh     # Delete many records (tests merge logic)
```

## Key Design Decisions

**Binary Encoding:**
- All integers: LittleEndian encoding via `encoding/binary`
- Strings: 4-byte length prefix + UTF-8 bytes
- Floats: `math.Float64bits` converts to uint64, then LittleEndian
- Bools: Single byte (1=true, 0=false)

**Concurrency Model:**
- Table cache in `cli` package ensures single `BTreeStore` per file
- `sync.RWMutex` on BTreeStore serializes tree access (Insert uses Lock, Find/ScanAll use RLock)
- B+ tree storage provides better concurrency characteristics than legacy append-only log

**Command Processing:**
- Commands take `io.Writer` to support both stdout (REPL) and `net.Conn` (TCP)
- `bufio.Writer` wrapper on TCP connections requires explicit `Flush()` after each command
- Per-session `DatabaseConfig` allows clients to switch tables independently

**Schema System:**
- Dynamic field parsing: Commands query `config.TableS.Schema().Fields` at runtime (method call, not field access)
- Type parsing via switch statements in `schema.ParseValue`
- Schema stored in file header (page 0), read on open (no external metadata files)
- **Primary key constraint**: First field must be int32 type (used as uint64 key in B+ tree)

## Important Implementation Details

- Schema field order is preserved during serialization (critical for binary format consistency)
- Float parsing in Go: `strconv.ParseFloat(s, 64)` for user input
- Table names in prompts: Read from `BTreeStore.Schema().TableName` (file header), not filename
- Server logs go to stderr (`log.SetOutput(os.Stderr)`) to avoid REPL interference
- Primary key extraction: `record[firstField.Name].(int32)` type assertion, cast to uint64 for tree key
- Stats command useful for debugging tree structure: shows root page, type (LEAF=0/INTERNAL=1), NextPageID

## Current Development Status

B+ tree page-based storage is **completed and fully integrated** with the CLI:

**Completed:**
- ✅ Slotted page layout with serialization/deserialization
- ✅ Binary search for sorted key insertion and lookup
- ✅ Leaf node splits with promoted key handling
- ✅ Internal node splits with RightmostChild pointer management
- ✅ Page-level I/O with DiskManager
- ✅ Table header with PageID allocation tracking
- ✅ Comprehensive test suite for pages and splits
- ✅ BTree orchestration with recursive insertion and split propagation
- ✅ Root split handling (creates new root with 2 children)
- ✅ Search implementation with tree traversal
- ✅ Range scan with sibling pointer traversal
- ✅ BTreeStore wrapper implementing TableStore interface
- ✅ Full CLI integration (create, use, insert, select, stats commands)
- ✅ Header durability fix (sync BTree changes back to DiskManager)
- ✅ Stress testing with 150 inserts verified multi-level tree growth

**✅ DELETE & MERGE Implementation (COMPLETED)**

**Phase 1 - DELETE with Merge Support:**
- ✅ Tree traversal to find key in leaf
- ✅ Record removal with tombstone + immediate compact pattern
- ✅ Underflow detection (`IsUnderfull()` checks if used space < PAGE_SIZE/2)
- ✅ Leaf node merging when siblings can fit together
- ✅ Internal node merging with separator key demotion
- ✅ Parent pointer updates after DeleteRecord (slots shift down)
- ✅ Recursive underflow propagation up the tree
- ✅ Root collapse when internal root has only RightmostChild
- ✅ Durability: `Sync()` in WritePage ensures all writes persist
- ✅ CLI: `delete <id>`, `update <values>`, `count`, `describe`, `drop` commands
- ✅ **Free page list**: Orphaned pages from merges added to FreePageIDs, reused on next allocatePage()

**Critical Bugs Fixed:**
- ✅ **Parent write before recursion**: Parent updates must be persisted BEFORE recursive underflow handling, otherwise stale parent pointers point to orphaned pages
- ✅ **Leaf write before underflow check**: Modified leaf must be written to disk BEFORE checking underflow (same pattern as parent fix)
- ✅ **Cycle detection in RangeScan**: Detects corrupted leaf chains that visit pages twice
- ✅ **Catastrophic durability bug**: Pages written to OS buffer but never synced. Fixed by adding `Sync()` to `WritePage()`. Caught via stress testing with 5000 concurrent inserts.
- ✅ **8 Tombstone bugs** (NumSlots vs len(Records) confusion):
  1. `Search` in leaf nodes - binary search breaks with tombstones (GetKey returns 0), fixed with linear scan
  2. `DeleteRecord` bounds check - used NumSlots, fixed to use len(Records)
  3. `RangeScan` iteration - used NumSlots, skipped high-index records, fixed to use len(Records)
  4. `MergeLeaf` iteration - used sibling.NumSlots, orphaned records, fixed to use len(sibling.Records)
  5. `MergeInternals` iteration - same issue, fixed to use len(sibling.Records)
  6. `SplitLeaf` iteration - used NumSlots as index bound with tombstone array, caused nil insertions, fixed with Compact-first pattern
  7. `SplitInternal` iteration - same issue, fixed with Compact-first pattern
  8. `findRightSibling` deserialization - accessed childIndex+1 without tombstone check, fixed with forward scan
- ✅ **Symptoms caught**: NextPageID burning (4829 pages for 49 records), records vanishing (103-146 missing), wrong key deletions (deleted 101-109 instead of staying)

**Merge Algorithm Details:**
- Prefers merging with left sibling (right into left) for consistency
- Falls back to right sibling if no left sibling exists
- Skips merge if combined size > PAGE_SIZE (accepts fragmentation, no borrowing yet)
- **Leaf merge**: Calls Compact on both siblings first, updates NextLeaf chain (`merged.NextLeaf = orphaned.NextLeaf`), adds orphaned page to free list
- **Internal merge**: Calls Compact on both siblings first, demotes separator key into merged node (`SerializeInternalRecord(separatorKey, leftNode.RightmostChild)`), adds orphaned page to free list
- **Parent updates with tombstones**: After `DeleteRecord(separatorIndex)` creates tombstone, finds next non-tombstone record (separatorIndex+1, +2, etc.) to update child pointer

**Free Page List Implementation:**
- `allocatePage()`: Pops from FreePageIDs if available, otherwise increments NextPageID
- Merge operations append orphaned PageID to FreePageIDs
- Serialized in TableHeader, persists across restarts
- Protected by BTreeStore's RWMutex (no separate mutex needed)
- **Verified**: 501 inserts after merges consumed 0 new pages (all reused from free list)

**✅ PageCache Integration (COMPLETED):**
- ✅ PageCache implementation with FIFO eviction (500 pages)
- ✅ Pin/unpin semantics for in-use pages
- ✅ BTree integrated with PageCache (loadNode/writeNode/Close)
- ✅ Header management moved to PageCache
- ✅ All 8 tombstone bugs fixed (NumSlots vs len(Records) pattern)
- ✅ Compact-before-split pattern prevents nil record insertion
- ✅ Linear scan for leaf Search (tombstone-safe)
- ✅ All unit tests passing (PageCache, BTree, Page)
- ✅ Stress testing: Chaos test shows 1,449 records across 41 pages (NextPageID=41, not 4829)

**✅ VACUUM Bulk Loading (COMPLETED)**

**Implementation:**
- ✅ `buildLeafLayer()`: Scans old tree left-to-right via NextLeaf pointers, packs records into dense new leaves
- ✅ `buildInternalLayer()`: Constructs parent layer using first key of each child as separator, RightmostChild for last pointer
- ✅ `BulkLoad()`: Orchestrates bottom-up tree construction, writes to temp file, atomic rename, reloads cache
- ✅ Atomic file replacement: Writes to `.db.tmp`, closes old file, renames temp to original, reopens
- ✅ Cache invalidation: `PageCache.UpdateFile()` clears stale cache entries after file swap
- ✅ CLI integration: `commandVacuum` always reloads table cache to ensure fresh handle after rebuild
- ✅ Error handling: Defer cleanup pattern removes temp file if BulkLoad fails

**Performance:**
- 10x speed improvement over Insert-based rebuild (O(n) vs O(n log n))
- ~50% space savings (e.g., 192 pages → 97 pages for 10k records)
- No client reconnect required (fixed via cache reload in commandVacuum)

**Future Enhancements:**
- Node borrowing/redistribution (when merge impossible due to size)
- Async background flusher for dirty pages (reduce fsync overhead, batch writes)
- Support non-int primary keys via hashing (currently first field must be int32)
- Transaction support with ACID guarantees (WAL, ARIES recovery)
- Page-level checksums and compression

## Learning-Focused Interaction Guidelines

**CRITICAL: This is a learning project. The user's goal is deep understanding, not quick completion.**

### What Works Well (User's Preferred Approach)

**DO:**
- Explain concepts, approaches, and trade-offs
- Show small, focused examples (5-10 lines max) to illustrate a pattern
- Ask clarifying questions before suggesting solutions
- Point to specific files/functions to examine: "Look at how `writeValue` handles type switching"
- Suggest "try X, then come back if you hit issues"
- Validate the user's architectural thinking when they reason through trade-offs
- Use analogies to other languages/systems (e.g., "like HashMap vs BTreeMap in Rust")
- Let the user discover bugs through testing rather than preemptively fixing everything

**DON'T:**
- Write large blocks of complete code (>20 lines)
- Make multiple file changes in rapid succession
- Implement entire features without user request
- Fix every potential issue proactively
- Provide solutions before the user has attempted implementation

### Anti-Pattern to Avoid

The user has experienced unproductive loops where:
1. Claude writes large amounts of code
2. User copies it without fully understanding
3. Something breaks or needs adjustment
4. User doesn't understand the code well enough to debug it
5. Asks Claude for more code to fix it
6. **Learning collapses**

### Effective Pattern Instead

1. **Conceptual explanation**: "Here's how table caching works and why it's needed..."
2. **Sketch the approach**: "You'll need to: (a) add a global map, (b) check cache before opening, (c) update commandUse"
3. **Let user implement**: User writes the code
4. **Review and refine**: "Good! One thing to consider: what if two goroutines call this simultaneously?"
5. **User iterates**: User adds the mutex
6. **Validate**: "Perfect, that's exactly the pattern. Now test it with..."

### When to Provide Code

**Small examples are fine:**
- Helper functions (~10 lines)
- Interface implementations
- Fixing syntax/import issues

**Require explicit request for:**
- Feature implementations
- Refactoring existing code
- Architectural changes

### Interaction Style

- **Socratic method**: "What do you think happens if two clients INSERT at the same time?"
- **Incremental**: One concept/change at a time, not batched
- **Empirical**: "Try it and see what happens" > "Here's the fix"
- **Context-building**: Connect to books being read (Petrov, CLRS), prior experience (Rust)

### User's Strengths to Leverage

- Solid Go fundamentals (syntax, stdlib, idioms)
- Systems thinking and architectural reasoning
- Willingness to debug and experiment
- Cross-language pattern recognition
- Reading technical documentation (database papers, RFCs)

The user learns best through guided exploration, not guided implementation.

## Important Notes

- **Primary key constraint**: First field of schema must be int32 type (cast to uint64 for B+ tree keys). Future: support arbitrary types via hashing.
- **Free list race condition protection**: FreePageIDs has no explicit mutex, but it's safe because BTreeStore's RWMutex serializes all BTree access. Every operation that touches FreePageIDs (via pc.AllocatePage during Insert, pc.FreePage during merge in Delete) holds the write lock.
- **PageCache concurrency**: PageCache.mu protects cache map and clock queue. BTreeStore.mu provides outer serialization for all BTree operations. Pin/unpin must be called within the same BTree operation scope.
- **Cache size**: maxCacheSize = 200 pages. With 4KB pages, caches up to 800KB of data. Clock eviction prevents cache exhaustion during deep tree operations.
- **Eviction and durability**: Dirty pages flushed to disk during eviction via flushRecord(). Close() flushes all remaining dirty pages. Header flushed via explicit FlushHeader() calls.
- **Performance trade-off**: flushRecord() calls Sync() for durability during eviction. This ensures evicted pages persist, but adds latency. Future: batch writes, async flusher.
- **Free list vs VACUUM**: Free list prevents file growth by reusing pages, but doesn't reclaim disk space. VACUUM rebuilds tree with bulk loading (O(n) sequential scan), achieving ~50% space savings and 10x performance improvement over Insert-based rebuild.
- **Stress testing importance**: Catastrophic durability bug (100% data loss) and all 8 tombstone bugs only discovered through concurrent stress tests with restart verification and chaos testing.
- **Tombstone model critical pattern**: `NumSlots` = active record count (decrements on delete), `len(Records)` = array size with tombstones. ALWAYS use `len(Records)` for iteration and bounds checks. Using NumSlots causes silent data loss (records at high indices skipped).
- **Linear scan for leaf Search**: Binary search breaks with tombstones because `GetKey(mid)` returns 0 for nil records, making comparison logic incorrect. Linear scan is simple, correct, and fast enough for small leaf pages (~50-100 records).
- **Compact-before-split**: SplitLeaf/SplitInternal must call Compact() first to ensure NumSlots == len(Records), preventing nil record insertion errors.
- **VACUUM implementation**: Uses bulk loading (buildLeafLayer + buildInternalLayer) for O(n) rebuild. Writes to temp file, atomic rename ensures durability. commandVacuum always reloads table cache to prevent stale file handles.
- **TCP/REPL cache sharing**: Global tableCache shared across connections. TCP clients don't close tables on disconnect (Close() commented out in commandExit) to prevent breaking other sessions.
- Consider adding a JSON/HTTP endpoint for external integrations