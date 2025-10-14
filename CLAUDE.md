# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A handrolled database implementation in Go (~6500 LOC) featuring a B+ tree storage engine, write-ahead logging, ACID transactions, and concurrent TCP server. Built as a learning project following "Database Internals" by Alex Petrov, with full page-based storage, caching, and transaction support.

**Key Stats:**
- 4KB fixed page size with slotted layout
- 250-page buffer pool with Clock eviction
- CRC32 page-level checksums
- Multi-client TCP server (port 42069)
- Channel-based WAL writer (single goroutine, no contention)
- Context-based graceful shutdown

## Development Commands

```bash
# Build and run
go build -o godb cmd/main.go
./godb 2>server.log        # logs to stderr

# Connect via TCP
nc localhost 42069

# Testing
go test ./...               # all tests
go test -v ./internal/btree/
go test -v -run TestInsertWithRootSplit ./internal/btree/

# Benchmarks
go test -bench=. -benchmem ./internal/store/

# Integration tests (require running server in background)
./test_wal_simple.sh       # simple WAL recovery (3 records, crash, verify)
./test_recovery.sh         # comprehensive: transactions, crash, recovery, checkpoint
./test_crud.sh             # full CRUD operations
./test_chaos.sh            # concurrent inserts + deletes (5 clients)
./test_reuse.sh            # free page reuse verification
./stress_test.sh           # 10k sequential inserts

# Format and vet
go fmt ./...
go vet ./...
```

## Architecture Overview

```
cmd/main.go              - TCP server + REPL with graceful shutdown
internal/cli/            - Command parsing, transaction state, table cache
internal/store/          - BTreeStore wrapper (transactions, WAL integration)
internal/btree/          - B+ tree operations (insert, delete, search, merge, split, borrow)
internal/pager/          - Page cache, disk I/O, slotted pages, WAL manager, checksums
internal/schema/         - Schema definition and binary serialization
internal/encoding/       - LittleEndian helpers for binary I/O
```

## CLI Commands

All commands are case-insensitive. First field of schema is always primary key (must be `int` type).

**Table Management:**
- `create <table> <field:type> ...` - Create table (first field is primary key)
- `use <table>` - Switch active table
- `show` - List all tables (.db files)
- `describe` - Show schema for active table
- `drop <table>` - Delete table file
- `stats` - Show B+ tree statistics (root page, depth, page count)

**Data Operations:**
- `insert <val1> <val2> ...` - Insert record (auto-commit or buffered if in transaction)
- `select` - Full table scan
- `select <id>` - Find by primary key
- `select <start> <end>` - Range scan
- `update <val1> <val2> ...` - Update record (DELETE + INSERT pattern)
- `delete <id>` - Delete by primary key
- `count` - Count all records
- `count <id>` - Count single record
- `count <start> <end>` - Count range

**Transaction Commands:**
- `begin` - Start transaction (buffers INSERT/DELETE/UPDATE)
- `commit` - Batch log to WAL, then apply operations to tree
- `abort` - Discard buffered operations

**Maintenance:**
- `vacuum` - Rebuild tree with bulk loading (O(n), ~50% space savings, 10x faster than insert-based rebuild)
- `recover` - Manually replay WAL (normally automatic on startup)

**System:**
- `.help` - Show all commands
- `.exit` - Checkpoint and close (TCP clients just disconnect)

**Supported Types:** `int` (int32), `string`, `float` (float64), `bool`, `date` (YYYY-MM-DD)

## Core Components

### Page Layer (`internal/pager/page.go`)

**SlottedPage** - 4KB pages with 13-byte header:
- Header: `[PageType:1][NumSlots:2][FreeSpacePtr:2][RightmostChild:4][NextLeaf:4]`
- Slot array grows down from byte 13, records grow up from byte 4091
- Trailer: CRC32 checksum at bytes 4092-4096
- Page types: LEAF (0) or INTERNAL (1)
- `NextLeaf` forms sibling chain for range scans

**Key Methods:**
- `InsertRecordSorted(data)` - Binary search insertion, maintains key order
- `DeleteRecord(index)` - Tombstone + immediate compact (compact-on-delete strategy)
- `Search(key)` - Binary search for exact key match
- `SearchInternal(key)` - Routes to child page in internal nodes
- `SplitLeaf/SplitInternal(newPageID, sequential)` - Node splitting (70/30 split if sequential detected)
- `MergeLeaf/MergeInternals(sibling)` - Combine underfull siblings
- `CanMergeWith(sibling)` - Check if combined size ≤ PAGE_SIZE
- `CanLendKeys()` - True if NumSlots ≥ 3 and would remain ≥50% full after lending
- `IsUnderfull()` - True if used space < PAGE_SIZE/2
- `Compact()` - Remove tombstone gaps, repack active records
- `Serialize/Deserialize()` - Convert between memory and disk with CRC32 validation

**Record Formats:**
- Leaf: `[key:8][full record data:variable]`
- Internal: `[key:8][child PageID:4]`

**CRC32 Checksums:** Calculated over bytes 0-4091, stored at 4092-4095. Deserialize returns error on mismatch.

**Compact-on-Delete Strategy:** Every `DeleteRecord()` calls `Compact()` immediately (page.go:219). Simplifies merge logic at cost of delete performance. `NumSlots` = active record count (decrements on delete), `len(Records)` = array size. Always use `len(Records)` for iteration to include all records.

### Page Cache (`internal/pager/page_cache.go`)

**PageCache** - Buffer pool with Clock eviction (250 pages = 1MB):
- `CacheRecord`: Tracks page data, dirty flag, pin count, and refBit (second-chance bit)
- Clock algorithm: refBit gives second chance before eviction, skips pinned pages
- Pin/unpin prevents eviction during tree operations

**Key Methods:**
- `Fetch(id)` - Load from cache or disk, auto-pins, sets refBit
- `AddNewPage(sp)` - Add to cache, mark dirty, pin
- `UnPin(id)` - Decrement pin count (allows eviction when reaches 0)
- `Evict()` - Clock sweep, flushes dirty pages before eviction (no sync during eviction now that we have WAL)
- `MakeDirty(id)` - Mark page dirty (flush on eviction or Close)
- `AllocatePage()` - Pop from FreePageIDs if available, else increment NextPageID
- `FreePage(id)` - Evict from cache, add to free list
- `FlushAll()` - Write all dirty pages, flush header, sync file
- `FlushHeader()` - Write table header (auto-calculates NumPages)
- `UpdateFile(file)` - Switch to new file after VACUUM rename, clear cache
- `ReplaceTreeFromPages(pages, rootID)` - Atomic file replacement for VACUUM
- `Close()` - FlushAll then close file

**Critical Pattern:** BTree operations pin pages on load (`defer bt.pc.UnPin(node.PageID)`), new pages from splits pinned via `AddNewPage`.

### Table Metadata (`internal/pager/header.go`)

**TableHeader** - Stored at page 0 (padded to 4KB):
- Magic: `"GDBT"`
- Version: `uint16`
- RootPageID: `PageID` (initially 1)
- NextPageID: `PageID` (next available page for allocation)
- NumPages: `uint32` (auto-calculated on flush)
- Schema: Full schema with field definitions
- FreePageIDs: `[]PageID` (orphaned pages from merges, reused before allocating new)

### Disk I/O (`internal/pager/disk_manager.go`)

Simple wrapper around `*os.File` with page-sized I/O:
- `ReadPage/WritePage` - Raw [4096]byte at offset (pageID * PAGE_SIZE)
- `ReadSlottedPage/WriteSlottedPage` - Deserialize/serialize with CRC32 validation
- `ReadHeader/WriteHeader` - Table metadata at offset 0, WriteHeader calls Sync()
- `Sync()` - Flush OS buffers to disk

### B+ Tree Operations (`internal/btree/btree.go`)

**BTree** - Orchestrates tree structure via PageCache:
- `BNode`: Thin wrapper around SlottedPage
- All operations use pin/unpin discipline

**Insert Flow:**
1. `findLeaf(key, breadcrumbs)` - Traverse to leaf, record descent path
2. Check for duplicate key
3. Try `InsertRecordSorted(data)` - may return "page full"
4. On full: `allocatePage()`, `splitNode()`, write both halves
5. Retry insert into appropriate half
6. `propogateSplit()` - Walk breadcrumbs, insert promoted keys into parents, cascade splits up
7. `handleRootSplit()` - Create new internal root if needed (tree height growth)
8. `defer bt.pc.FlushHeader()` - Sync header (RootPageID, NextPageID, FreePageIDs) to disk

**Sequential Insert Detection:** If `key > lastKey` in leaf, use 70/30 split ratio instead of 50/50.

**Delete Flow:**
1. `findLeaf(key, breadcrumbs)` - Traverse to leaf, record descent path
2. `Search(key)` - Find record index
3. `DeleteRecord(idx)` - Tombstone + immediate compact
4. `writeNode(leaf)` - **Critical: write BEFORE checking underflow**
5. `handleUnderflow(leafPageID, breadcrumbs)` - If underfull, attempt borrow or merge
6. `defer bt.pc.FlushHeader()` - Sync header to disk

**Underflow Handling:**
1. Check if root (special case: collapse if internal root has 0 slots)
2. Pop breadcrumb, load parent and underfull node
3. Find left sibling first, else right sibling
4. If right sibling exists and `CanLendKeys()`: `borrowFromRightLeaf/Internal()`
5. Else if `CanMergeWith()`: `mergeLeafNodes/mergeInternalNodes()`
6. **Critical: writeNode(parent) BEFORE recursive underflow check** (prevents stale pointers)
7. Recurse if parent now underfull

**Borrowing (Right Sibling Only):**
- **Leaf:** Move first record from right to left, update parent separator to right's new first key
- **Internal:** Demote parent separator to left (pointing to left's RightmostChild), move first record from right to left (becomes left's new RightmostChild), promote right's new first key to parent
- Requires ≥3 keys in sibling, would remain ≥50% full after lending
- **Not Implemented:** Left sibling borrowing

**Merging:**
- Always merge right into left (prefers left sibling, falls back to right if no left)
- **Leaf:** Copy all records, update NextLeaf chain (`merged.NextLeaf = orphaned.NextLeaf`)
- **Internal:** Demote parent separator into merged node, copy all records, update RightmostChild
- Remove separator from parent, update parent's child pointer
- Add orphaned page to FreePageIDs, evict from cache

**Search:** Max depth 100, descends tree via `SearchInternal()` in internal nodes, `Search()` in leaf.

**RangeScan:** Find leaf containing startKey, follow NextLeaf chain, cycle detection (returns error if page visited twice).

**VACUUM (Bulk Loading):**
1. `buildLeafLayer()` - Scan all leaves left-to-right via NextLeaf, pack into dense new leaves
2. `buildInternalLayer()` - Build parent layer using first key of each child as separator, RightmostChild for last pointer
3. Recurse until single root remains
4. `ReplaceTreeFromPages()` - Write to `.db.tmp`, close old, rename, reopen
5. **Critical:** Table cache reloaded after VACUUM (commands.go:223-233)

**O(n) Complexity:** Sequential scan + pack, vs O(n log n) for insert-based rebuild. ~50% space savings, 10x speed improvement.

### BTreeStore (`internal/store/btree_store.go`)

Wrapper providing transaction support and WAL integration:

**Auto-Commit Mode** (no BEGIN):
- `Insert(record)` → LogInsert() → bt.Insert() (lines 124-149)
- `Delete(key)` → LogDelete() → bt.Delete() (lines 151-158)

**Explicit Transaction Mode** (after BEGIN):
- `PrepareInsert(record)` → Create WALRecord WITHOUT logging (lines 304-321)
- `PrepareDelete(key)` → Create WALRecord WITHOUT logging (lines 323-329)
- Operations buffered in `config.txnBuffer` (per-session)
- `Commit(txnBuffer)` → Batch send to WAL writer → Apply to tree (lines 267-302)

**Background Checkpointer:**
- 30-second ticker in `startCheckpointer()` goroutine (lines 103-122)
- On tick or context cancellation: `Checkpoint()` → FlushAll pages → LogCheckpoint → Truncate WAL
- Uses BTreeStore.mu (serializes with commits, adds latency)

**Graceful Shutdown:**
- Context passed to constructor, monitored by checkpointer and WAL writer
- WaitGroup tracks active goroutines
- Signal handler (main.go:100-107) cancels context, waits for WaitGroup, calls `.exit`

### WAL Manager (`internal/pager/wal_manager.go`)

**Channel-Based Writer** (single goroutine, lines 59-79):
- `RequestChan`: Receives `WALRequest{Records, Done}`
- Writer goroutine: read request → writeRecords() → Sync() → respond on Done
- Eliminates cross-session contamination (each request batched separately)

**WALRecord Format:**
- LSN: `uint64` (byte offset in WAL file, seekable)
- Action: `uint8` (INSERT=0, DELETE=1, UPDATE=2, VACUUM=3, CHECKPOINT=4)
- Action-specific fields (Key, RecordBytes, RootPageID, NextPageID)

**Key Methods:**
- `NewWalManager(filename, ctx, wg)` - Start writer goroutine
- `LogInsert/LogDelete/LogUpdate` - Send single-record request, block on response
- `LogCheckpoint/LogVacuum` - Send metadata record
- `ReadAll()` - Deserialize all records (for recovery)
- `Truncate()` - Clear WAL (after checkpoint)
- `writeRecords(records)` - Assign LSNs, serialize, write, Sync()

**Recovery:** `BTreeStore.Recover()` calls `wal.ReadAll()`, replays INSERT/DELETE operations.

### Schema System (`internal/schema/schema.go`)

**Schema:**
- `TableName`: string
- `Fields`: `[]Field{Name, Type}`

**FieldType:** IntType (int32), StringType, BoolType, FloatType (float64), DateType (Unix timestamp, displayed as YYYY-MM-DD)

**Record:** `map[string]any`

**Binary Encoding:**
- Integers: LittleEndian uint32
- Strings: 4-byte length prefix + UTF-8 bytes
- Floats: `math.Float64bits` → uint64 → LittleEndian
- Bools: Single byte (1=true, 0=false)
- Dates: Unix timestamp as int64

**Key Methods:**
- `ParseFieldType(s)` - String to FieldType
- `ParseValue(s, fieldType)` - User input to typed value
- `SerializeRecord(record)` - Binary format: `[key:8][all fields...]`
- `DeserializeRecord(data)` - Parse binary → Record
- `ExtractPrimaryKey(record)` - Get first field as uint64 (must be int32)

**Primary Key Constraint:** First field must be `int` type (int32), cast to uint64 for tree operations.

### CLI Layer (`internal/cli/commands.go`)

**DatabaseConfig:**
- `TableS`: `*BTreeStore` (active table)
- `inTransaction`: `bool`
- `txnBuffer`: `[]pager.WALRecord`
- `ctx`, `wg`: For graceful shutdown

**Table Cache:**
- `GetOrOpenTable(filename, ctx, wg)` - Ensures single BTreeStore instance per file (mutex-protected map)
- Critical for BTreeStore.mu to work (all clients share same instance)

**Command Implementation:**
- All commands write to `io.Writer` (stdout or net.Conn)
- Dynamic schema: Commands query `config.TableS.Schema().Fields` at runtime
- Formatted output: Column-aligned tables with field names as headers
- Transaction-aware: INSERT/DELETE check `config.inTransaction` flag

**Key Patterns:**
- VACUUM: Reloads table cache after rebuild (commands.go:223-233)
- UPDATE: Naive DELETE + INSERT
- .exit: TCP clients just close connection, local client checkpoints all tables

### TCP Server (`cmd/main.go`)

**Server Setup:**
- Listens on port 42069
- `handleTCPConnection()` per client
- Each connection gets isolated `DatabaseConfig` via `Clone()` (shares TableS references)

**Graceful Shutdown:**
- Context + cancel (line 84)
- Signal handler (lines 88-107): Ctrl+C → cancel context → wg.Wait() → .exit → os.Exit(0)
- TCP server goroutine monitors context, closes listener on cancellation
- WAL writer goroutine drains RequestChan on context cancellation

**REPL Limitation:**
- `bufio.Scanner.Scan()` blocks on stdin without checking context (line 39)
- On Ctrl+C: checkpointers exit cleanly, prompt remains until Enter pressed
- Comment at line 35-36 acknowledges this

## Key Design Decisions

**Concurrency Model:**
- BTreeStore.mu (RWMutex): Serializes tree operations (Insert/Delete use Lock, Find/ScanAll use RLock)
- PageCache.mu: Protects cache map and clock queue
- Table cache mutex: Protects global tableCache map
- WAL writer: Single goroutine, channel-based requests (no contention)

**Durability Strategy:**
- WAL syncs after every flush (wal_manager.go:101)
- Page writes during eviction don't sync (rely on WAL)
- Header syncs after WriteHeader (disk_manager.go:59)
- Checkpoint: FlushAll → Sync → Truncate WAL

**Transaction Isolation:**
- No read isolation (BTreeStore.mu serializes everything)
- Uncommitted writes not visible to other sessions (buffered in session-level txnBuffer)
- COMMIT sends batch request to WAL writer (single fsync for entire transaction)

**Delete Strategy:**
- Compact-on-delete (immediate Compact after tombstone creation)
- Simplifies merge logic (NumSlots always = len(Records) after compact)
- Trade-off: Delete performance for simpler correctness

**Free Page Management:**
- Orphaned pages from merges added to FreePageIDs (btree.go:393, 430)
- AllocatePage pops from free list before incrementing NextPageID (page_cache.go:54-64)
- Pages evicted from cache when freed (prevents stale pointer bugs)

**Sequential Insert Optimization:**
- Detected when `key > lastKey` in leaf (btree.go:193)
- 70/30 split ratio instead of 50/50 (reduces future splits for monotonic workloads)

## Current State (October 2025)

**Production-Ready Features:**
- ✅ B+ tree with inserts, deletes, range scans
- ✅ Page cache with Clock eviction (250 pages)
- ✅ CRC32 checksums (page corruption detection)
- ✅ Merge operations (leaf + internal)
- ✅ Borrowing from right sibling (leaf + internal)
- ✅ Free page reuse
- ✅ VACUUM bulk loading (O(n) rebuild, ~50% space savings)
- ✅ WAL with channel-based writer (no cross-session contamination)
- ✅ Auto-commit and explicit transactions (BEGIN/COMMIT/ABORT)
- ✅ Recovery (replay WAL on startup)
- ✅ Background checkpointing (30s intervals)
- ✅ Graceful shutdown (context + WaitGroup + signal handling)
- ✅ Multi-client TCP server
- ✅ Per-session transaction state
- ✅ 5 data types (int, string, float, bool, date)

**Known Limitations:**
- Primary key must be `int` type (int32 cast to uint64)
- No left-sibling borrowing (only right-sibling implemented)
- No read isolation (BTreeStore.mu serializes all operations)
- Checkpoint can fire during active transactions (adds latency but safe due to mutex)
- REPL doesn't respect context cancellation (stdin blocks until Enter pressed)
- UPDATE uses DELETE + INSERT pattern (not in-place)
- No indexes beyond primary key
- No query optimizer
- No nested transactions or savepoints

**Test Results:**
- 10k sequential inserts: ✅ Works
- 5k concurrent inserts (5 clients): ✅ Works
- Concurrent inserts + deletes (chaos test): ✅ Works
- Free page reuse verified: ✅ Works
- WAL recovery after simulated crash: ✅ Works
- Transaction COMMIT/ABORT: ✅ Works
- Background checkpoint (30s interval): ✅ Works
- All test scripts work on macOS: ✅ Fixed (netcat compatibility, server TTY detection)

## Important Implementation Notes

- **Pin/unpin discipline:** Every `loadNode()` must have `defer bt.pc.UnPin(node.PageID)`. New pages from splits pinned via `AddNewPage`.
- **Write-before-underflow pattern:** Modified nodes must be written to disk BEFORE checking for underflow (btree.go:608, 571).
- **Header durability:** BTree modifies header via PageCache methods, `FlushHeader()` syncs to disk. Implemented as defer at start of Insert/Delete.
- **Compact-on-delete simplification:** `NumSlots` always equals `len(Records)` after Delete (no tombstone tracking needed in merge logic).
- **WAL recovery EOF handling:** Must use `errors.Is(err, io.EOF)` not `err == io.EOF` because `encoding.ReadInt64()` wraps EOF as `fmt.Errorf("failed to read int64: %w", err)`. Direct comparison fails, causing recovery to incorrectly report empty WAL despite data on disk (wal_manager.go:126, btree_store.go:242).
- **Table cache reload after VACUUM:** VACUUM replaces file atomically, must reload from cache to get fresh file handle.
- **TCP cache sharing:** Global tableCache shared across connections. Clients don't close tables on disconnect (prevents breaking other sessions).
- **Schema field order:** Preserved during serialization (critical for binary format consistency).
- **Server logs:** Go to stderr (`log.SetOutput(os.Stderr)`) to avoid REPL interference.
- **Date storage:** Unix timestamp (int64) for compact storage, formatted as YYYY-MM-DD on read.

## Recent Development (October 2025)

**Session: WAL Recovery & Test Infrastructure**

Fixed critical WAL recovery bug and established comprehensive test suite:

**Bug Fixed:**
- **WAL recovery returning empty despite data on disk** - Root cause: `encoding.ReadInt64()` wraps `io.EOF` errors as `fmt.Errorf("failed to read int64: %w", err)`, causing direct comparison `err == io.EOF` to fail. Solution: Use `errors.Is(err, io.EOF)` to properly unwrap and check underlying error. Files affected: `wal_manager.go:126`, `btree_store.go:242`.

**Test Infrastructure:**
- Fixed macOS netcat compatibility issues (BSD netcat doesn't support `-N` flag, now uses `.exit` pattern)
- Fixed server infinite loop when backgrounded (used `term.IsTerminal()` instead of `os.ModeCharDevice` check)
- Created comprehensive test suite: `test_wal_simple.sh`, `test_recovery.sh`, `test_crud.sh`
- All integration tests now work reliably on macOS

**Verification:**
- Manual test: 3 records inserted → server killed with `kill -9` → recovery replayed 3 records ✅
- Log output: "WAL recovery: Found 3 records to replay" ✅
- All records successfully recovered (alice, bob, charlie) ✅

## Future Enhancements

**Performance:**
- Async page flusher (background goroutine, batch writes, reduce fsync overhead)
- Group commit optimization (batch multiple transactions, single fsync)
- Parallel B+ tree with latch crabbing (reduce mutex contention)

**Correctness:**
- Left-sibling borrowing (currently only right-sibling)
- Active transaction tracking (skip checkpoints when txns in progress)
- Fix REPL context cancellation (non-blocking stdin or channel-based approach)

**Features:**
- MVCC for snapshot isolation (multi-version concurrency control)
- Secondary indexes (B+ tree per index, point back to primary key)
- Support non-int primary keys (hash to uint64)
- Query optimizer (cost-based, statistics on key distribution)
- Savepoints and nested transactions
- Compression (LZ4/Snappy for page-level compression)

**Robustness:**
- Redo-only WAL with physiological logging (currently simple redo)
- Log-structured merge tree for write-heavy workloads
- Online backup and point-in-time recovery

## Learning-Focused Interaction Guidelines

**CRITICAL: This is a learning project. The user's goal is deep understanding, not quick completion.**

### Scott Young's "Get Better at Anything" Principles

The user follows principles from Scott Young's learning framework. Apply these when helping:

**1. See (Observe the skill)**
- Show concrete examples from working code
- Point to specific implementations: "Look at how `PageCache.Evict()` handles the clock hand"
- Use analogies to connect to known concepts (Rust, other systems)
- Explain "why" decisions were made, not just "what" was implemented

**2. Do (Practice with feedback)**
- Let user implement first, then provide feedback
- Encourage experimentation: "Try it and see what breaks"
- Guide discovery of bugs through testing rather than preemptive fixes
- Validate attempts even if imperfect: "Good start! Now consider edge case X..."

**3. Feedback (Immediate, specific correction)**
- When code has issues, explain WHY it's wrong and WHAT breaks
- Show consequences: "This causes data loss because..."
- Offer specific fixes, not vague suggestions
- Connect errors to concepts: "This is the defer-in-loop bug we saw earlier"

**4. Retention (Spaced practice, retrieval)**
- Reference previous implementations: "Like you did with page splits..."
- Ask recall questions: "How did you handle this in PageCache?"
- Build on foundations iteratively (don't rebuild from scratch)
- Create connections between concepts (WAL durability ← fsync ← page writes)

**Learning Velocity Observations:**
- User built B+ tree + WAL + transactions in ~3 weeks
- High learning velocity when in flow state
- Prefers focused 2-4 hour sessions over extended marathons
- Book → project → book cycle works well (Petrov → database → Bodner Context chapter)
- Stress testing reveals deep bugs (tombstone bugs, durability issues) - always recommend testing

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

## References

- "Database Internals" by Alex Petrov (primary resource, breadcrumb pattern, B+ tree algorithms)
- "Introduction to Algorithms" (CLRS) - B-tree fundamentals
- Slotted page layout (standard in PostgreSQL, SQLite, most RDBMS)
- Clock eviction algorithm (second-chance page replacement)
