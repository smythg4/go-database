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
- SQL-like: `create`, `use`, `show`, `insert`, `select [id]`, `delete <id>`, `update <values>`, `stats`
- Dynamic schema: INSERT/SELECT/UPDATE adapt to current table's field definitions
- Formatted output: Column-aligned tables with field names as headers
- `delete <id>`: Finds record by key, displays it, then calls Delete (with merge support)
- `update <values>`: Naive implementation using DELETE + INSERT (shows record being updated)
- `stats` command: Shows tree structure (root page ID, type, NextPageID allocation)

### B+ Tree Storage Engine (`internal/pager` + `internal/btree`)

**Page Layer (`internal/pager/page.go`)** - Slotted page implementation:
- `SlottedPage`: In-memory representation with 13-byte header, slot array, and records
  - Header: PageType (LEAF/INTERNAL), NumSlots, FreeSpacePtr, RightmostChild, NextLeaf
  - NextLeaf forms sibling chain for efficient range scans (B+ tree feature)
  - Slot array grows down from byte 13, records grow up from end
  - 4KB fixed page size (PAGE_SIZE = 4096)
- Key methods:
  - `InsertRecordSorted`: Binary search insertion maintaining key order
  - `DeleteRecord`: Tombstone approach (mark Offset=0) then immediate Compact
  - `Search`: Binary search for exact key match in leaf nodes
  - `SearchInternal`: Routes search to correct child page in internal nodes
  - `SplitLeaf/SplitInternal`: Node splitting with promoted key handling and child pointer updates
  - `MergeLeaf/MergeInternals`: Combine two nodes (updates NextLeaf chain for leaves)
  - `CanMergeWith`: Check if two pages fit together (combined size + slot overhead ≤ PAGE_SIZE)
  - `IsUnderfull`: Returns true if GetUsedSpace() < PAGE_SIZE/2
  - `Compact`: Removes deleted record gaps by repacking active records
  - `Serialize/Deserialize`: Convert between in-memory (13-byte header) and disk ([4096]byte array)
- Record formats:
  - Leaf: [key: 8 bytes][full record data: variable]
  - Internal: [key: 8 bytes][child PageID: 4 bytes]
- First field in schema is always the key

**Table Metadata (`internal/pager/header.go`)**:
- `TableHeader`: Magic ("GDBT"), version, RootPageID, NextPageID, NumPages, Schema
- Page 0 reserved for header (padded to 4KB), B-tree nodes start at page 1
- NextPageID tracks next available page for allocation during splits
- `DefaultTableHeader(schema)`: Helper for creating initial headers with schema
- **Critical durability pattern**: BTree modifies its own header copy (RootPageID/NextPageID), must sync back to DiskManager before WriteHeader

**Disk I/O (`internal/pager/disk_manager.go`)**:
- `ReadPage/WritePage`: Load/store raw Page at offset (pageID * PAGE_SIZE)
- `ReadSlottedPage/WriteSlottedPage`: High-level wrappers with serialization
- `ReadHeader/WriteHeader`: Table metadata at offset 0 (WriteHeader calls Sync())
- `Sync()`: Flushes OS buffers to disk (critical for durability)

**B-Tree Orchestration (`internal/btree/btree.go`)**:
- `BTree`: Manages tree structure via DiskManager and TableHeader
- `BNode`: Thin wrapper around SlottedPage for tree-specific operations
- `NewBTree(dm, header)`: Constructor for creating BTree with unexported fields
- Key algorithms:
  - `Insert`: Uses breadcrumb stack to track descent path, handles splits bottom-up
    - **Critical**: Defer pattern syncs header and calls Sync(): `defer func() { bt.dm.SetHeader(*bt.Header); bt.dm.WriteHeader(); bt.dm.Sync() }()`
  - `Delete`: Traverses to leaf, deletes record, writes leaf BEFORE checking underflow, handles merges
    - **Critical**: Same defer pattern as Insert for header sync
    - Writes modified leaf before underflow check to ensure durability
  - `propagateSplit`: Inserts promoted keys into parents, cascades splits up tree
  - `handleRootSplit`: Creates new internal root when root overflows (tree height growth)
  - `handleUnderflow`: Detects underflow, finds sibling, attempts merge, recurses on parent underflow
    - **Critical**: Writes parent BEFORE checking parent underflow (prevents stale parent pointers)
  - `handleRootUnderflow`: Collapses root when internal root has NumSlots=0 (only RightmostChild remains)
  - `mergeLeafNodes/mergeInternalNodes`: Combines siblings, updates parent pointers
    - Internal merge demotes separator key: `SerializeInternalRecord(separatorKey, leftNode.RightmostChild)`
  - `findLeftSibling/findRightSibling`: Navigates parent to locate merge partners
  - `Search`: Descends tree using SearchInternal/Search, max depth check prevents infinite loops
  - `RangeScan`: Follows NextLeaf sibling chain with cycle detection
  - `Stats()`: Debug helper showing root page ID, type, NextPageID allocation
- Breadcrumb stack pattern (from Petrov): Tracks PageID and child index during descent for split/merge propagation
- Child pointer management: After inserting promoted key at position i, updates record[i+1] or RightmostChild

**Critical Implementation Details:**
- Split propagation updates child pointers: When `[key, leftPageID]` inserted at index i, the next record (i+1) must point to rightPageID, or RightmostChild if last
- Sibling chain maintenance: During SplitLeaf, `newPage.NextLeaf = oldPage.NextLeaf` then `oldPage.NextLeaf = newPageID`
- **Header durability bug fix**: BTree modifies its own header copy (RootPageID, NextPageID), but DiskManager holds original. Must call `bt.dm.SetHeader(*bt.Header)` before `bt.dm.WriteHeader()` to sync changes. Implemented as defer at start of Insert.
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
- ✅ Durability: `Sync()` after all DELETE operations
- ✅ CLI: `delete <id>` and `update <values>` commands

**Critical Bugs Fixed:**
- ✅ **Parent write before recursion**: Parent updates must be persisted BEFORE recursive underflow handling, otherwise stale parent pointers point to orphaned pages
- ✅ **Leaf write before underflow check**: Modified leaf must be written to disk BEFORE checking underflow (same pattern as parent fix)
- ✅ **Cycle detection in RangeScan**: Detects corrupted leaf chains that visit pages twice
- ✅ **Missing fsync**: Added `DiskManager.Sync()` and called after Insert/Delete to flush OS buffers

**Merge Algorithm Details:**
- Prefers merging with left sibling (right into left) for consistency
- Falls back to right sibling if no left sibling exists
- Skips merge if combined size > PAGE_SIZE (accepts fragmentation in Phase 1)
- **Leaf merge**: Updates NextLeaf chain (`merged.NextLeaf = orphaned.NextLeaf`)
- **Internal merge**: Demotes separator key into merged node (`SerializeInternalRecord(separatorKey, leftNode.RightmostChild)`)
- **Parent updates**: After `DeleteRecord(separatorIndex)`, slots shift - must update pointer at new `separatorIndex` position

**Phase 2 - Future Enhancements:**
- Node borrowing/redistribution (when merge impossible due to size)
- Free space tracking for space reuse (orphaned pages currently not reclaimed)
- Buffer pool for page caching (reduce disk I/O)
- Support non-int primary keys via hashing (currently first field must be int32)
- Transaction support with ACID guarantees (WAL, ARIES recovery)
- Background compaction (VACUUM-like operation to reclaim orphaned pages)

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
- note that for now the primary key (first field of schema) must be an int, but maybe we can hash it for a key value in the future that's a uint64