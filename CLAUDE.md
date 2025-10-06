# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A handrolled database implementation in Go with multi-client TCP server support, custom schema system, and persistent binary storage. Built as a learning project following "Database Internals" by Alex Petrov, with the goal of eventually implementing B-tree page-based storage.

## Architecture

### Active Storage Layer (`internal/store`)

**table.go** - Primary storage implementation using append-only log with deduplication:
- `TableStore`: Schema-based table storage with binary serialization
- File format: Schema header (table name, field definitions) + variable-length records
- Supports int32, string, bool, and float64 types (IEEE 754 encoding via `math.Float64bits`)
- `sync.RWMutex` for concurrent access protection (Insert uses Lock, Find/ScanAll use RLock)
- Deduplication: `ScanAll()` returns latest record per ID, `Find(id)` scans entire file for most recent match
- Helper functions: `writeValue/readValue` for type-aware serialization, `writeString/readString` for length-prefixed strings

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
- Table cache: `GetOrOpenTable(filename)` ensures single TableStore instance per file (critical for mutex to work)
- Commands write to `io.Writer` parameter, return errors
- SQL-like: `create`, `use`, `show`, `insert`, `select [id]`
- Dynamic schema: INSERT/SELECT adapt to current table's field definitions
- Formatted output: Column-aligned tables with field names as headers

### B+ Tree Storage Engine (`internal/pager` + `internal/btree`)

**Page Layer (`internal/pager/page.go`)** - Slotted page implementation:
- `SlottedPage`: In-memory representation with 13-byte header, slot array, and records
  - Header: PageType (LEAF/INTERNAL), NumSlots, FreeSpacePtr, RightmostChild, NextLeaf
  - NextLeaf forms sibling chain for efficient range scans (B+ tree feature)
  - Slot array grows down from byte 13, records grow up from end
  - 4KB fixed page size (PAGE_SIZE = 4096)
- Key methods:
  - `InsertRecordSorted`: Binary search insertion maintaining key order
  - `Search`: Binary search for exact key match in leaf nodes
  - `SearchInternal`: Routes search to correct child page in internal nodes
  - `SplitLeaf/SplitInternal`: Node splitting with promoted key handling and child pointer updates
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
- Written on every Insert via defer to maintain durability

**Disk I/O (`internal/pager/disk_manager.go`)**:
- `ReadPage/WritePage`: Load/store raw Page at offset (pageID * PAGE_SIZE)
- `ReadSlottedPage/WriteSlottedPage`: High-level wrappers with serialization
- `ReadHeader/WriteHeader`: Table metadata at offset 0

**B-Tree Orchestration (`internal/btree/btree.go`)**:
- `BTree`: Manages tree structure via DiskManager and TableHeader
- `BNode`: Thin wrapper around SlottedPage for tree-specific operations
- Key algorithms:
  - `Insert`: Uses breadcrumb stack to track descent path, handles splits bottom-up
  - `propagateSplit`: Inserts promoted keys into parents, cascades splits up tree
  - `handleRootSplit`: Creates new internal root when root overflows (tree height growth)
  - `Search`: Descends tree using SearchInternal/Search, max depth check prevents infinite loops
  - `RangeScan`: Follows NextLeaf sibling chain for efficient range queries
- Breadcrumb stack pattern (from Petrov): Tracks PageID path during descent for split propagation
- Child pointer management: After inserting promoted key at position i, updates record[i+1] or RightmostChild

**Critical Implementation Details:**
- Split propagation updates child pointers: When `[key, leftPageID]` inserted at index i, the next record (i+1) must point to rightPageID, or RightmostChild if last
- Sibling chain maintenance: During SplitLeaf, `newPage.NextLeaf = oldPage.NextLeaf` then `oldPage.NextLeaf = newPageID`
- Header durability: `defer dm.WriteHeader()` at start of Insert ensures PageID allocation persists
- Search safety: Max depth 100 prevents infinite loops from corrupted page structures

**Test Coverage (`internal/btree/btree_test.go` + `internal/pager/page_test.go`)**:
- Insert without split (single leaf)
- Insert with root split (tree height 1 → 2)
- Search in single-level and multi-level trees
- Range scan across multiple leaves via sibling pointers
- Page serialization round-trips
- Split mechanics (leaf/internal with child pointer validation)

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

# Run specific test
go test -v -run TestLeafSplit

# Run page-related tests (requires multiple files)
go test -v -run TestLeafSplit page_test.go page.go disk_manager.go header.go
```

## Key Design Decisions

**Binary Encoding:**
- All integers: LittleEndian encoding via `encoding/binary`
- Strings: 4-byte length prefix + UTF-8 bytes
- Floats: `math.Float64bits` converts to uint64, then LittleEndian
- Bools: Single byte (1=true, 0=false)

**Concurrency Model:**
- Table cache in `cli` package ensures single `TableStore` per file
- `sync.RWMutex` on TableStore serializes file access
- **Known limitation**: Shared `os.File` handle with concurrent Seek operations can cause corruption under heavy concurrent writes (file descriptor not thread-safe). Planned fix: per-operation file handles or move to page-based B-tree storage.

**Command Processing:**
- Commands take `io.Writer` to support both stdout (REPL) and `net.Conn` (TCP)
- `bufio.Writer` wrapper on TCP connections requires explicit `Flush()` after each command
- Per-session `DatabaseConfig` allows clients to switch tables independently

**Schema System:**
- Dynamic field parsing: Commands query `config.TableS.Schema.Fields` at runtime
- Type parsing via switch statements in `schema.ParseValue`
- Schema stored in file header, read on open (no external metadata files)

## Important Implementation Details

- `INSERT` deduplication happens on read (ScanAll/Find), not write - allows last-write-wins semantics
- Schema field order is preserved during serialization (critical for binary format consistency)
- Float parsing in Go: `strconv.ParseFloat(s, 64)` for user input
- EOF handling: Break inner loop on first field EOF, then exit outer record-reading loop
- Table names in prompts: Read from `TableStore.Schema.TableName` (file header), not filename
- Server logs go to stderr (`log.SetOutput(os.Stderr)`) to avoid REPL interference

## Current Development Status

B-tree page-based storage is **in active development**:

**Completed:**
- ✅ Slotted page layout with serialization/deserialization
- ✅ Binary search for sorted key insertion and lookup
- ✅ Leaf node splits with promoted key handling
- ✅ Internal node splits with RightmostChild pointer management
- ✅ Page-level I/O with DiskManager
- ✅ Table header with PageID allocation tracking
- ✅ Comprehensive test suite for pages and splits

**Next Steps:**
- BTree struct to orchestrate recursive insertion with split propagation
- Root split handling (creates new root with 2 children)
- Search implementation with tree traversal
- Integration with existing CLI/schema system
- Node merging/rebalancing (optional optimization, can defer)
- Buffer pool for page caching
- Replace append-only TableStore with B-tree backend

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
