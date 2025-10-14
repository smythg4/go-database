# go-database

A database implementation in Go with B+ tree storage, WAL, and ACID transactions. Built as a learning project following "Database Internals" by Alex Petrov (~6500 LOC).

## Features

- **B+ tree page-based storage** (4KB pages with slotted layout)
- **Multi-client TCP server** on port 42069
- **Basic CRUD operations** with dynamic schema support
- **Clock eviction page cache** with pin/unpin semantics (250 pages)
- **Free page reuse** after deletions (automatic recycling)
- **Range scans** via leaf sibling pointers
- **Fast bulk-loading VACUUM** (O(n) rebuild, ~50% space savings, 10x faster)
- **Write-ahead logging (WAL)** with channel-based single-writer goroutine
- **ACID transactions** with BEGIN/COMMIT/ABORT (auto-commit for single operations)
- **Background checkpointing** (30s intervals)
- **Context-based graceful shutdown** (signal handling, WaitGroup coordination)
- **CRC32 page-level checksums** (corruption detection)
- **Sequential insert optimization** (70/30 split ratio for monotonic keys)
- **Left/Right-sibling borrowing** (reduces page underflow fragmentation)

## Quick Start

```bash
# Build
go build -o godb cmd/main.go

# Run server (logs to stderr)
./godb 2>server.log

# Connect via TCP
nc localhost 42069
```

## Example Session

```sql
create users id:int name:string age:int
begin               -- start transaction
insert 1 alice 30
insert 2 bob 25
commit              -- commit transaction
select              -- full table scan
select 1            -- find by id
select 1 10         -- range scan (ids 1-10)
count
count 5 15          -- count range
update 1 alice 31   -- update (DELETE + INSERT)
begin
delete 2
abort               -- rollback transaction (delete not applied)
describe            -- show schema
stats               -- show tree structure (root page, depth, page count)
vacuum              -- rebuild tree (compaction)
.exit
```

## Supported Types

- `int` - 32-bit integers (primary key must be int)
- `string` - variable-length UTF-8
- `float` - 64-bit floats
- `bool` - boolean values
- `date` - YYYY-MM-DD format (stored as Unix timestamp)

## Commands

```
create <table> <field:type> ...   Create table (first field is primary key)
use <table>                       Switch to table
begin                             Start transaction
commit                            Commit transaction
abort                             Rollback transaction
insert <val1> <val2> ...          Insert record
select [id] [start end]           Query records
update <val1> <val2> ...          Update record (DELETE + INSERT pattern)
delete <id>                       Delete by primary key
count [id] [start end]            Count records
describe                          Show table schema
stats                             Show B+ tree statistics
vacuum                            Rebuild and compact tree
drop <table>                      Delete table file
show                              List all tables
.exit                             Close connection (triggers checkpoint)
```

## Architecture

```
cmd/main.go              - TCP server + REPL with graceful shutdown
internal/cli/            - Command parsing and transaction state
internal/store/          - BTreeStore wrapper with transaction support
internal/btree/          - B+ tree implementation
internal/pager/          - Page cache, disk I/O, slotted pages, WAL manager
internal/schema/         - Schema and serialization
```

**Storage:** B+ tree with slotted pages (4KB). Primary key (first field) must be `int` type. Records stored in leaf nodes, internal nodes store routing keys with child pointers.

**Caching:** 250-page buffer pool with Clock eviction algorithm. Pages pinned during operations, unpinned when done. Clock gives "second chance" to recently accessed pages before eviction.

**Concurrency:** `sync.RWMutex` on BTreeStore serializes tree operations. Single WAL writer goroutine handles all write requests via channels (request/response pattern eliminates cross-session contamination). Table cache ensures one BTreeStore instance per file.

**Transactions:** Auto-commit for single operations (INSERT/DELETE/UPDATE). Explicit transactions buffer operations and batch WAL writes on COMMIT. ABORT discards buffered operations. Each transaction sends all records in one WAL request for optimal performance (single fsync).

**Durability:** WAL syncs after every write batch. Page writes during eviction don't sync (rely on WAL). Checkpoint on graceful shutdown (or 30s intervals) flushes all dirty pages and truncates WAL. Header flushed after insert/delete operations.

**Checksums:** Every page has CRC32 checksum in trailer (bytes 4092-4095). Calculated over bytes 0-4091. Deserialization returns error on mismatch. Detects corruption from crashes, disk errors, or bugs.

## Testing

```bash
# Unit tests
go test ./...
go test -v ./internal/btree/

# Integration tests (require running server in background)
./test_wal_simple.sh    # Simple WAL recovery test (3 records, crash, verify)
./test_recovery.sh      # Comprehensive: transactions, crash, recovery, checkpoint
./test_crud.sh          # Full CRUD operations (create, insert, select, update, delete)
./test_chaos.sh         # Concurrent inserts/deletes (5 clients, tests free list)
./stress_test.sh        # 10k sequential inserts

# Benchmarks
go test -bench=. ./internal/store/
```

## Known Limitations

- Primary key must be `int` type (int32 cast to uint64)
- Single-level atomicity (no nested transactions or savepoints)
- No read isolation (BTreeStore.mu serializes all operations)
- No indexes beyond primary key
- UPDATE uses DELETE + INSERT pattern (not in-place)
- No query optimizer
- REPL doesn't respect context cancellation (prompt persists on Ctrl+C until Enter pressed)

## Implementation Notes

**Delete strategy:** Compact-on-delete. Every `DeleteRecord()` calls `Compact()` to remove gaps immediately. Simplifies merge logic at the cost of delete performance.

**Page format:** 13-byte header + slot array (grows down from byte 13) + records (grow up from byte 4091) + CRC32 trailer (bytes 4092-4095). Binary search within sorted pages.

**Free pages:** Merges add orphaned pages to free list. Allocator checks free list before creating new pages. Pages evicted from cache when freed (prevents stale pointer bugs).

**Breadcrumb pattern:** Tracks descent path for bottom-up split/merge propagation. Approach from Petrov's "Database Internals" book.

**VACUUM bulk loading:** Scans all records sequentially via leaf chain, packs into dense leaf pages, builds internal layers bottom-up. O(n) complexity vs O(n log n) for insert-based rebuild. Typically achieves ~50% space savings and 10x speed improvement.

**Sequential insert optimization:** Detects monotonic keys (`key > lastKey` in leaf), uses 70/30 split ratio instead of 50/50. Reduces future splits for monotonic workloads (auto-increment IDs, timestamps).

**Borrowing:** When node underfull but sibling too large to merge, borrow first record from left or right sibling. Requires ≥3 keys in sibling and would remain ≥50% full after lending.

## File Format

Tables stored as `.db` files:
- Page 0: Header (magic "GDBT", version, root page ID, schema, free list)
- Page 1+: Slotted pages (leaf or internal nodes, each with CRC32 checksum)
- 4KB pages with LittleEndian binary serialization

WAL stored as `.wal` files:
- LSN (8 bytes) + Action (1 byte) + record-specific fields
- Actions: INSERT, DELETE, UPDATE, CHECKPOINT, VACUUM
- LSN is byte offset (seekable)
- Truncated on checkpoint, replayed on recovery

## Development

```bash
go fmt ./...
go vet ./...
go test -v ./internal/btree/
go test -bench=. -benchmem ./internal/store/
```

## References

- "Database Internals" by Alex Petrov (primary resource)
- "Introduction to Algorithms" (CLRS) - B-tree fundamentals
- Slotted page layout (standard in PostgreSQL, SQLite)
- Clock eviction algorithm (second-chance page replacement)

## License

MIT - Educational project, use freely.
