# go-database

A database implementation in Go with B+ tree storage. Built as a learning project following "Database Internals" by Alex Petrov.

## Features

- B+ tree page-based storage (4KB pages)
- Multi-client TCP server on port 42069
- Basic CRUD operations with schema support
- Clock eviction page cache with pin/unpin semantics
- Free page reuse after deletions
- Range scans via leaf sibling pointers
- Fast bulk-loading VACUUM (O(n) rebuild and compact)
- **Write-ahead logging (WAL)** with channel-based writer
- **ACID transactions** with BEGIN/COMMIT/ABORT
- Auto-commit mode for single operations
- Background checkpointing (30s intervals) with graceful shutdown
- Context-based cancellation and coordinated cleanup

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
begin
delete 2
abort               -- rollback transaction (delete not applied)
stats               -- show tree structure
vacuum              -- rebuild tree
recover             -- replay WAL (for crash recovery)
.exit
```

## Supported Types

- `int` - 32-bit integers
- `string` - variable-length UTF-8
- `float` - 64-bit floats
- `bool` - boolean values
- `date` - YYYY-MM-DD format

## Commands

```
create <table> <field:type> ...   Create table (first field is primary key)
use <table>                       Switch to table
begin                             Start transaction
commit                            Commit transaction
abort                             Rollback transaction
insert <val1> <val2> ...          Insert record
select [id] [start end]           Query records
update <val1> <val2> ...          Update record (delete + insert)
delete <id>                       Delete by primary key
count [id] [start end]            Count records
describe                          Show table schema
stats                             Show B+ tree statistics
vacuum                            Rebuild and compact tree
recover                           Replay WAL (manual recovery)
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

**Storage:** B+ tree with slotted pages. Primary key (first field) must be `int` type. Records stored in leaf nodes, internal nodes store routing keys.

**Caching:** 200-page buffer pool with Clock eviction algorithm. Pages pinned during operations, unpinned when done. Clock gives "second chance" to recently accessed pages before eviction.

**Concurrency:** `sync.RWMutex` on BTreeStore serializes tree operations. Single WAL writer goroutine handles all write requests via channels (request/response pattern). Table cache ensures one instance per file.

**Transactions:** Auto-commit for single operations (INSERT/DELETE/UPDATE). Explicit transactions buffer operations and batch WAL writes on COMMIT. ABORT discards buffered operations. Each transaction sends all records in one WAL request for optimal performance.

**Durability:** Page writes call `Sync()` during eviction. WAL writer syncs after every flush. Checkpoint on graceful shutdown ensures clean state. Header flushed after insert/delete operations.

## Testing

```bash
# Unit tests
go test ./...
go test -v ./internal/btree/

# Integration tests
./test_chaos.sh         # Concurrent inserts/deletes (5 clients)
./test_reuse.sh         # Free page reuse verification
./stress_test.sh        # 10k sequential inserts

# Benchmarks
go test -bench=. ./internal/store/
```

## Known Limitations

- Primary key must be `int` type
- Single-level atomicity (no nested transactions or savepoints)
- No isolation levels (writes serialized by BTreeStore mutex)
- No indexes beyond primary key
- UPDATE uses DELETE + INSERT pattern (not in-place)
- No query optimizer
- REPL doesn't respect context cancellation (prompt persists on Ctrl+C until Enter pressed)

## Implementation Notes

**Delete strategy:** Compact-on-delete. Every `DeleteRecord()` calls `Compact()` to remove gaps immediately. Simplified merge logic at the cost of delete performance.

**Page format:** 13-byte header + slot array (grows down) + records (grow up). Binary search within sorted pages.

**Free pages:** Merges add orphaned pages to free list. Allocator checks free list before creating new pages. Cache evicts freed pages to prevent stale pointer bugs.

**Breadcrumb pattern:** Tracks descent path for bottom-up split/merge propagation. Approach from Petrov's book.

**VACUUM bulk loading:** Scans all records sequentially, packs into dense leaf pages, builds internal layers bottom-up. O(n) complexity vs O(n log n) for insert-based rebuild. Typically achieves ~50% space savings and 10x speed improvement.

## File Format

Tables stored as `.db` files:
- Page 0: Header (magic, version, root page ID, schema, free list)
- Page 1+: Slotted pages (leaf or internal nodes)
- 4KB pages with binary serialization (LittleEndian)

WAL stored as `.wal` files:
- LSN (8 bytes) + Action (1 byte) + record-specific fields
- Actions: INSERT, DELETE, UPDATE, CHECKPOINT, VACUUM
- Truncated on checkpoint, replayed on recovery

## Development

```bash
go fmt ./...
go vet ./...
go test -bench=. ./internal/store/  # Compare B+tree vs append-only
```

## References

- "Database Internals" by Alex Petrov
- "Introduction to Algorithms" (CLRS) - B-tree algorithms
- Slotted page layout standard in most RDBMS implementations

## License

MIT - Educational project, use freely.
