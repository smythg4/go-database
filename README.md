# go-database

A database implementation in Go with B+ tree storage. Built as a learning project following "Database Internals" by Alex Petrov.

## Features

- B+ tree page-based storage (4KB pages)
- Multi-client TCP server on port 42069
- Basic CRUD operations with schema support
- Page caching with pin/unpin semantics
- Free page reuse after deletions
- Range scans via leaf sibling pointers
- VACUUM to rebuild and compact tree

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
insert 1 alice 30
insert 2 bob 25
select              -- full table scan
select 1            -- find by id
select 1 10         -- range scan (ids 1-10)
count
delete 2
stats               -- show tree structure
vacuum              -- rebuild tree
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
insert <val1> <val2> ...          Insert record
select [id] [start end]           Query records
update <val1> <val2> ...          Update record (delete + insert)
delete <id>                       Delete by primary key
count [id] [start end]            Count records
describe                          Show table schema
stats                             Show B+ tree statistics
vacuum                            Rebuild tree (requires reconnect)
drop <table>                      Delete table file
show                              List all tables
.exit                             Close connection
```

## Architecture

```
cmd/main.go              - TCP server + REPL
internal/cli/            - Command parsing
internal/store/          - BTreeStore wrapper
internal/btree/          - B+ tree implementation
internal/pager/          - Page cache, disk I/O, slotted pages
internal/schema/         - Schema and serialization
```

**Storage:** B+ tree with slotted pages. Primary key (first field) must be `int` type. Records stored in leaf nodes, internal nodes store routing keys.

**Caching:** 500-page FIFO buffer pool. Pages pinned during operations, unpinned via defer. Evicts unpinned pages when full.

**Concurrency:** `sync.RWMutex` on BTreeStore serializes tree operations. Table cache ensures one instance per file.

**Durability:** Page writes call `Sync()` during eviction. Header flushed after insert/delete operations.

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
- No transactions or ACID guarantees
- No indexes beyond primary key
- VACUUM requires client reconnect
- UPDATE uses DELETE + INSERT pattern
- Count command has off-by-one bug
- No query optimizer

## Implementation Notes

**Delete strategy:** Compact-on-delete. Every `DeleteRecord()` calls `Compact()` to remove gaps immediately. Simplified merge logic at the cost of delete performance.

**Page format:** 13-byte header + slot array (grows down) + records (grow up). Binary search within sorted pages.

**Free pages:** Merges add orphaned pages to free list. Allocator checks free list before creating new pages. Cache evicts freed pages to prevent stale pointer bugs.

**Breadcrumb pattern:** Tracks descent path for bottom-up split/merge propagation. Approach from Petrov's book.

## File Format

Tables stored as `.db` files:
- Page 0: Header (magic, version, root page ID, schema, free list)
- Page 1+: Slotted pages (leaf or internal nodes)
- 4KB pages with binary serialization (LittleEndian)

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
