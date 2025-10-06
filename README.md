# Go-Database

A handrolled database implementation in Go, built from scratch as a learning project to understand database internals.

## Features

- **Custom Binary Storage Format** - Schema headers with variable-length records
- **Multi-Client TCP Server** - Concurrent connections on port 42069
- **Dynamic Schema System** - User-defined tables with custom field types
- **SQL-like Interface** - CREATE TABLE, INSERT, SELECT with point lookups
- **Type Support** - int32, string, bool, float64 (IEEE 754 encoding)
- **Interactive REPL** - Local command-line interface + network clients

## Quick Start

```bash
# Build the database
go build -o godb cmd/main.go

# Run the server (logs go to stderr)
./godb

# In another terminal, connect via TCP
nc localhost 42069

# Run tests
go test ./...

# Run specific B-tree tests
go test -v -run TestLeafSplit page_test.go page.go disk_manager.go header.go
```

## Example Usage

```sql
Go-DB> create products id:int name:string price:float stock:int
New table created: products

Go-DB [products]> insert 1 laptop 999.99 50
Inserting map[id:1 name:laptop price:999.99 stock:50] into table products

Go-DB [products]> insert 2 mouse 29.95 200
Inserting map[id:2 name:mouse price:29.95 stock:200] into table products

Go-DB [products]> select
| id   | name     | price      | stock      |
--------------------------------------------------------------------------------
| 1    | laptop   | 999.99     | 50         |
| 2    | mouse    | 29.95      | 200        |

Go-DB [products]> select 2
| id   | name     | price      | stock      |
--------------------------------------------------------------------------------
| 2    | mouse    | 29.95      | 200        |

Go-DB [products]> show
products
users
floaters

Go-DB [products]> use users
Switching to table: users
```

## Architecture

### Storage Layer
- **Append-only log** with last-write-wins semantics
- **Binary serialization** using LittleEndian encoding
- **Schema metadata** stored in file headers
- **Deduplication** on read (ScanAll/Find return latest record per ID)

### Concurrency
- **Table cache** ensures single TableStore instance per file
- **RWMutex protection** (writes locked, reads shared)
- **Per-session state** for network clients

### Command Processing
- **io.Writer abstraction** - same commands work for REPL and TCP
- **Dynamic schema parsing** - INSERT/SELECT adapt to table structure
- **Command registry** pattern for extensibility

## Commands

| Command | Description | Example |
|---------|-------------|---------|
| `create <table> <field:type> ...` | Create new table | `create users id:int name:string age:int` |
| `use <table>` | Switch active table | `use products` |
| `show` | List all tables | `show` |
| `insert <values...>` | Insert record | `insert 1 alice 30` |
| `select` | Scan all records | `select` |
| `select <id>` | Point lookup by ID | `select 5` |
| `.help` | Show help | `.help` |
| `.exit` | Exit the database | `.exit` |

## Network Protocol

Connect via TCP on port 42069:

```bash
nc localhost 42069
```

Multiple clients can connect simultaneously. Each client maintains independent session state (active table) while sharing the underlying data storage.

## File Format

Tables are stored as `.db` files with the following structure:

```
[Schema Header]
  - Table name (length-prefixed string)
  - Field count (uint32)
  - For each field:
    - Field name (length-prefixed string)
    - Field type (1 byte: 0=int, 1=string, 2=bool, 3=float)

[Records]
  - Variable-length records in schema field order
  - Types encoded as:
    - int32: 4 bytes LittleEndian
    - string: 4-byte length + UTF-8 bytes
    - bool: 1 byte (0/1)
    - float64: 8 bytes (IEEE 754 via math.Float64bits)
```

Example hexdump of `products.db`:
```
00000000: 0800 0000 7072 6f64 7563 7473 0400 0000  ....products....
00000010: 0200 0000 6964 0004 0000 006e 616d 6501  ....id.....name.
00000020: 0500 0000 7072 6963 6503 0500 0000 7374  ....price.....st
00000030: 6f63 6b00 0100 0000 0600 0000 6c61 7074  ock.........lapt
00000040: 6f70 d7a3 703d 0a8f 40c0 3200 0000       op..p=..@.2...
```

## Known Limitations (Current Append-Only Storage)

- **File descriptor sharing**: Concurrent writes under heavy load may cause corruption (shared `os.File` handle not thread-safe)
- **Map iteration randomness**: SELECT results may appear in different order each time (Go map iteration is deliberately randomized)
- **No file closing**: Files remain open for program lifetime (OS cleans up on exit)
- **O(n) scans**: Full table scans required for all queries (no indexing yet)

**Note**: These are accepted limitations of the current append-only log design. The in-progress B-tree page-based storage will address all of them.

## Learning Resources

This project follows concepts from:
- **"Database Internals" by Alex Petrov** - Foundation for B-tree implementation
- **"Introduction to Algorithms" (CLRS)** - B-tree algorithms
- **"Building a Database from Scratch in Go" by James Smith** - Initial inspiration

## B+ Tree Storage Engine

A fully functional B+ tree implementation is complete and tested:

**✅ Completed:**
- **Slotted page layout** - 4KB fixed pages with 13-byte headers, slot arrays, and variable-length records
- **Page-level disk I/O** - Binary serialization with LittleEndian encoding
- **Leaf node splits** - Promoted key handling with sibling pointer maintenance
- **Internal node splits** - Child pointer management and cascading split propagation
- **Root split handling** - Dynamic tree height growth when root overflows
- **Insert operation** - O(log n) insertion with automatic splitting and rebalancing
- **Search operation** - O(log n) point queries with multi-level tree traversal
- **RangeScan operation** - O(log n + k) range queries using sibling pointer chain
- **Table header** - PageID allocation tracking with durable persistence
- **Comprehensive test suite** - TestInsertNoSplit, TestInsertWithRootSplit, TestSearch, TestSearchAfterSplit, TestRangeScan

**Key Features:**
- Binary search for sorted insertion and lookup
- Breadcrumb stack pattern for bottom-up split propagation
- Sibling pointers linking leaf nodes for efficient range scans
- Max depth safety checks to prevent infinite loops
- Critical child pointer updates after split operations

**📋 Next Steps:**
- Integrate B+ tree with existing CLI (replace append-only TableStore)
- Implement Delete operation with node merging/rebalancing
- Add buffer pool for page caching in memory
- Update command handlers to use B-tree backend
- O(log n) lookups replacing current O(n) scans

## Project Goals

This is a **learning project**. The goal is deep understanding of:
- Binary serialization and file formats
- Database storage engines
- Concurrency control
- Network protocol design
- Systems programming in Go

## License

MIT License - This is an educational project, use freely!
