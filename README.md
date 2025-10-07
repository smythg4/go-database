# Go-Database

A from-scratch database implementation in Go, built to answer the question: 'How does this actually work?' Spoiler: slotted pages, breadcrumb stacks, and more off-by-one errors than I'd like to admit.

## Features

- **B+ Tree Storage Engine** - O(log n) insertions, lookups, and deletions with automatic page splitting
- **Page-Based Disk Format** - 4KB slotted pages with binary serialization
- **Multi-Client TCP Server** - Concurrent connections on port 42069
- **Dynamic Schema System** - User-defined tables with custom field types
- **SQL-like Interface** - CREATE, INSERT, SELECT, UPDATE, DELETE, DESCRIBE, COUNT, DROP with primary key constraints
- **Node Merging** - Automatic page merging when nodes become underfull after deletion
- **Free Page List** - Orphaned pages from merges are tracked and reused, preventing file growth
- **Range Queries** - Efficient range scans with O(log n + k) complexity via leaf sibling pointers
- **Type Support** - int32, string, bool, float64, date (ISO 8601)
- **Primary Key Uniqueness** - Duplicate key detection with PostgreSQL-style errors
- **Interactive REPL** - Local command-line interface + network clients
- **Schema Introspection** - DESCRIBE command shows table structure and field types

## Points of Pride
- **3,538x faster lookups** - Benchmarked against legacy append-only storage: 6.9μs vs 24ms for 10,000 record lookups. O(log n) vs O(n) in action.
- **5,000 concurrent inserts, zero corruption** - Stress tested with 5 concurrent TCP clients hammering the database simultaneously. All data persists correctly.
- **Chaos testing with concurrent deletes** - 5 clients running interleaved inserts and deletes (2,500 inserts, 1,017 deletes) with zero corruption. Free page list verified working.
- **Zero file growth from page reuse** - After chaos test, inserting 501 new records consumed 0 new pages (all reused from free list). NextPageID growth: 0.
- **Caught catastrophic durability bug** - Stress testing revealed 100% data loss on restart. Pages written to OS buffer but never synced to disk. Fixed by adding `Sync()` to `WritePage()`. 5,000 records now survive restart.
- **Breadcrumb stack for split propogation** - Implemented Petrov's breadcrumb pattern for bottom-up split cascading. Took 3 tries to get child pointer updates right.
- **Full CRUD operations** - CREATE, INSERT, SELECT, UPDATE, DELETE all working with proper error handling and persistence.
- **Write-before-recursion pattern** - Critical durability insight: write nodes before checking underflow to prevent stale pointers. Appears at both leaf and parent levels.

## Quick Start

```bash
# Build the database
go build -o godb cmd/main.go

# Run the server (logs go to stderr)
./godb

# In another terminal, connect via TCP
nc localhost 42069

# Run all tests
go test ./...

# Run B-tree tests specifically
go test -v ./internal/btree/

# Run page tests
go test -v ./internal/pager/

# Benchmark B+Tree vs legacy storage
go test -bench=. ./internal/store/

# Stress test concurrent operations
./test_concurrent.sh      # 5 clients inserting 1000 records each
./stress_test.sh          # 150 inserts, 75 deletes with persistence verification
./test_chaos.sh           # Chaos test: 5 clients with interleaved inserts/deletes
./test_freelist.sh        # Verify free page reuse after chaos test
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

Go-DB [products]> insert 2 keyboard 79.99 100
Error: key 2 already exists

Go-DB [products]> delete 1
Deleting map[id:1 name:laptop price:999.99 stock:50] from table products

Go-DB [products]> select
| id   | name     | price      | stock      |
--------------------------------------------------------------------------------
| 2    | mouse    | 29.95      | 200        |

Go-DB [products]> create employees id:int name:string birthdate:date
New table created: employees

Go-DB [employees]> insert 1 alice 1985-06-15
Inserting map[birthdate:487900800 id:1 name:alice] into table employees

Go-DB [employees]> select
| id   | name     | birthdate  |
----------------------------------------
| 1    | alice    | 1985-06-15 |

Go-DB [employees]> stats
Root: page 1, Type: 0, NextPageID: 2, NumPages: 1, Tree Depth: 1

Go-DB [employees]> show
products
employees

Go-DB [employees]> use products
Switching to table: products

Go-DB [products]> describe
Table: products
   id (int) - PRIMARY KEY
   name (string)
   price (float)
   stock (int)

Go-DB [products]> insert 3 keyboard 79.99 150
Inserting map[id:3 name:keyboard price:79.99 stock:150] into table products

Go-DB [products]> insert 4 monitor 299.99 75
Inserting map[id:4 name:monitor price:299.99 stock:75] into table products

Go-DB [products]> select 2 4
| id   | name     | price      | stock      |
--------------------------------------------------------------------------------
| 2    | mouse    | 29.95      | 200        |
| 3    | keyboard | 79.99      | 150        |
| 4    | monitor  | 299.99     | 75         |

Go-DB [products]> count 2 4
Count: 3

Go-DB [products]> count
Count: 3

Go-DB [products]> stats
Root: page 1, Type: 0, NextPageID: 2, NumPages: 1, Tree Depth: 1
```

## Architecture

### Storage Layer
- **B+ Tree storage engine** - O(log n) insertions, lookups, and range scans
- **Slotted page format** - 4KB fixed pages with 13-byte headers, slot arrays, and variable-length records
- **Page-based disk I/O** - Binary serialization using LittleEndian encoding
- **Schema metadata** - Stored in page 0 header with table name and field definitions
- **Primary key constraints** - Unique constraint on first field (must be int32)
- **Automatic splitting** - Leaf and internal nodes split when full, tree height grows dynamically

### Concurrency
- **Table cache** ensures single BTreeStore instance per file
- **RWMutex protection** (writes locked, reads shared)
- **Per-session state** for network clients

### Command Processing
- **io.Writer abstraction** - Same commands work for REPL and TCP
- **Dynamic schema parsing** - INSERT/SELECT adapt to table structure
- **Command registry** pattern for extensibility

## Commands

| Command | Description | Example |
|---------|-------------|---------|
| `create <table> <field:type> ...` | Create new table (first field = primary key) | `create users id:int name:string birthdate:date` |
| `use <table>` | Switch active table | `use products` |
| `show` | List all tables | `show` |
| `describe` | Show schema for active table | `describe` |
| `insert <values...>` | Insert record (errors on duplicate key) | `insert 1 alice 1990-05-15` |
| `select` | Scan all records | `select` |
| `select <id>` | Point lookup by ID (O(log n)) | `select 5` |
| `select <start> <end>` | Range query (O(log n + k)) | `select 10 100` |
| `update <values...>` | Update record (DELETE + INSERT) | `update 5 bob 1992-03-20` |
| `delete <id>` | Delete record by ID (O(log n)) | `delete 5` |
| `count` | Count all records | `count` |
| `count <id>` | Count single record (0 or 1) | `count 5` |
| `count <start> <end>` | Count records in range | `count 10 100` |
| `drop <table>` | Drop table and delete file | `drop products` |
| `stats` | Show tree structure (root, depth, pages) | `stats` |
| `.help` | Show help | `.help` |
| `.exit` | Exit the database | `.exit` |

**Supported Types:**
- `int` - 32-bit signed integer
- `string` - Variable-length UTF-8 string
- `bool` - Boolean (true/false)
- `float` - 64-bit IEEE 754 floating point
- `date` - Date in ISO 8601 format (YYYY-MM-DD), stored as Unix timestamp

## Network Protocol

Connect via TCP on port 42069:

```bash
nc localhost 42069
```

Multiple clients can connect simultaneously. Each client maintains independent session state (active table) while sharing the underlying data storage.

## File Format

Tables are stored as `.db` files with page-based B+ tree structure:

```
[Page 0: Table Header - 4KB]
  - Magic: "GDBT" (4 bytes)
  - Version: 1 (uint16)
  - RootPageID: Page ID of root node (uint32)
  - NextPageID: Next available page for allocation (uint32)
  - NumPages: Total pages in file (uint32)
  - Schema:
    - Table name (length-prefixed string)
    - Field count (uint32)
    - For each field:
      - Field name (length-prefixed string)
      - Field type (1 byte: 0=int, 1=string, 2=bool, 3=float, 4=date)
  - FreePageIDs:
    - Free page count (uint32)
    - For each free page: PageID (uint32)

[Page 1+: Slotted Pages - 4KB each]
  Header (13 bytes):
    - PageType: LEAF (0) or INTERNAL (1)
    - NumSlots: Number of records/keys (uint16)
    - FreeSpacePtr: Offset to free space (uint16)
    - RightmostChild: Child page for keys > all slots (uint32, internal only)
    - NextLeaf: Sibling page pointer (uint32, leaf only)

  Slot Array (grows downward from byte 13):
    - Each slot: [offset: uint16][length: uint16]

  Records (grow upward from end):
    - Leaf: [key: 8 bytes uint64][serialized record data]
    - Internal: [key: 8 bytes uint64][child PageID: 4 bytes uint32]

  Types encoded as:
    - int32: 4 bytes LittleEndian
    - string: 4-byte length + UTF-8 bytes
    - bool: 1 byte (0/1)
    - float64: 8 bytes (IEEE 754 via math.Float64bits)
    - date: 8 bytes (Unix timestamp as int64, displayed as YYYY-MM-DD UTC)
```

Example hexdump showing header with B+ tree metadata:
```
00000000: 4744 4254 0100 0100 0000 0200 0000 0100  GDBT............
          ^^^^ ^^^^ ^^^^ ^^^^  ^^^^ ^^^^  ^^^^ ^^^^
          Magic Ver  Root=1     Next=2     NumPages=1
```

## Current Limitations

- **UPDATE uses DELETE + INSERT**: Not true in-place modification (functional but not optimal)
- **No borrowing during rebalance**: Merge-only strategy may cause fragmentation in some cases
- **No VACUUM command**: Free page list prevents growth but doesn't reclaim space from file (NextPageID never decreases)
- **Primary key must be int32**: First field in schema must be int32 type
- **No transactions**: Operations commit immediately, no rollback support
- **No buffer pool**: Every page read/write hits disk (future optimization)

## Learning Resources

This project follows concepts from:
- **"Database Internals" by Alex Petrov** - Foundation for B-tree implementation
- **"Introduction to Algorithms" (CLRS)** - B-tree algorithms
- **"Building a Database from Scratch in Go" by James Smith** - Initial inspiration

## B+ Tree Implementation Details

The storage engine uses a fully functional B+ tree with the following characteristics:

**✅ Core Operations:**
- **Insert** - O(log n) insertion with duplicate key detection (PostgreSQL-style error), automatic splitting, and cascading propagation
- **Search** - O(log n) point queries with multi-level tree traversal (max depth 100)
- **Delete** - O(log n) deletion with tombstone pattern, automatic merging when underfull, and bottom-up cascade
- **RangeScan** - O(log n + k) range queries using sibling pointer chain across leaf nodes with cycle detection
- **Stats** - Debug helper showing root page ID, node type (LEAF/INTERNAL), and NextPageID allocation

**Key Implementation Details:**
- **Slotted pages** - 4KB fixed pages with 13-byte headers, slot arrays growing downward, records growing upward
- **Binary search** - Sorted insertion and lookup within pages
- **Breadcrumb stack** - Tracks descent path for bottom-up split/merge propagation (pattern from Petrov's "Database Internals")
- **Sibling pointers** - Leaf nodes linked for efficient range scans (B+ tree characteristic)
- **Child pointer management** - After inserting promoted key at index i, updates record[i+1] or RightmostChild
- **Header durability** - Defer pattern syncs BTree header changes (RootPageID, NextPageID, FreePageIDs) back to DiskManager before write
- **Primary key uniqueness** - Search before insert, error on duplicate key (PostgreSQL-style behavior)
- **Tombstone deletion** - DeleteRecord() marks slot as empty (Offset=0), then Compact() removes gaps
- **Merge strategy** - Prefer left sibling, merge left-into-right, demote separator key for internal nodes
- **Free page list** - allocatePage() pops from FreePageIDs before allocating new pages; merge operations append orphaned pages to list
- **Write-before-recursion** - Critical pattern: write leaf before checking underflow, write parent before checking parent underflow
- **Root collapse** - When internal root has only RightmostChild, promote it to new root

**Test Coverage:**
- Insert without split (single leaf)
- Insert with root split (tree height growth from 1 → 2)
- Search in single-level and multi-level trees
- Range scan across multiple leaves via sibling pointers
- Page serialization round-trips
- Split mechanics (leaf/internal with child pointer validation)
- Stress testing: 150 inserts verified multi-level tree growth

**Future Enhancements:**
- VACUUM command (rebuild tree compactly, reclaim disk space from file)
- Node borrowing (currently merge-only, no borrowing from siblings)
- Buffer pool for page caching (reduce disk I/O)
- Support non-int primary keys via hashing (currently first field must be int32)
- Transaction support with ACID guarantees (WAL, ARIES recovery)
- True in-place UPDATE (currently uses DELETE + INSERT pattern)

## Project Goals

This is a **learning project**. The goal is deep understanding of:
- Binary serialization and file formats
- Database storage engines
- Concurrency control
- Network protocol design
- Systems programming in Go

## License

MIT License - This is an educational project, use freely!
