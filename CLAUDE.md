# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A handrolled database implementation in Go featuring a B-tree data structure, a REPL interface, and multiple storage layers including key-value and table storage with schema support.

## Architecture

The codebase has evolved into a layered architecture with three main storage implementations:

### Core B-Tree Layer (`godatabase` package)
- **btree.go**: B-tree implementation with insertion, search, and node splitting
  - `BNode`: Tree nodes with keys (uint64), values ([]byte), and child pointers
  - `BTreeInsert`, `BTreeSearch`: Core operations with disk I/O through `DiskManager` interface
  - `BTreeSplitChild`, `BTreeInsertNonFull`: Node management during insertions
  - Constant `T = 1000`: Minimum degree controlling tree height
  - Type system: `PageID` (uint32), `KeyCount` (uint16), `Key` (uint64), `Value` ([]byte)

- **memory_disk.go**: `MemoryDiskManager` implementing `DiskManager` interface
  - In-memory map-based storage for testing/learning
  - Simulates disk I/O with node copying (serialization/deserialization)
  - Page allocation with auto-incrementing PageIDs

### Storage Layer (`internal/store` package)
- **kv.go**: `KVStore` - Simple append-only key-value storage
  - Direct file-based storage using binary encoding (LittleEndian)
  - Stores int32 key-value pairs as 8-byte records
  - Linear search from end of file for latest value (allows overwrites)

- **table.go**: `TableStore` - Schema-based table storage
  - Header contains schema metadata (table name, field definitions)
  - Fixed schema defined at initialization (users table: id, name, age)
  - Binary encoding with length-prefixed strings
  - Full table scans for SELECT operations
  - Supports int32, string, and bool field types

### Schema Layer (`internal/schema` package)
- **schema.go**: Type definitions for structured data
  - `Schema`: Table name + field definitions
  - `Field`: Name and type (IntType, StringType, BoolType, FloatType)
  - `Record`: map[string]any for row data

### CLI/REPL Layer (`cmd/main.go` + `internal/cli/commands.go`)
- **main.go**: Entry point with REPL loop
  - `DatabaseConfig`: Holds references to all storage backends
  - Command routing through `commandRegistry`
  - Creates two persistent files: `test.db` (KV), `table.db` (Table)

- **commands.go**: Command implementations
  - Meta-commands: `.help`, `.exit`
  - KV commands: `put`, `get` (int32 keys/values)
  - Table commands: `insert`, `select` (id, name, age records)

## Development Commands

```bash
# Run the interactive database
go run cmd/main.go

# Build executable
go build -o godb cmd/main.go

# Run tests
go test ./...

# Format and vet
go fmt ./...
go vet ./...
```

## Key Architecture Patterns

- **DiskManager Interface**: Abstraction allowing swap between in-memory and file-based storage
- **Binary Encoding**: LittleEndian encoding for all numeric types
- **Schema Evolution**: Table schemas written to file headers, read on startup
- **Multiple Storage Backends**: B-tree (unused in CLI currently), KV store, and Table store coexist
- **REPL Design**: Command registry pattern with lowercase command parsing

## Important Implementation Details

- B-tree operations properly handle disk I/O through `DiskManager` interface
- KVStore searches backwards from end of file to find most recent key value
- TableStore enforces schema order during insert/read operations
- Node splitting in B-tree follows classic algorithm with median key promotion
- Package imports in `cmd/main.go` need to reference types from `internal/` packages (currently shows compilation issues)
- this is a learning project. Guide me in general, but don't write code for me.