# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a handrolled database implementation written in Go, featuring a B-tree data structure for efficient key-value storage and retrieval.

## Architecture

The project is organized into two main components:

- **btree.go**: Core B-tree data structure implementation in the `godatabase` package
  - `BNode` struct representing tree nodes with keys, values, and child pointers
  - `BTreeSearch` method for key lookup operations
  - `NewBTree` constructor for creating new B-tree instances
  - Uses `PageID` (uint32) and `KeyCount` (uint16) types for database page management

- **cmd/main.go**: Entry point application in the `main` package
  - Currently minimal, prints placeholder message

## Development Commands

```bash
# Run the application
go run cmd/main.go

# Build the application
go build -o godb cmd/main.go

# Run tests (when available)
go test ./...

# Format code
go fmt ./...

# Vet code for issues
go vet ./...
```

## Key Types and Interfaces

- `PageID`: Represents database page identifiers (uint32)
- `KeyCount`: Represents number of keys in a node (uint16) 
- `BNode`: Core B-tree node structure with leaf/internal node support
- Missing type definitions: `Key` and `Value` types need to be defined

## Notes

- The B-tree implementation includes comments indicating where disk I/O operations will be added
- The search algorithm currently has a recursive call that needs proper disk page loading
- Module name is `godb` (from go.mod)