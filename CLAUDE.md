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

### Legacy B-Tree Scaffolding (`godatabase` package)

**btree.go** + **memory_disk.go** - CLRS-based B-tree implementation:
- Not currently used by CLI
- `DiskManager` interface for page-based I/O abstraction
- Implements insertion, search, and node splitting
- Will be replaced with page-based storage following Petrov's approach

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

## Next Steps (Future Development)

Planned transition to B-tree page-based storage:
- Fixed 4KB pages instead of append-only variable-length records
- Slotted page layout with page headers
- Page-level locking instead of file-level mutex
- Buffer pool for page caching
- O(log n) lookups instead of O(n) full scans

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
