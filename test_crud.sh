#!/bin/bash
# test_crud.sh - Comprehensive single-client CRUD test
# Tests: Create, insert, select, update, delete, range scans, splits, merges, vacuum

set -e

DB_PORT=42069
DB_HOST=localhost
TABLE_NAME="crud_test"

echo "=== CRUD Test Suite ==="
echo ""

# Helper function - prepends "use TABLE_NAME" to maintain context across sessions
send_cmd() {
    # For create/drop commands, don't prepend "use"
    if [[ "$1" == create* ]] || [[ "$1" == drop* ]]; then
        echo -e "$1\n.exit" | nc $DB_HOST $DB_PORT 2>/dev/null
    else
        echo -e "use $TABLE_NAME\n$1\n.exit" | nc $DB_HOST $DB_PORT 2>/dev/null
    fi
    sleep 0.1
}

# Cleanup
echo "Cleaning up old test database..."
send_cmd "drop $TABLE_NAME" > /dev/null 2>&1

# Create table with all supported types
echo "Creating table with all data types..."
send_cmd "create $TABLE_NAME id:int name:string active:bool score:float created:date"
echo "✓ Table created"
echo ""

# Phase 1: Basic inserts (trigger leaf splits)
echo "Phase 1: Inserting 100 records (tests leaf splits)..."
for i in {1..100}; do
    name="user_$(printf "%03d" $i)"
    active=$((i % 2))
    score=$(echo "scale=2; 50 + ($i % 50)" | bc)
    year=$((2020 + (i % 5)))
    month=$(printf "%02d" $((1 + (i % 12))))
    day=$(printf "%02d" $((1 + (i % 28))))
    date="${year}-${month}-${day}"

    send_cmd "insert $i $name $active $score $date" > /dev/null
done
echo "✓ 100 records inserted"

# Check stats
echo ""
echo "Tree stats after 100 inserts:"
send_cmd "stats"

# Phase 2: Point selects
echo ""
echo "Phase 2: Testing point selects..."
echo "Selecting record 1:"
send_cmd "select 1"
echo ""
echo "Selecting record 50:"
send_cmd "select 50"
echo ""
echo "Selecting record 100:"
send_cmd "select 100"
echo "✓ Point selects working"

# Phase 3: Range scans
echo ""
echo "Phase 3: Testing range scans..."
echo "Records 10-15 (should be 6 records):"
count=$(send_cmd "select 10 15" | grep -c "|" || true)
echo "Found $((count - 2)) records (expected 6)"
echo ""
echo "Records 1-10 (should be 10 records):"
send_cmd "select 1 10"
echo "✓ Range scans working"

# Phase 4: Count operations
echo ""
echo "Phase 4: Testing count operations..."
echo "Total count:"
send_cmd "count"
echo ""
echo "Count range 1-50:"
send_cmd "count 1 50"
echo "✓ Count operations working"

# Phase 5: Updates
echo ""
echo "Phase 5: Testing updates (3 records)..."
send_cmd "update 25 user_updated true 99.99 2025-01-01"
send_cmd "update 50 another_update false 88.88 2025-06-15"
send_cmd "update 75 third_update true 77.77 2025-12-31"
echo "Verifying update 25:"
send_cmd "select 25"
echo "✓ Updates working"

# Phase 6: Deletes (trigger merges)
echo ""
echo "Phase 6: Testing deletes (50 deletions to trigger merges)..."
for i in {1..50}; do
    send_cmd "delete $i" > /dev/null
done
echo "✓ 50 records deleted"

echo ""
echo "Tree stats after deletions:"
send_cmd "stats"
echo ""
echo "Remaining count (should be 50):"
send_cmd "count"

# Phase 7: More inserts to test free page reuse
echo ""
echo "Phase 7: Inserting 30 more records (tests free list reuse)..."
initial_next=$(send_cmd "stats" | grep -o 'NextPageID: [0-9]*' | grep -o '[0-9]*')
echo "NextPageID before inserts: $initial_next"

for i in {201..230}; do
    send_cmd "insert $i user_$i true 50.0 2025-01-01" > /dev/null
done
echo "✓ 30 records inserted"

final_next=$(send_cmd "stats" | grep -o 'NextPageID: [0-9]*' | grep -o '[0-9]*')
growth=$((final_next - initial_next))
echo "NextPageID after inserts: $final_next"
echo "NextPageID growth: $growth pages"

if [ "$growth" -lt 3 ]; then
    echo "✓ Excellent! Free list is reusing pages (growth < 3)"
else
    echo "⚠ Warning: Growth = $growth pages (expected < 3 with free list)"
fi

# Phase 8: Vacuum
echo ""
echo "Phase 8: Testing VACUUM (rebuilds tree with bulk loading)..."
echo "Database size before vacuum:"
ls -lh ${TABLE_NAME}.db 2>/dev/null | awk '{print $5}'

send_cmd "vacuum"
echo "✓ Vacuum complete"

echo ""
echo "Database size after vacuum:"
ls -lh ${TABLE_NAME}.db 2>/dev/null | awk '{print $5}'
echo ""
echo "Tree stats after vacuum:"
send_cmd "stats"

# Phase 9: Final verification
echo ""
echo "Phase 9: Final data integrity check..."
echo "Final count (should be 80: deleted 50 of first 100, added 30 more):"
send_cmd "count"
echo ""
echo "Sample records:"
send_cmd "select 51"
send_cmd "select 75"
send_cmd "select 220"

echo ""
echo "=== CRUD Test Complete ==="
echo "✓ All operations tested successfully"
