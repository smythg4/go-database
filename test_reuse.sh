#!/bin/bash

# Test script: Insert 100, delete 50, insert 100 more
# Verifies free page reuse after deletions

echo "Cleaning up old test database..."
echo -e "drop reuse\n.exit" | nc localhost 42069

echo ""
echo "Creating fresh test table..."
echo -e "create reuse id:int data:string value:float\n.exit" | nc localhost 42069

echo ""
echo "Phase 1: Inserting records 1-100..."
{
    echo "use reuse"
    for i in {1..100}; do
        value=$(echo "scale=1; $i * 1.1" | bc)
        echo "insert $i data_$i $value"
    done
    echo ".exit"
} | nc localhost 42069 > /dev/null

echo "Phase 1 complete. Checking stats..."
echo -e "use reuse\nstats\ncount\n.exit" | nc localhost 42069

echo ""
echo "Phase 2: Deleting records 26-100 (75 deletions)..."
{
    echo "use reuse"
    for i in {26..100}; do
        echo "delete $i"
    done
    echo ".exit"
} | nc localhost 42069 > /dev/null

echo "Phase 2 complete. Checking stats after deletions..."
echo -e "use reuse\nstats\ncount\n.exit" | nc localhost 42069

echo ""
echo "Phase 3: Inserting records 101-200 (100 more inserts)..."
{
    echo "use reuse"
    for i in {101..200}; do
        value=$(echo "scale=1; $i * 2.2" | bc)
        echo "insert $i data_$i $value"
    done
    echo ".exit"
} | nc localhost 42069 > /dev/null

echo "Phase 3 complete. Final stats..."
echo -e "use reuse\nstats\ncount\n.exit" | nc localhost 42069

echo ""
echo "Verifying data integrity..."
echo "Should have: records 1-25, 101-200 (total: 125)"
echo -e "use reuse\nselect 1\nselect 25\nselect 26\nselect 100\nselect 101\nselect 200\n.exit" | nc localhost 42069

echo ""
echo "Full table dump (first 10 and last 10 records)..."
echo -e "use reuse\nselect\n.exit" | nc localhost 42069 | head -20
echo "..."
echo -e "use reuse\nselect\n.exit" | nc localhost 42069 | tail -15

echo ""
echo "Database file size BEFORE vacuum:"
ls -lh reuse.db

echo ""
echo "Running VACUUM to compact and reclaim space..."
echo -e "use reuse\nvacuum\nstats\ncount\n.exit" | nc localhost 42069

echo ""
echo "Database file size AFTER vacuum:"
ls -lh reuse.db

echo ""
echo "Expected: NextPageID should NOT grow much in phase 3"
echo "         (freed pages from phase 2 should be reused)"
echo "After VACUUM: File should be smaller, NextPageID reset to minimal"
