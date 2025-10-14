#!/bin/bash
# Simple WAL recovery test - easy to understand and debug

DB_PORT=42069
TABLE="wal_test"

echo "=== Simple WAL Recovery Test ==="
echo ""

# Step 1: Start server
echo "Step 1: Starting server..."
./godb </dev/null 2>wal_test.log &
SERVER_PID=$!
sleep 2
echo "Server started (PID: $SERVER_PID)"
echo ""

# Step 2: Create table and insert 3 records
echo "Step 2: Creating table and inserting 3 records..."
{
    echo "drop $TABLE"
    echo "create $TABLE id:int name:string"
    echo "use $TABLE"
    echo "insert 1 Alice"
    echo "insert 2 Bob"
    echo "insert 3 Charlie"
    echo "count"
    echo ".exit"
} | nc localhost $DB_PORT

echo ""
echo "Step 3: Check database files..."
ls -lh ${TABLE}.db ${TABLE}.wal 2>/dev/null || echo "Files not found!"
echo ""

# Step 4: Kill server (simulated crash)
echo "Step 4: Killing server (simulated crash)..."
kill -9 $SERVER_PID
sleep 2
echo "Server killed"
echo ""

# Step 5: Check WAL file still exists
echo "Step 5: WAL file after crash:"
ls -lh ${TABLE}.wal 2>/dev/null || echo "WAL file missing!"
if [ -f "${TABLE}.wal" ]; then
    WAL_SIZE=$(stat -f%z "${TABLE}.wal" 2>/dev/null || echo "0")
    echo "WAL size: $WAL_SIZE bytes"
fi
echo ""

# Step 6: Restart server
echo "Step 6: Restarting server..."
./godb </dev/null 2>>wal_test.log &
NEW_PID=$!
sleep 2
echo "Server restarted (PID: $NEW_PID)"
echo ""

# Step 7: Check if records were recovered
echo "Step 7: Checking if records were recovered from WAL..."
{
    echo "use $TABLE"
    echo "count"
    echo "select 1"
    echo "select 2"
    echo "select 3"
    echo ".exit"
} | nc localhost $DB_PORT

echo ""
echo "=== Test Complete ==="
echo ""
echo "EXPECTED: Count = 3, all records found"
echo "ACTUAL: See output above"
echo ""
echo "Server log: wal_test.log"

# Cleanup
kill -9 $NEW_PID 2>/dev/null
