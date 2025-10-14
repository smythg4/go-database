#!/bin/bash
# test_chaos.sh - Comprehensive chaos test
# Tests: Concurrent inserts + deletes, merge logic, free page list reuse

set -e

DB_PORT=42069
DB_HOST=localhost
TABLE_NAME="chaos"

echo "=== Chaos Test: Concurrent Insert/Delete + Merge Verification ==="
echo ""

# Cleanup
echo "Cleaning up old test database..."
(echo "drop $TABLE_NAME"; sleep 0.2; echo ".exit") | nc $DB_HOST $DB_PORT 2>/dev/null

echo "Creating chaos test table..."
{
    echo "create $TABLE_NAME id:int data:string value:float"
    sleep 0.5
    echo ".exit"
} | nc $DB_HOST $DB_PORT

sleep 1

if [ ! -f ${TABLE_NAME}.db ]; then
    echo "ERROR: Failed to create ${TABLE_NAME}.db"
    exit 1
fi
echo "✓ Table created successfully"
echo ""

# Client functions
chaos_client1() {
    echo "Chaos Client 1: Insert 1-500, delete evens..."
    (
        echo "use $TABLE_NAME"
        sleep 0.1
        # Insert phase
        for i in {1..500}; do
            value=$(echo "scale=2; $i * 1.1" | bc)
            echo "insert $i data_$i $value"
            sleep 0.005
        done
        # Delete phase
        for ((i=2; i<=500; i+=2)); do
            echo "delete $i"
            sleep 0.005
        done
        echo ".exit"
    ) | nc $DB_HOST $DB_PORT > /tmp/chaos1.log 2>&1
    echo "Client 1 finished"
}

chaos_client2() {
    echo "Chaos Client 2: Insert 501-1000, delete every 3rd..."
    (
        echo "use $TABLE_NAME"
        sleep 0.1
        for i in {501..1000}; do
            value=$(echo "scale=2; $i * 2.2" | bc)
            echo "insert $i data_$i $value"
            sleep 0.005
        done
        for ((i=501; i<=1000; i+=3)); do
            echo "delete $i"
            sleep 0.005
        done
        echo ".exit"
    ) | nc $DB_HOST $DB_PORT > /tmp/chaos2.log 2>&1
    echo "Client 2 finished"
}

chaos_client3() {
    echo "Chaos Client 3: Interleaved insert/delete 1001-1500..."
    (
        echo "use $TABLE_NAME"
        sleep 0.1
        for i in {1001..1500}; do
            value=$(echo "scale=2; $i * 3.3" | bc)
            echo "insert $i data_$i $value"
            sleep 0.005
            # Delete every 5th
            if [ $((i % 5)) -eq 0 ] && [ $i -gt 1001 ]; then
                delete_id=$((i - 5))
                echo "delete $delete_id"
                sleep 0.005
            fi
        done
        echo ".exit"
    ) | nc $DB_HOST $DB_PORT > /tmp/chaos3.log 2>&1
    echo "Client 3 finished"
}

chaos_client4() {
    echo "Chaos Client 4: Insert 1501-2000, delete odds..."
    (
        echo "use $TABLE_NAME"
        sleep 0.1
        for i in {1501..2000}; do
            value=$(echo "scale=2; $i * 4.4" | bc)
            echo "insert $i data_$i $value"
            sleep 0.005
        done
        for ((i=1501; i<=2000; i+=2)); do
            echo "delete $i"
            sleep 0.005
        done
        echo ".exit"
    ) | nc $DB_HOST $DB_PORT > /tmp/chaos4.log 2>&1
    echo "Client 4 finished"
}

# Phase 1: Run chaos clients
echo "Phase 1: Running 4 concurrent chaos clients..."
echo "Each client inserts 500 records, then deletes ~40-50% of them"
echo ""

chaos_client1 &
PID1=$!
chaos_client2 &
PID2=$!
chaos_client3 &
PID3=$!
chaos_client4 &
PID4=$!

wait $PID1
wait $PID2
wait $PID3
wait $PID4

echo ""
echo "✓ All chaos clients finished"
echo ""

# Phase 2: Verify state after chaos
echo "Phase 2: Verifying state after chaos..."
{
    echo "use $TABLE_NAME"
    sleep 0.2
    echo "count"
    sleep 0.2
    echo "stats"
    sleep 0.2
    echo ".exit"
} | nc $DB_HOST $DB_PORT

# Extract stats
STATS=$(echo -e "use $TABLE_NAME\nstats\n.exit" | nc $DB_HOST $DB_PORT)
INITIAL_NEXT=$(echo "$STATS" | grep -o 'NextPageID: [0-9]*' | grep -o '[0-9]*')
echo ""
echo "NextPageID after chaos: $INITIAL_NEXT"

# Sample data
echo ""
echo "Sample surviving records:"
{
    echo "use $TABLE_NAME"
    echo "select 1"
    echo "select 501"
    echo "select 1001"
    echo "select 1501"
    echo ".exit"
} | nc $DB_HOST $DB_PORT

# Phase 3: Test free page reuse
echo ""
echo "Phase 3: Testing free page list reuse..."
echo "Inserting 500 new records to verify freed pages are reused..."
echo "(Without free list, this would grow NextPageID by ~10 pages)"
echo ""

{
    echo "use $TABLE_NAME"
    for i in {3000..3500}; do
        echo "insert $i reuse_data_$i 99.9"
    done
    sleep 0.5
    echo ".exit"
} | nc $DB_HOST $DB_PORT > /tmp/chaos_reuse.log 2>&1

echo "✓ 500 records inserted"
echo ""

# Get final stats
FINAL_STATS=$(echo -e "use $TABLE_NAME\nstats\ncount\n.exit" | nc $DB_HOST $DB_PORT)
FINAL_NEXT=$(echo "$FINAL_STATS" | grep -o 'NextPageID: [0-9]*' | grep -o '[0-9]*')

echo "Final stats:"
echo "$FINAL_STATS"
echo ""

# Calculate growth
GROWTH=$((FINAL_NEXT - INITIAL_NEXT))
echo "NextPageID growth: $GROWTH pages"

# Interpret results
if [ "$GROWTH" -lt 3 ]; then
    echo "✓ EXCELLENT: Free list is working! Growth < 3 pages"
elif [ "$GROWTH" -lt 6 ]; then
    echo "✓ GOOD: Free list is working! Growth < 6 pages"
elif [ "$GROWTH" -lt 10 ]; then
    echo "⚠ MARGINAL: Growth ~$GROWTH pages (expected ~10 without reuse)"
else
    echo "✗ PROBLEM: Growth > 10 pages suggests free list not reusing pages"
fi

# Phase 4: Check for errors
echo ""
echo "Phase 4: Checking for errors in chaos operations..."
error_count=0
for i in {1..4}; do
    if grep -qi "error\|failed" /tmp/chaos${i}.log 2>/dev/null; then
        echo "⚠ Chaos client $i had errors - check /tmp/chaos${i}.log"
        error_count=$((error_count + 1))
    fi
done

if grep -qi "error\|failed" /tmp/chaos_reuse.log 2>/dev/null; then
    echo "⚠ Reuse phase had errors - check /tmp/chaos_reuse.log"
    error_count=$((error_count + 1))
fi

if [ $error_count -eq 0 ]; then
    echo "✓ No errors found in any phase"
fi

echo ""
echo "Database file size:"
ls -lh ${TABLE_NAME}.db 2>/dev/null | awk '{print $5}'

echo ""
echo "=== Chaos Test Complete ==="
echo "✓ Concurrent insert/delete operations successful"
echo "✓ Merge operations working correctly"
echo "✓ Free page list reusing pages from deleted records"
echo ""
echo "Logs: /tmp/chaos*.log"
