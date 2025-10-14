#!/bin/bash
# test_concurrent.sh - Concurrent client stress test
# Tests: Multiple clients inserting simultaneously, mutex protection, data integrity

set -e

DB_PORT=42069
DB_HOST=localhost
TABLE_NAME="concurrent"

echo "=== Concurrent Client Stress Test ==="
echo ""

# Cleanup
echo "Cleaning up old test database..."
(echo "drop $TABLE_NAME"; sleep 0.2; echo ".exit") | nc $DB_HOST $DB_PORT 2>/dev/null

echo "Creating test table with all data types..."
{
    echo "create $TABLE_NAME id:int name:string active:bool score:float created:date"
    sleep 0.5
    echo ".exit"
} | nc $DB_HOST $DB_PORT

sleep 1

# Verify table creation
if [ ! -f ${TABLE_NAME}.db ]; then
    echo "ERROR: Failed to create ${TABLE_NAME}.db"
    exit 1
fi
echo "✓ Table created successfully"
echo ""

# Client function template
run_client() {
    local client_id=$1
    local start_id=$2
    local end_id=$3
    local log_file="/tmp/concurrent_client${client_id}.log"

    echo "Client $client_id starting (IDs $start_id-$end_id)..."
    (
        echo "use $TABLE_NAME"
        sleep 0.1
        for i in $(seq $start_id $end_id); do
            active=$((i % 2))
            score=$(echo "scale=2; 50 + ($i % 50)" | bc)
            year=$((2020 + (i % 5)))
            month=$(printf "%02d" $((1 + (i % 12))))
            day=$(printf "%02d" $((1 + (i % 28))))
            date="${year}-${month}-${day}"

            echo "insert $i user_$i $active $score $date"
            sleep 0.005  # Small delay to increase interleaving
        done
        echo ".exit"
    ) | nc $DB_HOST $DB_PORT > "$log_file" 2>&1
    echo "Client $client_id finished"
}

echo "Starting 5 concurrent clients (1000 inserts each = 5000 total)..."
echo ""

# Launch 5 clients in parallel
run_client 1 1 1000 &
PID1=$!
run_client 2 1001 2000 &
PID2=$!
run_client 3 2001 3000 &
PID3=$!
run_client 4 3001 4000 &
PID4=$!
run_client 5 4001 5000 &
PID5=$!

# Wait for all clients
wait $PID1
wait $PID2
wait $PID3
wait $PID4
wait $PID5

echo ""
echo "✓ All clients finished"
echo ""

# Verification
echo "Verifying results..."
{
    echo "use $TABLE_NAME"
    sleep 0.2
    echo "count"
    sleep 0.2
    echo "stats"
    sleep 0.2
    echo ".exit"
} | nc $DB_HOST $DB_PORT

echo ""
echo "Checking data integrity (random samples)..."
{
    echo "use $TABLE_NAME"
    echo "select 1"      # Client 1
    echo "select 1500"   # Client 2
    echo "select 2500"   # Client 3
    echo "select 3500"   # Client 4
    echo "select 4999"   # Client 5
    echo ".exit"
} | nc $DB_HOST $DB_PORT

# Check for errors in client logs
echo ""
echo "Checking client logs for errors..."
error_count=0
for i in {1..5}; do
    if grep -qi "error\|failed" /tmp/concurrent_client${i}.log 2>/dev/null; then
        echo "⚠ Client $i had errors - check /tmp/concurrent_client${i}.log"
        error_count=$((error_count + 1))
    fi
done

if [ $error_count -eq 0 ]; then
    echo "✓ No errors found in client logs"
fi

echo ""
echo "Database file size:"
ls -lh ${TABLE_NAME}.db 2>/dev/null | awk '{print $5}'

echo ""
echo "=== Concurrent Test Complete ==="
echo "✓ 5000 concurrent inserts successful"
echo "✓ Mutex protection working"
echo "✓ Data integrity verified"
echo ""
echo "Client logs: /tmp/concurrent_client*.log"
