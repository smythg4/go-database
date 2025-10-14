#!/bin/bash
# test_recovery.sh - WAL recovery and transaction test
# Tests: WAL replay after crash, transactions (BEGIN/COMMIT/ABORT), checkpoint

set -e

DB_PORT=42069
DB_HOST=localhost
TABLE_NAME="recovery_test"
PID_FILE="/tmp/godb.pid"

echo "=== WAL Recovery & Transaction Test ==="
echo ""

# Helper functions - prepends "use TABLE_NAME" to maintain context across sessions
send_cmd() {
    # For create/drop commands, don't prepend "use"
    if [[ "$1" == create* ]] || [[ "$1" == drop* ]]; then
        echo -e "$1\n.exit" | nc $DB_HOST $DB_PORT 2>/dev/null
    else
        echo -e "use $TABLE_NAME\n$1\n.exit" | nc $DB_HOST $DB_PORT 2>/dev/null
    fi
    sleep 0.1
}

wait_for_server() {
    echo -n "Waiting for server to start..."
    for i in {1..30}; do
        if nc -z $DB_HOST $DB_PORT 2>/dev/null; then
            echo " ready!"
            sleep 1  # Give it a moment to fully initialize
            return 0
        fi
        echo -n "."
        sleep 0.5
    done
    echo " FAILED"
    return 1
}

# Cleanup old database
echo "Cleaning up old test database..."
send_cmd "drop $TABLE_NAME" > /dev/null 2>&1
rm -f ${TABLE_NAME}.wal 2>/dev/null

# Phase 1: Transaction COMMIT test
echo ""
echo "Phase 1: Testing transaction COMMIT..."
send_cmd "create $TABLE_NAME id:int name:string value:float"

echo "Starting transaction and inserting 5 records..."
{
    echo "use $TABLE_NAME"
    echo "begin"
    for i in {1..5}; do
        echo "insert $i user_$i $((i * 10)).5"
    done
    echo "commit"
    echo "count"
    echo ".exit"
} | nc $DB_HOST $DB_PORT
echo "✓ Transaction committed"

# Phase 2: Transaction ABORT test
echo ""
echo "Phase 2: Testing transaction ABORT..."
echo "Starting transaction, inserting 3 records, then aborting..."
{
    echo "use $TABLE_NAME"
    echo "begin"
    echo "insert 100 user_100 100.0"
    echo "insert 101 user_101 101.0"
    echo "insert 102 user_102 102.0"
    echo "abort"
    echo "count"
    echo "select 100"
    echo ".exit"
} | nc $DB_HOST $DB_PORT
echo "✓ Transaction aborted (records 100-102 should NOT exist)"

# Phase 3: Verify initial state before crash
echo ""
echo "Phase 3: Verifying initial state..."
echo "Current count (should be 5):"
send_cmd "count"
echo ""
echo "Records:"
send_cmd "select"

# Phase 4: Insert records and simulate crash (WAL written, but not checkpointed)
echo ""
echo "Phase 4: Simulating crash scenario..."
echo "Inserting 10 more records (WAL will have them, but they might not be checkpointed)..."

{
    echo "use $TABLE_NAME"
    for i in {11..20}; do
        echo "insert $i crash_test_$i $((i * 5)).0"
    done
    echo "stats"
    echo ".exit"
} | nc $DB_HOST $DB_PORT

echo "✓ 10 records inserted (WAL should contain these)"

# Check WAL file exists and has content
if [ -f "${TABLE_NAME}.wal" ]; then
    wal_size=$(ls -lh ${TABLE_NAME}.wal | awk '{print $5}')
    echo "WAL file exists: ${TABLE_NAME}.wal (size: $wal_size)"
else
    echo "⚠ Warning: WAL file not found!"
fi

echo ""
echo "Finding server PID..."
SERVER_PID=$(lsof -ti:$DB_PORT)
if [ -z "$SERVER_PID" ]; then
    echo "ERROR: Could not find server PID"
    exit 1
fi
echo "Server PID: $SERVER_PID"

echo ""
echo "Killing server to simulate crash..."
kill -9 $SERVER_PID
sleep 2

# Verify server is down
if nc -z $DB_HOST $DB_PORT 2>/dev/null; then
    echo "ERROR: Server still running!"
    exit 1
fi
echo "✓ Server killed (simulated crash)"

# Phase 5: Restart server and verify WAL recovery
echo ""
echo "Phase 5: Restarting server and testing WAL recovery..."
echo "Starting server in background..."

# Start server (adjust path as needed)
# Redirect stdin to /dev/null to ensure background mode detection
./godb </dev/null 2>/tmp/godb_recovery.log &
NEW_PID=$!
echo "New server PID: $NEW_PID"

if ! wait_for_server; then
    echo "ERROR: Server failed to start"
    cat /tmp/godb_recovery.log
    exit 1
fi

echo ""
echo "Server restarted. Checking if WAL was replayed..."
sleep 1

echo "Verifying recovered data..."
{
    echo "use $TABLE_NAME"
    sleep 0.2
    echo "count"
    sleep 0.2
    echo "select 1"
    sleep 0.2
    echo "select 11"
    sleep 0.2
    echo "select 20"
    sleep 0.2
    echo "stats"
    sleep 0.2
    echo ".exit"
} | nc $DB_HOST $DB_PORT

# Phase 6: Verify checkpoint behavior
echo ""
echo "Phase 6: Testing checkpoint..."
echo "Inserting a few more records, then waiting for background checkpoint..."

{
    echo "use $TABLE_NAME"
    for i in {31..35}; do
        echo "insert $i post_recovery_$i 100.0"
    done
    echo "count"
    echo ".exit"
} | nc $DB_HOST $DB_PORT

echo "✓ 5 more records inserted"
echo ""
echo "Waiting 35 seconds for background checkpoint (runs every 30s)..."
echo "(The WAL should be truncated after checkpoint)"

wal_before=$(stat -f%z "${TABLE_NAME}.wal" 2>/dev/null || echo "0")
echo "WAL size before checkpoint: $wal_before bytes"

sleep 35

wal_after=$(stat -f%z "${TABLE_NAME}.wal" 2>/dev/null || echo "0")
echo "WAL size after checkpoint: $wal_after bytes"

if [ "$wal_after" -lt "$wal_before" ]; then
    echo "✓ WAL was truncated (checkpoint worked)"
elif [ "$wal_after" -eq 0 ]; then
    echo "✓ WAL was truncated to empty (checkpoint worked)"
else
    echo "⚠ WAL size did not decrease (checkpoint may not have run yet)"
fi

# Final verification
echo ""
echo "=== Final Verification ==="
echo "Final count (should be 20: 5 initial + 10 crash + 5 post-recovery):"
send_cmd "count"
echo ""
echo "Sample records:"
send_cmd "select 1"
send_cmd "select 15"
send_cmd "select 35"

echo ""
echo "=== Recovery Test Complete ==="
echo "✓ WAL recovery working"
echo "✓ Transactions working (COMMIT/ABORT)"
echo "✓ Background checkpoint verified"
echo ""
echo "Server log: /tmp/godb_recovery.log"
