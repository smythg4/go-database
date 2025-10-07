#!/bin/bash
# Chaos test: Concurrent inserts AND deletes to stress free page list and merge logic

# Clean up old test table if it exists
echo "Cleaning up old test database..."
(
    echo "drop chaos"
    sleep 0.2
    echo ".exit"
) | nc localhost 42069 2>/dev/null

echo "Creating chaos test table..."
(
    echo "create chaos id:int data:string value:float"
    sleep 0.5
    echo ".exit"
) | nc localhost 42069

# Wait for table creation to complete
sleep 1

# Verify table was created
if [ ! -f chaos.db ]; then
    echo "ERROR: Failed to create chaos.db"
    exit 1
fi

echo "Table created successfully, starting chaos clients..."
sleep 1

# Client 1: Insert 1-500, then delete evens
client1() {
    echo "Client 1 starting (insert 1-500, delete evens)..."
    (
        echo "use chaos"
        sleep 0.1
        # Insert phase
        for i in {1..500}; do
            value=$(echo "scale=2; $i * 1.1" | bc)
            echo "insert $i data_$i $value"
            sleep 0.005
        done
        # Delete phase - delete evens
        for ((i=2; i<=500; i+=2)); do
            echo "delete $i"
            sleep 0.005
        done
    ) | nc localhost 42069 > /tmp/chaos1.log 2>&1
    echo "Client 1 finished"
}

# Client 2: Insert 501-1000, then delete every 3rd
client2() {
    echo "Client 2 starting (insert 501-1000, delete every 3rd)..."
    (
        echo "use chaos"
        sleep 0.1
        # Insert phase
        for i in {501..1000}; do
            value=$(echo "scale=2; $i * 2.2" | bc)
            echo "insert $i data_$i $value"
            sleep 0.005
        done
        # Delete phase - delete every 3rd
        for ((i=501; i<=1000; i+=3)); do
            echo "delete $i"
            sleep 0.005
        done
    ) | nc localhost 42069 > /tmp/chaos2.log 2>&1
    echo "Client 2 finished"
}

# Client 3: Interleaved insert/delete (insert 1001-1500, delete as we go)
client3() {
    echo "Client 3 starting (interleaved insert/delete 1001-1500)..."
    (
        echo "use chaos"
        sleep 0.1
        for i in {1001..1500}; do
            value=$(echo "scale=2; $i * 3.3" | bc)
            echo "insert $i data_$i $value"
            sleep 0.005
            # Delete previous record every 5th insert
            if [ $((i % 5)) -eq 0 ] && [ $i -gt 1001 ]; then
                delete_id=$((i - 5))
                echo "delete $delete_id"
                sleep 0.005
            fi
        done
    ) | nc localhost 42069 > /tmp/chaos3.log 2>&1
    echo "Client 3 finished"
}

# Client 4: Insert 1501-2000, delete odds
client4() {
    echo "Client 4 starting (insert 1501-2000, delete odds)..."
    (
        echo "use chaos"
        sleep 0.1
        # Insert phase
        for i in {1501..2000}; do
            value=$(echo "scale=2; $i * 4.4" | bc)
            echo "insert $i data_$i $value"
            sleep 0.005
        done
        # Delete phase - delete odds
        for ((i=1501; i<=2000; i+=2)); do
            echo "delete $i"
            sleep 0.005
        done
    ) | nc localhost 42069 > /tmp/chaos4.log 2>&1
    echo "Client 4 finished"
}

# Client 5: Insert 2001-2500, then delete half
client5() {
    echo "Client 5 starting (insert 2001-2500, delete half)..."
    (
        echo "use chaos"
        sleep 0.1
        # Insert phase
        for i in {2001..2500}; do
            value=$(echo "scale=2; $i * 5.5" | bc)
            echo "insert $i data_$i $value"
            sleep 0.005
        done
        # Delete phase
        for i in {2001..2250}; do
            echo "delete $i"
            sleep 0.005
        done
    ) | nc localhost 42069 > /tmp/chaos5.log 2>&1
    echo "Client 5 finished"
}

# Run all clients in parallel
echo "Starting 5 chaos clients..."
client1 &
CLIENT1_PID=$!
client2 &
CLIENT2_PID=$!
client3 &
CLIENT3_PID=$!
client4 &
CLIENT4_PID=$!
client5 &
CLIENT5_PID=$!

# Wait for all background jobs
wait $CLIENT1_PID
wait $CLIENT2_PID
wait $CLIENT3_PID
wait $CLIENT4_PID
wait $CLIENT5_PID

echo ""
echo "All chaos clients finished!"
echo ""

# Verify results
echo "Verifying results..."
(
    echo "use chaos"
    sleep 0.2
    echo "count"
    sleep 0.2
    echo "stats"
    sleep 0.2
    echo "select 1"
    sleep 0.2
    echo "select 1001"
    sleep 0.2
    echo "select 2500"
    sleep 0.2
    echo ".exit"
) | nc localhost 42069

echo ""
echo "Check /tmp/chaos*.log for individual client output"
echo "Database file size:"
ls -lh chaos.db 2>/dev/null || echo "chaos.db not found"

echo ""
echo "Expected surviving records: ~1250-1500 (many deleted)"
echo "Free page list should have reused pages from deleted records"
