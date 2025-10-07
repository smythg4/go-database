#!/bin/bash

# Stress test with all data types: int, string, bool, float, date
# Tests concurrent inserts with 500 total records across 5 clients

# Clean up old test table if it exists
echo "Cleaning up old test database..."
(
    echo "drop stresstest"
    sleep 0.2
    echo ".exit"
) | nc localhost 42069 2>/dev/null

echo "Creating stress test table with all data types..."
(
    echo "create stresstest id:int name:string active:bool score:float created:date"
    sleep 0.5
    echo ".exit"
) | nc localhost 42069

# Wait for table creation to complete
sleep 1

# Verify table was created
if [ ! -f stresstest.db ]; then
    echo "ERROR: Failed to create stresstest.db"
    exit 1
fi

echo "Table created successfully, starting concurrent clients..."
sleep 1

# Client 1: IDs 1-1000
client1() {
    echo "Client 1 starting (IDs 1-1000)..."
    (
        echo "use stresstest"
        sleep 0.1
        for i in {1..1000}; do
            # Alternate between active true/false
            active=$((i % 2 == 0))
            # Random-ish scores
            score=$(echo "scale=2; 50 + ($i % 50)" | bc)
            # Dates in 2020-2024 range
            year=$((2020 + (i % 5)))
            month=$(printf "%02d" $((1 + (i % 12))))
            day=$(printf "%02d" $((1 + (i % 28))))
            date="${year}-${month}-${day}"

            echo "insert $i user_$i $active $score $date"
            sleep 0.01
        done
        echo "select"
    ) | nc localhost 42069 > /tmp/client1.log 2>&1
    echo "Client 1 finished"
}

# Client 2: IDs 1001-2000
client2() {
    echo "Client 2 starting (IDs 1001-2000)..."
    (
        echo "use stresstest"
        sleep 0.1
        for i in {1001..2000}; do
            active=$((i % 3 == 0))
            score=$(echo "scale=2; 60 + ($i % 40)" | bc)
            year=$((2020 + (i % 5)))
            month=$(printf "%02d" $((1 + (i % 12))))
            day=$(printf "%02d" $((1 + (i % 28))))
            date="${year}-${month}-${day}"

            echo "insert $i user_$i $active $score $date"
            sleep 0.01
        done
    ) | nc localhost 42069 > /tmp/client2.log 2>&1
    echo "Client 2 finished"
}

# Client 3: IDs 2001-3000
client3() {
    echo "Client 3 starting (IDs 2001-3000)..."
    (
        echo "use stresstest"
        sleep 0.1
        for i in {2001..3000}; do
            active=$((i % 4 == 0))
            score=$(echo "scale=2; 70 + ($i % 30)" | bc)
            year=$((2020 + (i % 5)))
            month=$(printf "%02d" $((1 + (i % 12))))
            day=$(printf "%02d" $((1 + (i % 28))))
            date="${year}-${month}-${day}"

            echo "insert $i user_$i $active $score $date"
            sleep 0.01
        done
    ) | nc localhost 42069 > /tmp/client3.log 2>&1
    echo "Client 3 finished"
}

# Client 4: IDs 3001-4000
client4() {
    echo "Client 4 starting (IDs 3001-4000)..."
    (
        echo "use stresstest"
        sleep 0.1
        for i in {3001..4000}; do
            active=$((i % 5 == 0))
            score=$(echo "scale=2; 80 + ($i % 20)" | bc)
            year=$((2020 + (i % 5)))
            month=$(printf "%02d" $((1 + (i % 12))))
            day=$(printf "%02d" $((1 + (i % 28))))
            date="${year}-${month}-${day}"

            echo "insert $i user_$i $active $score $date"
            sleep 0.01
        done
    ) | nc localhost 42069 > /tmp/client4.log 2>&1
    echo "Client 4 finished"
}

# Client 5: IDs 4001-5000
client5() {
    echo "Client 5 starting (IDs 4001-5000)..."
    (
        echo "use stresstest"
        sleep 0.1
        for i in {4001..5000}; do
            active=$((i % 6 == 0))
            score=$(echo "scale=2; 90 + ($i % 10)" | bc)
            year=$((2020 + (i % 5)))
            month=$(printf "%02d" $((1 + (i % 12))))
            day=$(printf "%02d" $((1 + (i % 28))))
            date="${year}-${month}-${day}"

            echo "insert $i user_$i $active $score $date"
            sleep 0.01
        done
    ) | nc localhost 42069 > /tmp/client5.log 2>&1
    echo "Client 5 finished"
}

# Run all clients in parallel
echo "Starting 5 concurrent clients..."
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
echo "All clients finished!"
echo ""

# Verify results
echo "Verifying results..."
(
    echo "use stresstest"
    sleep 0.2
    echo "stats"
    sleep 0.2
    echo "select 1"
    sleep 0.2
    echo "select 250"
    sleep 0.2
    echo "select 500"
    sleep 0.2
    echo ".exit"
) | nc localhost 42069

echo ""
echo "Check /tmp/client*.log for individual client output"
echo "Database file: ls -lh stresstest.db"
ls -lh stresstest.db 2>/dev/null || echo "stresstest.db not found"
