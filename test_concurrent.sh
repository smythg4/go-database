#!/bin/bash

# Stress test with all data types: int, string, bool, float, date
# Tests concurrent inserts with 500 total records across 5 clients

echo "Creating stress test table with all data types..."
(
    echo "create stresstest id:int name:string active:bool score:float created:date"
    sleep 0.5
    echo ".exit"
) | nc localhost 42069

sleep 1

# Client 1: IDs 1-100
client1() {
    echo "Client 1 starting (IDs 1-100)..."
    (
        echo "use stresstest"
        sleep 0.1
        for i in {1..100}; do
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

# Client 2: IDs 101-200
client2() {
    echo "Client 2 starting (IDs 101-200)..."
    (
        echo "use stresstest"
        sleep 0.1
        for i in {101..200}; do
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

# Client 3: IDs 201-300
client3() {
    echo "Client 3 starting (IDs 201-300)..."
    (
        echo "use stresstest"
        sleep 0.1
        for i in {201..300}; do
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

# Client 4: IDs 301-400
client4() {
    echo "Client 4 starting (IDs 301-400)..."
    (
        echo "use stresstest"
        sleep 0.1
        for i in {301..400}; do
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

# Client 5: IDs 401-500
client5() {
    echo "Client 5 starting (IDs 401-500)..."
    (
        echo "use stresstest"
        sleep 0.1
        for i in {401..500}; do
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
