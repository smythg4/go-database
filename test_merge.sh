#!/bin/bash

# Test script for merge functionality
# Builds up a tree with many records, then deletes them to trigger merges

DB_PORT=42069
DB_HOST=localhost

echo "=== Building test database ==="

# Function to send commands to database
send_cmd() {
    echo "$1" | nc -N $DB_HOST $DB_PORT 2>/dev/null
    sleep 0.1
}

# Start fresh
echo "Creating table..."
send_cmd "create mergetest id:int name:string age:int"

echo ""
echo "=== Phase 1: Insert 150 records (should cause multiple splits) ==="
for i in {1..150}
do
    name="user_$(printf "%03d" $i)"
    age=$((20 + (i % 50)))
    send_cmd "insert $i $name $age"
    if (( i % 25 == 0 )); then
        echo "Inserted $i records..."
        send_cmd "stats" | tail -1
    fi
done

echo ""
echo "=== Tree stats after 150 inserts ==="
send_cmd "stats"

echo ""
echo "=== Phase 2: Delete every other record (75 deletions) ==="
for i in {2..150}
do
    send_cmd "delete $i*2"
    if (( i*2 % 20 == 0 )); then
        echo "Deleted up to $i*2..."
    fi
done

echo ""
echo "=== Tree stats after deleting events ==="
send_cmd "stats"

echo ""
echo "=== Phase 3: Delete more records to trigger aggressive merging ==="
for i in {1..149}
do
    send_cmd "delete $i*4"
    if (( i*4 % 20 == 1 )); then
        echo "Deleted $i*4..."
    fi
done

echo ""
echo "=== Tree stats after aggressive deletion ==="
send_cmd "stats"

echo ""
echo "=== Phase 4: Count remaining records ==="
result=$(send_cmd "select" | grep -c "|")
echo "Remaining records: $((result - 2))" # subtract header rows

echo ""
echo "=== Final tree structure ==="
send_cmd "stats"

echo ""
echo "=== Test complete! ==="
