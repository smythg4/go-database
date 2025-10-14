#!/bin/bash
# Simple CRUD test - sends commands in a single session

DB_PORT=42069
TABLE="simple_test"

echo "=== Simple CRUD Test ==="
echo ""

# Single session with all commands
{
    echo "drop $TABLE"
    echo "create $TABLE id:int name:string value:float"
    echo "use $TABLE"

    # Insert 20 records
    for i in {1..20}; do
        value="$i.5"
        echo "insert $i user_$i $value"
    done

    # Stats
    echo "stats"
    echo "count"

    # Point select
    echo "select 10"

    # Range scan
    echo "select 5 15"

    # Update
    echo "update 10 updated_user 99.9"
    echo "select 10"

    # Delete
    echo "delete 1"
    echo "delete 2"
    echo "delete 3"
    echo "count"

    # Final stats
    echo "stats"

    echo ".exit"
} | nc localhost $DB_PORT

echo ""
echo "=== Test Complete ==="
