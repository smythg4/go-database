#!/bin/bash
# Test free page list reuse after chaos test
# Run this AFTER test_chaos.sh to verify freed pages are being reused

echo "=== Free Page List Reuse Test ==="
echo ""
echo "This test verifies that pages freed during merges are reused on new inserts"
echo ""

# Get initial stats
echo "Checking initial state..."
INITIAL_STATS=$(
    echo "use chaos"
    sleep 0.2
    echo "stats"
    sleep 0.2
    echo ".exit"
) | nc localhost 42069

echo "$INITIAL_STATS"
echo ""

# Extract NextPageID from stats (format: "NextPageID: 40")
INITIAL_NEXT=$(echo "$INITIAL_STATS" | grep -o 'NextPageID: [0-9]*' | grep -o '[0-9]*')
echo "Initial NextPageID: $INITIAL_NEXT"
echo ""

# Insert 500 new records (would normally require ~10 new pages)
echo "Inserting 500 new records (3000-3500)..."
echo "Without free list, this would grow NextPageID by ~10 pages"
echo ""

(
    echo "use chaos"
    for i in {3000..3500}; do
        echo "insert $i data_$i 99.9"
    done
    sleep 0.5
    echo ".exit"
) | nc localhost 42069 > /tmp/freelist_insert.log 2>&1

echo "Inserts complete. Checking final state..."
echo ""

# Get final stats
FINAL_STATS=$(
    echo "use chaos"
    sleep 0.2
    echo "count"
    sleep 0.2
    echo "stats"
    sleep 0.2
    echo ".exit"
) | nc localhost 42069

echo "$FINAL_STATS"
echo ""

# Extract final NextPageID
FINAL_NEXT=$(echo "$FINAL_STATS" | grep -o 'NextPageID: [0-9]*' | grep -o '[0-9]*')
echo "Final NextPageID: $FINAL_NEXT"

# Calculate growth
GROWTH=$((FINAL_NEXT - INITIAL_NEXT))
echo "NextPageID growth: $GROWTH pages"
echo ""

# Interpret results
if [ "$GROWTH" -lt 5 ]; then
    echo "✓ EXCELLENT: Free list is working! Growth < 5 pages (expected ~10 without reuse)"
elif [ "$GROWTH" -lt 8 ]; then
    echo "✓ GOOD: Free list is working! Growth < 8 pages (expected ~10 without reuse)"
elif [ "$GROWTH" -lt 11 ]; then
    echo "⚠ MARGINAL: Growth ~$GROWTH pages (expected ~10 without reuse)"
else
    echo "✗ PROBLEM: Growth > 10 pages suggests free list not reusing pages"
fi

echo ""
echo "Database file size:"
ls -lh chaos.db 2>/dev/null || echo "chaos.db not found"

echo ""
echo "Check /tmp/freelist_insert.log for any errors during insert"
