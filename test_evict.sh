#!/bin/bash

echo "Running first 100 inserts to trigger evictions..."
for i in {1..100}; do
  echo -e "use chaos\ninsert $i data_$i $i.5" | nc localhost 42069 > /dev/null
done

echo "Done with inserts. Checking stats..."
echo -e "use chaos\nstats\n.exit" | nc localhost 42069
