  #!/bin/bash

  # Function to run commands in parallel
  client1() {
      (
          sleep 0.5
          echo "use floaters"
          sleep 0.1
          echo "insert 10 alice 95.5"
          sleep 0.1
          echo "insert 11 bob 87.3"
          sleep 0.1
          echo "insert 12 charlie 92.1"
          sleep 0.5
          echo "select"
      ) | nc localhost 42069
  }

  client2() {
      (
          sleep 0.5
          echo "use floaters"
          sleep 0.1
          echo "insert 13 diana 88.9"
          sleep 0.1
          echo "insert 14 eve 91.2"
          sleep 0.1
          echo "insert 15 frank 89.7"
          sleep 0.5
          echo "select"
      ) | nc localhost 42069
  }

  client3() {
      (
          sleep 0.5
          echo "use floaters"
          sleep 0.1
          echo "insert 16 grace 94.3"
          sleep 0.1
          echo "insert 17 henry 86.5"
          sleep 0.5
          echo "select"
      ) | nc localhost 42069
  }

  # Run all clients in parallel
  client1 &
  client2 &
  client3 &

  # Wait for all background jobs
  wait

  echo "All clients finished!"
