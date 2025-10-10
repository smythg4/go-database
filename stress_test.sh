  #!/bin/bash
  # stress_test.sh

  {
      echo "drop stress"
      sleep 0.2
      echo "create stress id:int data:string value:float"
      echo "use stress"

      for i in {1..10000}; do
          value=$(echo "scale=2; $i * 3.14" | bc)
          echo "insert $i record_$i $value"
      done

      echo "select 50"
      echo "select 100"
      echo "select 149"
      echo "select"
      echo "stats"
      echo ".exit"

  } | nc localhost 42069
