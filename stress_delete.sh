  #!/bin/bash
  # stress_test.sh

  {
      #echo "create stress id:int data:string value:float"
      echo "use stress"

      for i in {1..145}; do
	#key=$((i*2))
          echo "delete $i"
      done

      echo "select 50"
      echo "select 100"
      echo "select 149"
      echo "select"
      echo "stats"
  } | nc localhost 42069
