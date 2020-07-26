#! /bin/bash

SERVER_INDEX=$*

for idx in {1..10}
do
    curl http://localhost:100${SERVER_INDEX}0/test-logs?num-logs=200 > /dev/null 2>&1 &
done
