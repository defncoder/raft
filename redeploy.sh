#! /bin/bash

# lein uberjar
rm -f  ~/data/raft/*.db
# rm -f log*.txt
./launchn.sh 0 1 2 3 4
echo "New server deployment complete..."
