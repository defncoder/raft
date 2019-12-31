#! /bin/bash

lein run -- -i 0 deployment.edn > log0.txt 2>&1 &
lein run -- -i 1 deployment.edn > log1.txt 2>&1 &
lein run -- -i 2 deployment.edn > log2.txt 2>&1 &
lein run -- -i 3 deployment.edn > log3.txt 2>&1 &
lein run -- -i 4 deployment.edn > log4.txt 2>&1 &
