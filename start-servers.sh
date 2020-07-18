#! /bin/bash

java -jar target/uberjar/raft-0.0.1-standalone.jar -i 0 deployment.edn > log0.txt 2>&1 &
java -jar target/uberjar/raft-0.0.1-standalone.jar -i 1 deployment.edn > log1.txt 2>&1 &
java -jar target/uberjar/raft-0.0.1-standalone.jar -i 2 deployment.edn > log2.txt 2>&1 &
java -jar target/uberjar/raft-0.0.1-standalone.jar -i 3 deployment.edn > log3.txt 2>&1 &
java -jar target/uberjar/raft-0.0.1-standalone.jar -i 4 deployment.edn > log4.txt 2>&1 &
