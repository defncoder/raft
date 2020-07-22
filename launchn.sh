#! /bin/bash

for idx in $*
do
    if [ -f pid${idx}.txt ]
    then
       kill -9 `cat pid${idx}.txt`
       rm -f pid${idx}.txt
    fi

    # java -XX:+UnlockExperimentalVMOptions -XX:+UseShenandoahGC -Xmx300M -Xlog:gc -jar target/uberjar/raft-0.0.1-standalone.jar -i ${idx} deployment.edn >> log${idx}.txt 2>&1 &
    # java -XX:+UnlockExperimentalVMOptions -XX:+UseZGC -Xmx400M -Xlog:gc -jar target/uberjar/raft-0.0.1-standalone.jar -i ${idx} deployment.edn >> log${idx}.txt 2>&1 &

    # java -XX:+UnlockExperimentalVMOptions -XX:+UseZGC -Xmx400M -jar target/uberjar/raft-0.0.1-standalone.jar -i ${idx} deployment.edn >> log${idx}.txt 2>&1 &

    lein trampoline run -- -i ${idx} deployment.edn >> log${idx}.txt 2>&1 &
    echo $! > pid${idx}.txt
done    
