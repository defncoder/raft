#! /bin/bash

function is_process_running {
    ps -p $1 -o command | grep -e 'java.*raft' > /dev/null 2>&1
}

for idx in $*
do
    if [ -f pid${idx}.txt ]
    then
	CUR_PROC_ID=`cat pid${idx}.txt`
	is_process_running ${CUR_PROC_ID}
	if [ $? -eq 0 ]
	then
	    kill -9 ${CUR_PROC_ID} > /dev/null 2>&1
	    rm -f pid${idx}.txt
	fi
    fi
done    
