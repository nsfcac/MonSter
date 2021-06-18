#! /bin/bash

if [ $# == 0 ]
  then
    echo "Please specify a argument: 0 to start sse_queue_status, other value to stop sse_queue_status."
    exit 0
fi

if [ $1 == 0 ]
then
    readonly sourceFile="../env/bin/activate"
    source ${sourceFile}
    echo "Start sse_queue_status ..."
    nohup python ./sse_queue_status.py >> ./sse_queue_status.log &
else
    echo "Stop sse_queue_status ..."
    kill -9 `ps -ef | grep sse_queue_status.py | grep -v grep | awk '{print $2}'`
fi
