#! /bin/bash
if [ $# == 0 ]
  then
    echo "Please specify a argument: 0 to start MonSTer, other value to stop MonSTer."
    exit 0
fi

if [ $1 == 0 ]
then
    echo "Start MonSTer ..."
    nohup python3 ./monster.py >> running.log &
else
    echo "Stop MonSTer ..."
    kill -9 `ps -ef | grep monster.py | grep -v grep | awk '{print $2}'`
fi