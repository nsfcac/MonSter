#! /bin/bash

if [ $# == 0 ]
  then
    echo "Please specify a argument: 0 to start MonSTer, other value to stop MonSTer."
    exit 0
fi

if [ $1 == 0 ]
then
    echo > previous_jobs.json
    echo "Start MonSTer cron job..."
    crontab /home/monster/MonSter/cornfile
    crontab -l
else
    echo "Stop MonSTer cron job..."
    crontab -l > /home/monster/MonSter/cornfile
    crontab -r
fi