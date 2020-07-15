#!/usr/bin/env bash

readonly sourceFile="/root/MonSter/env/bin/activate"

source ${sourceFile}

python /root/MonSter/monster.py

# Submit cron job by:
# crontab -e
# * * * * *              /root/MonSter/controller.sh
# * * * * * ( sleep 20 ; /root/MonSter/controller.sh ) 
# Show cron job
# crontab -l