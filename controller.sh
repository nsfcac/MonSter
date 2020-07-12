#!/usr/bin/env bash

readonly sourceFile="$PWD/MonSter/env/bin/activate"

source ${sourceFile}

python $PWD/MonSter/monster.py

# Submit cron job by:
# crontab -e
# */1 * * * * /home/lijie/MonSter/controller.sh
# Show cron job
# crontab -l