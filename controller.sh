#!/usr/bin/env bash

readonly sourceFile="$PWD/MonSter/env/bin/activate"

source ${sourceFile}

python $PWD/MonSter/monster.py

# Submit cron job by:
# crontab -e
# */1 * * * * /home/monster/MonSter/controller.sh
# Show cron job
# crontab -l

# */1 * * * * /home/monster/MonSter/controller.sh
# * */2 * * * /home/monster/MonSter/cleanlog.sh