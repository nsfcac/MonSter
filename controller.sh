#!/usr/bin/env bash

readonly sourceFile="$PWD/env/bin/activate"

source ${sourceFile}

python $PWD/slurmapi/fetch_slurm.py
python $PWD/bmcapi/fetch_bmc_idrac9.py

# Submit cron job by:
# crontab -e
# */1 * * * * /home/monster/MonSter/controller.sh
# Show cron job
# crontab -l

# */1 * * * * /home/monster/MonSter/controller.sh
# * */2 * * * /home/monster/MonSter/cleanlog.sh