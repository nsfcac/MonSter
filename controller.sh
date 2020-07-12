#!/usr/bin/env bash

readonly sourceFile="/home/lijie/MonSter/env/bin/activate"

source ${sourceFile}

python /home/lijie/MonSter/monster.py

# */1 * * * * /home/lijie/MonSter/controller.sh