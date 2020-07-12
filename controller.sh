#!/usr/bin/env bash

readonly sourceFile="/home/monster/MonSter/env/bin/activate"

source ${sourceFile}

python /home/monster/MonSter/monster.py

# */1 * * * * /home/monster/MonSter/controller.sh