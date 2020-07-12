#!/usr/bin/env bash

readonly sourceFile="$PWD/env/bin/activate"

source ${sourceFile}

python $PWD/monster.py

# */1 * * * * /home/monster/MonSter/controller.sh