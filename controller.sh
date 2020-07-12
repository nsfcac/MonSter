#!/usr/bin/env bash

readonly sourceFile="$PWD/MonSter/env/bin/activate"
echo ${sourceFile}
source ${sourceFile}
python -V
python $PWD/MonSter/monster.py

# */1 * * * * /home/lijie/MonSter/controller.sh