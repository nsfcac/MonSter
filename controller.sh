#!/usr/bin/env bash

readonly sourceFile="$PWD/env/bin/activate"
echo ${sourceFile}
source ${sourceFile}
python -V
# python $PWD/monster.py

# */1 * * * * /home/lijie/MonSter/controller.sh