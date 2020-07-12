#!/usr/bin/env bash

readonly sourceFile="$PWD/MonSter/env/bin/activate"
source ${sourceFile}

python $PWD/MonSter/monster.py