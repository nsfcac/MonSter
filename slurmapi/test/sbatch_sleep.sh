#!/bin/bash
#
#SBATCH --job-name=test
#SBATCH --output=sleep_res.txt
#
#SBATCH --ntasks=1
#SBATCH --time=1:00
#SBATCH --mem-per-cpu=100

srun hostname
srun sleep 30