#!/bin/bash
#
#SBATCH --job-name=test_mpi
#SBATCH --output=res_mpi.txt
#
#SBATCH --ntasks=4
#SBATCH --time=1:00
#SBATCH --mem-per-cpu=100

srun hello.mpi