# -*- coding: utf-8 -*-
"""
This module calls sacct in Slurm to obtain detailed accouting information about
individual jobs or job steps. The can only be used in Python 3.5 or above.

Jie Li (jie.li@ttu.edu)
"""
import subprocess

def main():
    # Job data format, should be configurable
    format = ["partition", "nodelist", "group", "user", "jobname", "jobid", \
              "submit","start","end","exitcode","cputimeraw","tresusageintot", \
              "tresusageouttot","maxvmsize","alloccpus","ntasks","cluster",\
              "timelimitraw","reqmem"]
    command = ["sacct --format=" + ",".join(format) + " -p"]

    # Get strings from command line
    rtn_str = subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
    
    # Split strings by line
    rtn_str_arr = rtn_str.splitlines()

    print(rtn_str_arr)
    return


if __name__ == '__main__':
    main()