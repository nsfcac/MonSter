# -*- coding: utf-8 -*-
"""
This module calls sacct in Slurm to obtain detailed accouting information about
individual jobs or job steps. This module can only be used in Python 3.5 or above.

Jie Li (jie.li@ttu.edu)
"""
import json
import subprocess


def main():
    # Job data format, should be configurable
    format = ["partition", "nodelist", "group", "user", "jobname", "jobid", \
              "submit","start","end","exitcode","cputimeraw","tresusageintot", \
              "tresusageouttot","maxvmsize","alloccpus","ntasks","cluster",\
              "timelimitraw","reqmem"]
    
    # The command used in command line
    command = ["sacct --format=" + ",".join(format) + " -p"]

    # Get strings from command line
    rtn_str = subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
    
    # Split strings by line, and discard the first line that indicates the metrics name
    rtn_str_arr = rtn_str.splitlines()[1:]

    # Parallel process the accouting information

    for job_str in rtn_str_arr:
        processed = process_str(format, job_str)
        print(json.dumps(processed, indent=4))
    return

def process_str(format: list, job_str: str) -> dict:
    """
    Process the job string, and generate the job data in json format
    """
    job_data = {}
    job_str_arr = job_str.split("|")
    
    for i in range(len(format)):
        job_data.update({
            format[i]: job_str_arr[i]
        })

    return job_data

if __name__ == '__main__':
    main()