# -*- coding: utf-8 -*-
"""
This module calls sacct in Slurm to obtain detailed accouting information about
individual jobs or job steps. This module can only be used in Python 3.5 or above.

Jie Li (jie.li@ttu.edu)
"""
import json
import subprocess
from multiprocessing import Process, Queue


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

    # Generate all job data dict
    for i in rtn_str_arr:
        processed = str_2_json(format, i)
        print(json.dumps(processed, indent=4))
    return


def str_2_json(format: list, job_str: str) -> dict:
    """
    Process the job string, and generate the json format job data corresponding to job id.
    """
    job_dict = {}
    job_data = {}
    job_str_arr = job_str.split("|")
    
    for i in range(len(format)):
        job_data.update({
            format[i]: job_str_arr[i]
        })
    
    # Unfold metrics in treusageintot and tresusageoutot
    if job_data.get("tresusageintot", None):
        unfolded_metrics = unfold(job_data["tresusageintot"])
        job_date.update(unfolded_metrics)
        job_data.pop("tresusageintot")
    
    if job_data.get("tresusageouttot", None):
        unfolded_metrics = unfold(job_data["tresusageouttot"])
        job_date.update(unfolded_metrics)
        job_data.pop("tresusageouttot")
    
    job_dict = {
        job_data["jobid"]: job_data
    }

    return job_dict


def unfold(metric_str: str) -> dict:
    """
    Unfold the metrics under the same metric name(such as tresusageintot, tresusageouttot)
    """
    metric_dict = {}
    for item in metric_str.split(","):
        item_pair = item.split("=")
        metric_dict.update({
            item_pair[0]: item_pair[1]
        })

    return metric_dict


def process_job_dict(rtn_str_arr: arr, job_dict_all: dict) -> dict:
    return

if __name__ == '__main__':
    main()