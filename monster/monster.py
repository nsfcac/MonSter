# -*- coding: utf-8 -*-

import json
import sys
sys.path.append('../')

from bmcapi.fetch_bmc import fetch_bmc
from ugeapi.fetch_uge import fetch_uge
from sharings.utils import parse_config, check_config


# Temporarily store previous job list; compared with the current job list and
# estimate the Finish time of the job; If it is in the previous list but not in
# the current job list, its finish time should before the current time stamp
prev_joblist = []


def main():
    # Read configuration file
    config = parse_config('./config.yml')
    # Check sanity
    if not check_config(config):
        return
    try:
        all_datapoints = []

        bmc_config = config['bmc']
        uge_config = config['uge']
        
        all_datapoints = fetch_datapoints(bmc_config, uge_config)

        return
    except Exception as err:
        print(err)
    return


def fetch_datapoints(bmc_config: dict, uge_config: dict) -> list:
    """
    Fetch and concatenate BMC data points, UGE data points, 
    and estimate job finish time
    """
    all_datapoints = []
    global prev_joblist
    # Fetch BMC datapoints and uge metrics
    bmc_datapoints = fetch_bmc(bmc_config)
    uge_metrics = fetch_uge(uge_config)

    # UGE metrics
    uge_datapoints = uge_metrics["datapoints"]
    timestamp = uge_metrics["timestamp"]
    jobs_info = uge_metrics["jobs_info"]

    # Current job list
    curr_joblist = list(jobs_info.keys())
    
    # Compare the current job list with the previous job list and update finish time
    for job in prev_joblist:
        if job not in curr_joblist:
            jobs_info[job]["fields"].update({
                "FinishTime": timestamp
            })
    job_datapoints = list(jobs_info.values())

    # Update previous job list
    prev_joblist = curr_joblist

    # Contatenate all data points
    all_datapoints = bmc_datapoints + uge_datapoints + job_datapoints

    print(f"BMC data points length: {len(bmc_datapoints)}")
    print(f"UGE data points length: {len(uge_datapoints)}")
    print(f"Job data points length: {len(job_datapoints)}")
    print(json.dumps(all_datapoints, indent=4))

    return all_datapoints

if __name__ == '__main__':
    main()