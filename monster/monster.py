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
        
        return
    except Exception as err:
        print(err)
    return


def fetch_datapoints(bmc_config: dict, uge_config: dict) -> list:
    # Fetch BMC and uge data points
    bmc_datapoints = fetch_bmc(bmc_config)
    uge_datapoints = fetch_uge(uge_config)

    # Aggregate data points
    all_datapoints = bmc_datapoints + uge_datapoints["datapoints"]

    all_jobspoints = uge_datapoints["jobspoints"]
    curr_joblist = get_joblist(all_jobspoints)

    # 

def get_joblist(jobspoints: list) -> list:
    """
    Get job list from the jobs points
    """
    joblist = [ job["tags"]["JobId"] for job in jobspoints ]
    return joblist


if __name__ == '__main__':
    main()