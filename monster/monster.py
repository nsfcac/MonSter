# -*- coding: utf-8 -*-

import json
import sys
sys.path.append('../')

from bmcapi.fetch_bmc import fetch_bmc
from ugeapi.fetch_uge import fetch_uge
from sharings.utils import parse_config, check_config


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

        # Fetch BMC and uge data points
        bmc_datapoints = fetch_bmc(bmc_config)
        uge_datapoints = fetch_uge(uge_config)

        # Aggregate data points
        all_datapoints = bmc_datapoints + uge_datapoints["datapoints"]
        print(f"All datapoints length: {len(all_datapoints)}")

        all_jobspoints = uge_datapoints["jobspoints"]
        print(f"All jobspoints length: {len(all_jobspoints)}")
        
        return
    except Exception as err:
        print(err)
    return


if __name__ == '__main__':
    main()