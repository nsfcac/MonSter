# -*- coding: utf-8 -*-

import json
import time
import logging
import threading
from influxdb import InfluxDBClient
import sys
sys.path.append('/home/monster/TestMonSter/MonSter/')

from bmcapi.fetch_bmc import fetch_bmc
from ugeapi.fetch_uge import fetch_uge
from sharings.utils import parse_config, check_config

logging.basicConfig(
    level=logging.ERROR,
    filename='monster.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def main():
    # Read configuration file
    config = parse_config('./config.yml')

    # Check sanity
    if not check_config(config):
        return
    try:
        bmc_config = config['bmc']
        uge_config = config['uge']
        influx_config = config['influxdb']

        # Initialize influxdb
        host = influx_config["host"]
        port = influx_config["port"]
        dbname = influx_config["database"]
        influx_client = InfluxDBClient(host=host, port=port, database=dbname)

        # Fetch data points
        all_datapoints = fetch_datapoints(bmc_config, uge_config)

        # Write data points
        influx_client.write_points(all_datapoints)
        
        return
    except Exception as err:
        logging.error(f"main error : {err}")
    return


def fetch_datapoints(bmc_config: dict, uge_config: dict) -> list:
    """
    Fetch and concatenate BMC data points, UGE data points, 
    and estimate job finish time
    """
    all_datapoints = []
    job_datapoints = []
    try:
        # Fetch BMC datapoints and uge metrics
        bmc_datapoints = fetch_bmc(bmc_config)
        uge_metrics = fetch_uge(uge_config)

        # UGE metrics
        uge_datapoints = uge_metrics["datapoints"]
        timestamp = uge_metrics["timestamp"]
        curr_jobs_info = uge_metrics["jobs_info"]

        job_datapoints = list(curr_jobs_info.values())

        # Contatenate all data points
        all_datapoints.extend(bmc_datapoints)
        all_datapoints.extend(uge_datapoints)
        all_datapoints.extend(job_datapoints)

        # print(f"BMC data points length: {len(bmc_datapoints)}")
        # print(f"UGE data points length: {len(uge_datapoints)}")
        # print(f"Job data points length: {len(job_datapoints)}")
    except Exception as err:
        logging.error(f"fetch_datapoints error : {err}")

    return all_datapoints

if __name__ == '__main__':
    main()