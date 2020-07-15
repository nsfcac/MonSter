# -*- coding: utf-8 -*-

import json
import time
import logging
import threading
from influxdb import InfluxDBClient
import os
import sys
sys.path.append(os.getcwd())

from glancesapi.fetch_glances import fetch_glances
from sharings.utils import parse_config, check_config

path = os.getcwd()
logging_path = path + '/monster.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def main():
    # Read configuration file
    config_path = path + '/config.yml'
    config = parse_config(config_path)

    # Check sanity
    if not check_config(config):
        return
    try:
        # Add decision to trigger corresponding monitoring modules
        glances_config = config['glances']
        influx_config = config['influxdb']

        # Initialize influxdb
        host = influx_config["host"]
        port = influx_config["port"]
        dbname = influx_config["database"]
        influx_client = InfluxDBClient(host=host, port=port, database=dbname)

        # Fetch data points
        all_datapoints = fetch_datapoints(glances_config)

        # print(json.dumps(all_datapoints, indent=4))
        # Write data points
        influx_client.write_points(all_datapoints)
        
        return
    except Exception as err:
        logging.error(f"main error : {err}")
    return


def fetch_datapoints(glances_config: dict) -> list:
    """
    Fetch and concatenate BMC data points, UGE data points, 
    and estimate job finish time
    """
    all_datapoints = []
    job_datapoints = []
    try:
        # Fetch Glances datapoints
        glances_datapoints = fetch_glances(glances_config)
        
        all_datapoints.extend(glances_datapoints)

    except Exception as err:
        logging.error(f"fetch_datapoints error : {err}")

    return all_datapoints

if __name__ == '__main__':
    main()