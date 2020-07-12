# -*- coding: utf-8 -*-

import json
import time
import logging
import threading
import schedule
from influxdb import InfluxDBClient
import sys
sys.path.append('../')

from bmcapi.fetch_bmc import fetch_bmc
from ugeapi.fetch_uge import fetch_uge
from sharings.utils import parse_config, check_config

logging.basicConfig(
    level=logging.ERROR,
    filename='monster.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

# Temporarily store previous job list; compared with the current job list and
# estimate the Finish time of the job; If it is in the previous list but not in
# the current job list, its finish time should before the current time stamp
prev_jobs_info = {}


def main():
    # Read configuration file
    config = parse_config('./config.yml')
    # Check sanity
    if not check_config(config):
        return
    try:
        # Monitoring frequency
        freq = config["frequency"]

        bmc_config = config['bmc']
        uge_config = config['uge']
        influx_config = config['influxdb']

        fetch_datapoints(bmc_config, uge_config)

        # Initialize influxdb
        host = influx_config["host"]
        port = influx_config["port"]
        dbname = influx_config["database"]

        influx_client = InfluxDBClient(host=host, port=port, database=dbname)

        # Schedule run_monster
        schedule.every(freq).seconds.do(run_monster, monster, 
                                        bmc_config, uge_config, influx_client)
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(schedule.idle_seconds())
            except KeyboardInterrupt:
                break   

        return
    except Exception as err:
        logging.error(f"main error : {err}")
    return


def run_monster(monster, bmc_config: dict, uge_config: dict, influx_client: object):
    """
    Create monster threads
    """
    try:
        job_thread = threading.Thread(target=monster, 
                                    args=(bmc_config, uge_config, influx_client))
        job_thread.start()
    except Exception as err:
        logging.error(f"run_monster error : {err}")


def monster(bmc_config: dict, uge_config: dict, influx_client: object) -> None:
    """
    Fetch and write datapoints into influxdb
    """
    try:
        all_datapoints = fetch_datapoints(bmc_config, uge_config)
        influx_client.write_points(all_datapoints)
    except Exception as err:
        logging.error(f"monster error : {err}")
    return


def fetch_datapoints(bmc_config: dict, uge_config: dict) -> list:
    """
    Fetch and concatenate BMC data points, UGE data points, 
    and estimate job finish time
    """
    all_datapoints = []
    job_datapoints = []
    global prev_jobs_info
    try:
        # Fetch BMC datapoints and uge metrics
        bmc_datapoints = fetch_bmc(bmc_config)
        uge_metrics = fetch_uge(uge_config)

        # UGE metrics
        uge_datapoints = uge_metrics["datapoints"]
        timestamp = uge_metrics["timestamp"]
        curr_jobs_info = uge_metrics["jobs_info"]
        
        # Compare the current job list with the previous job list and update finish time
        for job in prev_jobs_info:
            if job not in curr_jobs_info:
                prev_jobs_info[job]["fields"].update({
                    "FinishTime": timestamp
                })
                job_datapoints.append(prev_jobs_info[job])

        job_datapoints.extend(list(curr_jobs_info.values()))

        # Update previous job list
        prev_jobs_info = curr_jobs_info

        # Contatenate all data points
        all_datapoints.extend(bmc_datapoints)
        all_datapoints.extend(uge_datapoints)
        all_datapoints.extend(job_datapoints)

        # print(f"BMC data points length: {len(bmc_datapoints)}")
        # print(f"UGE data points length: {len(uge_datapoints)}")
        # print(f"Job data points length: {len(job_datapoints)}")
        # print(json.dumps(all_datapoints, indent=4))
    except Exception as err:
        logging.error(f"fetch_datapoints error : {err}")

    return all_datapoints

if __name__ == '__main__':
    main()