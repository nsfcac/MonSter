# -*- coding: utf-8 -*-
# This function is for calculating the job finish time by comparing the current
# job list with the previous job list

import json
import time
import logging
import schedule
from influxdb import InfluxDBClient
import os
import sys
sys.path.append(os.getcwd())

from ugeapi.fetch_uge import fetch
from sharings.utils import parse_config, check_config
from sharings.JobRequests import JobRequests

path = os.getcwd()
# logging_path = path + '/MonSter/calft.log'
logging_path = path + '/calft.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

prev_joblist = []


def main():
    # Read configuration file
    # config_path = path + '/MonSter/config.yml'
    config_path = path + '/config.yml'
    config = parse_config(config_path)

    # Check sanity
    if not check_config(config):
        return
    try:
        uge_config = config['uge']
        influx_config = config['influxdb']

        # Initialize influxdb
        host = influx_config["host"]
        port = influx_config["port"]
        dbname = influx_config["database"]
        influx_client = InfluxDBClient(host=host, port=port, database=dbname)

        # Monitoring frequency
        freq = config["frequency"]
        
        schedule.every(freq).seconds.do(update_ft, client, uge_config)

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


def update_ft(client:object, uge_config: object) -> None:
    """
    Find finished jobs
    """
    global prev_joblist
    curr_joblist = []
    updated_points = []
    try:
        # Get finish jobs
        fin_jobs = []
        curr_joblist = fetch_jobs(uge_config)
        for job_id in prev_joblist:
            if job_id not in curr_joblist:
                fin_jobs.append(job_id)
        
        # Generate SQLs for quering the JobsInfo
        sqls = generate_sqls(fin_jobs)

        # Query JobsInfo
        job_data = query_influx(sqls, client)

        print(json.dumps(job_data, indent = 4))

    except Exception as err:
        logging.error(f"Update finish time error : {err}")


def query_influx(sqls: list, client: object) -> list:
    """
    Use JobRequests to query urls
    """
    data = []
    try:

        request = JobRequests(client)
        data = request.bulk_fetch(sqls)

    except Exception as err:
        logging.error(f"query_jobdata : query_influx : {err}")
    return data


def generate_sqls(job_ids: list) -> list:
    """
    Generate sqls from accroding to the job_ids
    """
    sqls = []
    try:
        for job_id in job_ids:
            sql = "SELECT * FROM JobsInfo WHERE JobId='" + job_id + "'"
            sqls.append(sql)
    except Exception as err:
        logging.error(f"query_jobdata : generate_sqls: cannot generate sql strings: {err}")

    return sqls


def fetch_jobs(uge_config: dict) -> list:
    """
    fetch UGE metrics from UGE API. 
    Examples of using UGE API:
    curl http://129.118.104.35:8182/jobs | python -m json.tool
    """
    job_list = []
    try:
        api = uge_config["api"]
        job_list_url = f"http://{api['hostname']}:{api['port']}{api['job_list']}"

        # Fetch UGE metrics from urls
        job_list = fetch(uge_config, job_list_url)

        return job_list
    except Exception as err:
        logging.error(f"Fetch job list error : {err}")

if __name__ == '__main__':
    main()