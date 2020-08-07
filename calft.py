# -*- coding: utf-8 -*-
# This function is for calculating the job finish time by comparing the current
# job list with the previous job list

import json
import time
import logging
import schedule
import multiprocessing
from itertools import repeat
from influxdb import InfluxDBClient
import os
import sys
sys.path.append(os.getcwd())

from ugeapi.fetch_uge import fetch
from sharings.utils import parse_config, check_config
from sharings.JobRequests import JobRequests

path = os.getcwd()
logging_path = path + '/MonSter/calft.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

prev_joblist = []


def main():
    # Read configuration file
    config_path = path + '/MonSter/config.yml'
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

        update_ft(influx_client, uge_config)
        # Monitoring frequency
        freq = config["frequency"]
        
        schedule.every(freq).seconds.do(update_ft, influx_client, uge_config)

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
        
        finish_time = int(time.time() * 1000000000)

        # Generate SQLs for quering the JobsInfo
        sqls = generate_sqls(fin_jobs)

        # Query JobsInfo
        jobs_data = query_influx(sqls, client)

        # Update JobsInfo
        with multiprocessing.Pool() as pool:
            update_jobs_args = zip(jobs_data, repeat(finish_time))
            updated_jobs_data = pool.starmap(update_jobs, update_jobs_args) 

        # print(json.dumps(updated_jobs_data, indent = 4))

        # Write updated job data points
        if updated_jobs_data:
            client.write_points(updated_jobs_data)

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


def update_jobs(job_data: dict, finish_time: int) -> list:
    """
    update finish time of the job
    """
    try:
        job_id = job_data["job"]
        values = job_data["values"]
        if values:
            datapoint = {
                "measurement": "JobsInfo",
                "tags": {
                    "JobId": job_id
                },
                "time": 0,
                "fields": {
                    "StartTime": values["StartTime"],
                    "SubmitTime": values["SubmitTime"],
                    "FinishTime": finish_time,
                    "JobName": values["JobName"],
                    "User": values["User"],
                    "TotalNodes": values["TotalNodes"],
                    "CPUCores": values["CPUCores"],
                    "NodeList":values["NodeList"]
                }
            }
            return datapoint
    except Exception as err:
        logging.error(f"Update job info error : {err}")
    return


if __name__ == '__main__':
    main()