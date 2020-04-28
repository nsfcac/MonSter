#!/usr/bin python3.6

import json
import time
from influxdb import InfluxDBClient
import logging
import schedule
from helper import parse_config, check_config, get_hostlist
from ugeapi.fetch_uge import fetch_uge
from bmcapi.fetch_bmc import fetch_bmc
import threading

logging.basicConfig(
    level=logging.ERROR,
    filename='monster.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

prev_joblist = []


def main():
    config = parse_config()

    # Check sanity
    if not check_config(config):
        print("Error: Bad configuration!")
        return
    
    try:
        global prev_joblist

        # Initialize influxdb
        host = config["influxdb"]["host"]
        port = config["influxdb"]["port"]
        dbname = config["influxdb"]["database"]
        hostlist = get_hostlist(config["hostlistdir"])
        client = InfluxDBClient(host=host, port=port, database=dbname)

        # Monitoring frequency
        freq = config["frequency"]
        
        schedule.every(freq).seconds.do(run_write_db, write_db, client, config, hostlist)

        while True:
            try:
                schedule.run_pending()
                time.sleep(schedule.idle_seconds())
            except KeyboardInterrupt:
                break   

    except Exception as err:
        print(err)
    return 


def run_write_db(write_db, client, config, hostlist):
    job_thread = threading.Thread(target=write_db, args=(client, config, hostlist))
    job_thread.start()


def write_db(client: object, config: object, hostlist: list) -> None:    
    global prev_joblist
    all_points = []
    curr_joblist = []
    # print(json.dumps(prev_joblist, indent=4))

    try:
        # Fetch UGE information
        uge_info = fetch_uge(config["uge"])

        uge_host_points = uge_info["all_host_points"]
        all_points.extend(uge_host_points)

        uge_job_points = uge_info["all_job_points"]
        uge_epoch_time = uge_info["epoch_time"]
        
        for job_point in uge_job_points:
            job_id = job_point["tags"]["JobId"]
            curr_joblist.append(job_id)
            if not fetch_job(client, job_id):
                all_points.append(job_point)

        # Compare current job list with previous job list and update finish time
        for job_id in prev_joblist:
            # This job is finished 
            if job_id not in curr_joblist:
                updated_job = update_job(client, job_id, uge_epoch_time)
                if updated_job:
                    all_points.append(updated_job)

        # Update previous jobs
        prev_joblist = curr_joblist

        # Fetch BMC information
        bmc_points = fetch_bmc(config["redfish"], hostlist)
        all_points.extend(bmc_points)

        # Write points into influxdb
        client.write_points(all_points)
        # print(json.dumps(all_points, indent=4))

    except:
        logging.error("Cannot write data points to influxDB")


def fetch_job(client: object, job_id: str) -> object:
    data = {}
    try:
        query_str = "SELECT * FROM JobsInfo WHERE JobId = '" + job_id + "'"
        data = client.query(query_str)
    except:
        # job is not in database
        pass
        
    return data


def update_job(client: object, job_id: str, finishtime: int) -> None:
    updated_job = {}
    history_job = {}
    try:
        job_info = fetch_job(client, job_id).raw
        if job_info:
            for index, item in enumerate(job_info["series"][0]["columns"]):
                history_job[item] = job_info["series"][0]["values"][0][index]
            updated_job = {
                "measurement": "JobsInfo",
                "tags": {
                    "JobId": job_id,
                },
                "time": history_job["time"],
                "fields": {
                    "StartTime": history_job["StartTime"],
                    "SubmitTime": history_job["SubmitTime"],
                    "FinishTime": int(finishtime/1000000000),
                    "JobName": history_job["JobName"],
                    "User": history_job["User"],
                    "TotalNodes": history_job["TotalNodes"],
                    "CPUCores": history_job["CPUCores"],
                    "NodeList": history_job["NodeList"]
                }
            }
    except:
        logging.error("Failed to update job: %s", job_id)

    return updated_job

if __name__ == '__main__':
    main()