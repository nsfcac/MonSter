# -*- coding: utf-8 -*-
"""
This module calls sacct in Slurm to obtain detailed accouting information about
individual jobs or job steps. This module can only be used in Python 3.5 or above.

Job State:
https://slurm.schedmd.com/sacct.html#SECTION_JOB-STATE-CODES

Get squeue: squeue -o "%all"
All available fields: ['ACCOUNT', 'TRES_PER_NODE', 'MIN_CPUS', 'MIN_TMP_DISK', 
'END_TIME', 'FEATURES', 'GROUP', 'OVER_SUBSCRIBE', 'JOBID', 'NAME', 'COMMENT', 
'TIME_LIMIT', 'MIN_MEMORY', 'REQ_NODES', 'COMMAND', 'PRIORITY', 'QOS', 'REASON', 
'', 'ST', 'USER', 'RESERVATION', 'WCKEY', 'EXC_NODES', 'NICE', 'S:C:T', 'JOBID', 
'EXEC_HOST', 'CPUS', 'NODES', 'DEPENDENCY', 'ARRAY_JOB_ID', 'GROUP', 
'SOCKETS_PER_NODE', 'CORES_PER_SOCKET', 'THREADS_PER_CORE', 'ARRAY_TASK_ID', 
'TIME_LEFT', 'TIME', 'NODELIST', 'CONTIGUOUS', 'PARTITION', 'PRIORITY', 
'NODELIST(REASON)', 'START_TIME', 'STATE', 'UID', 'SUBMIT_TIME', 'LICENSES', 
'CORE_SPEC', 'SCHEDNODES', 'WORK_DIR']

Jie Li (jie.li@ttu.edu)
"""
import re
import sys
import csv
import json
import time
import pytz
import datetime
import logging
import psycopg2
import schedule
import subprocess
import hostlist

sys.path.append('../')

from tqdm import tqdm
from pgcopy import CopyManager
from datetime import datetime, timezone
from sharings.utils import parse_config, parse_hostnames, init_tsdb_connection

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%Z'

# 'ARRAY_JOB_ID', 'ARRAY_TASK_ID', 
SELECT_FIELDS = ['JOBID', 'ARRAY_JOB_ID', 'NAME', 'USER', 'STATE', 'PARTITION', 'NODELIST', 'CPUS', 'MIN_MEMORY', 'SUBMIT_TIME', 'START_TIME', 'TIME', 'TIME_LEFT']

# JOBID: Job ID. 
# NAME: Job name.
# USER: User name for a job.
# STATE: Job state. For array jobs, this field includes all states of array jobs under the same job id. 
# PARTITION: Partition of the job
# NODELIST: List of nodes allocated to the job
# CPUS: the number of CPUs allocated to the job.
# MIN_MEMORY: Minimum size of memory (in MB) requested by the job. If the job is an array job, this field is 'MIN_MEMORY' X '#of array jobs'
# START_TIME: Actual or expected start time of the job or job step.
# TIME: Time used by the job or job step in days-hours:minutes:seconds. 
# TIME_LEFT: Time left for the job or job step to execute in days-hours:minutes:seconds. This value is calculated by subtracting the job's time used from its time limit. The value may be "NOT_SET" if not yet established or "UNLIMITED" for no limit. 

logging_path = './fetch_slurm_queue.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)


def main():
    # # Read configuration file
    # config = parse_config('../config.yml')
    # config_slurm = config['slurm_rest_api']

    # # Connect to TimescaleDB
    # connection = init_tsdb_connection(config)

    # Schedule fetch fetch_slurm_queue
    schedule.every(2).minutes.at(":00").do(fetch_slurm_queue)

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            schedule.clear()
            break 


def fetch_slurm_queue() -> list:
    # The command used in cli
    command = ["ssh monster@login.hpcc.ttu.edu squeue -o '%all'"]
    rtn_str = ""
    try:
        # Get strings from command line
        rtn_str = subprocess.run(command, shell=True, 
                                stdout=subprocess.PIPE).stdout.decode('utf-8')
    except Exception as err:
        logging.error(f"Call slurm cmd error: {err}")

    if rtn_str:
        # Process queue status
        # timestamp = datetime.now(pytz.utc).replace(microsecond=0).strftime(DATETIME_FORMAT)
        timestamp = int(datetime.now().timestamp())
        queue_status = convert_str_list(rtn_str)

        # Dump status into status file
        time_queue_status = {
            'timestamp': timestamp,
            'queue_status': queue_status
        }
        with open('./samples/queue_status.json', 'w') as f:
            json.dump(time_queue_status, f)

    return


def convert_str_list(rtn_str: str) -> dict:
    """
    Convert data in string to data in json.
    """
    queue_dict = {}
    rtn_list = rtn_str.splitlines()
    all_fields = rtn_list[0].split('|')

    # Get valid field and index
    field_index = get_index(all_fields)
    records_raw = rtn_list[1:]
    
    # for record_raw in tqdm(records_raw):
    for record_raw in records_raw: 
        record = parse_record(field_index, record_raw)
        job_id = record['JOBID']
        if job_id not in queue_dict:
            queue_dict[job_id] = record

    return list(queue_dict.values())


def get_index(all_fields: list) -> list:
    field_index = {}
    valid_fields = []
    valid_index = []
    # for field in SELECT_FIELDS:
        # if field in all_fields:
    for field in all_fields:
        valid_fields.append(field)
        index = all_fields.index(field)
        valid_index.append(index)
    field_index = {
        'valid_fields': valid_fields,
        'valid_index': valid_index
    }
    return field_index


def parse_record(field_index: dict, record_raw: str) -> dict:
    # record = {'TOTAL': 1}
    record = {}
    record_list = record_raw.split('|')
    # index = field_index['valid_index']
    for i, index in enumerate(field_index['valid_index']):
        field = field_index['valid_fields'][i]
        if field == 'NODELIST':
            if record_list[index]:
                record[field] = hostlist.expand_hostlist(record_list[index])
                 ## Do not expand node list
                # record[field] = [record_list[index]]
            else:
                record[field] = []
        # elif field == 'STATE':
        #     record[field] = [record_list[index]]
        elif field == 'CPUS':
            record[field] = int(record_list[index])
        elif field == 'START_TIME' or field == 'SUBMIT_TIME' or field == 'END_TIME':
            try:
                time_object = datetime.strptime(record_list[index], "%Y-%m-%dT%H:%M:%S")
                record[field] = int(time_object.timestamp())
            except:
                record[field] = 'N/A'
        else:
            record[field] = record_list[index]

    return record


# def dump_queue_metrics(timestamp: object, queue_status: list, conn: object) -> None:
#     """
#     Dump queue status into TimescaleDB
#     """
#     all_records = []
#     target_table = 'slurm.queue_status'
#     cols = ('timestamp', 'jobid', 'name', 'user', 'state', 'partition', 
#             'nodelist', 'cpus', 'min_memory', 'start_time', 'time', 'time_left')
#     for job_status in queue_status:
#         all_records.append((timestamp, job_status['JOBID'], job_status['NAME'], 
#                             job_status['USER'], job_status['STATE'], 
#                             job_status['PARTITION'], job_status['NODELIST'], 
#                             job_status['CPUS'], job_status['MIN_MEMORY'], 
#                             job_status['START_TIME'], job_status['TIME'],
#                             job_status['TIME_LEFT']))
#     mgr = CopyManager(conn, target_table, cols)
#     mgr.copy(all_records)
#     conn.commit()


# # Write to json
# print('--> Writing to a CSV file...')
# count = 0
# with open('./squeue.csv', 'w') as csvfile:
#     csvwriter = csv.writer(csvfile)
#     for key, values in queue_dict.items():
#         if count == 0:
#             header = values.keys()
#             csvwriter.writerow(header)
#             count += 1
#         csvwriter.writerow(values.values())

# print('--> Done!')

# with open('./samples/squeue.json', 'w') as f:
#     json.dump(queue_status, f, indent = 4)

if __name__ == '__main__':
    main()
    # fetch_slurm_queue()