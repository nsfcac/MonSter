# -*- coding: utf-8 -*-
"""
This module create a SSE to push queue status of redraider cluster
Jie Li (jie.li@ttu.edu)
"""
import os
import sys
import time
import json
import logging
import flask

sys.path.append('../')

logging_path = './sse_queue_status.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

from sharings.utils import parse_config
from slurmapi.fetch_slurm import get_slurm_token, fetch_slurm
app = flask.Flask(__name__)

def get_jobs_metrics() -> dict:
    # Read configuration file
    config_path = '../config.yml'
    config_slurm = parse_config(config_path)['slurm_rest_api']

    # Read token file, if it is out of date, get a new token
    try:
        with open('./token.json', 'r') as f:
            token_record = json.load(f)
            time_interval = int(time.time()) - token_record['time']
            if time_interval >= 3600:
                token = get_slurm_token(config_slurm)
            else:
                token = token_record['token']
    except:
        token = get_slurm_token(config_slurm)

    jobs_url = f"http://{config_slurm['ip']}:{config_slurm['port']}{config_slurm['slurm_jobs']}"
    jobs_metrics = fetch_slurm(config_slurm, token, jobs_url)

    return jobs_metrics


def aggregate_queue_status() -> dict:
    """
    Aggregate job status from the job info, 
    all job state codes can be found: https://slurm.schedmd.com/squeue.html#lbAG
    BOOT_FAIL, CANCELLED, COMPLETED, CONFIGURING, COMPLETING, DEADLINE, FAILED,
    NODE_FAIL, OUT_OF_MEMORY, PENDING, PREEMPTED, RUNNING, RESV_DEL_HOLD, 
    REQUEUE_FED,REQUEUE_HOLD, REQUEUED, RESIZING, REVOKED, SIGNALING, 
    SPECIAL_EXIT, STAGE_OUT,STOPPED, SUSPENDED, TIMEOUT
    """
    time.sleep(15)
    # Get jobs metrics
    jobs_metrics = get_jobs_metrics()
    # Aggregate queue status
    all_jobs = jobs_metrics['jobs']
    quanah_allocated_cpus = 0
    nocona_allocated_cpus = 0
    matador_allocated_cpus = 0
    status = {}
    status_list = {
        'RUNNING': [],
        'PENDING': [],
        'COMPLETED': [],
        # 'TIMEOUT': [],
        'CANCELLED': [],
        'FAILED': [],
        # 'STOPPED': [],
        # 'REQUEUED':[]
    }

    for job in all_jobs:
        job_id = job['job_id']
        job_state = job['job_state']
        if job_state in status_list:
            status_list[job_state].append(job_id)
        if job_state == "RUNNING":
            cpus = job['job_resources']['allocated_cpus']
            if job['partition'] == 'quanah':
                quanah_allocated_cpus += cpus
            if job['partition'] == 'nocona':
                nocona_allocated_cpus += cpus
            if job['partition'] == 'matador':
                matador_allocated_cpus += cpus
    
    for status_name, job_list in status_list.items():
        status.update({
            status_name: len(job_list)
        })
    status.update({
        'QUANAH_ALLOCATED_CORES': quanah_allocated_cpus,
        'NOCONA_ALLOCATED_CORES': nocona_allocated_cpus,
        'MATADOR_ALLOCATED_CORES': matador_allocated_cpus
    })
        # print(status)
    return status


@app.route('/sse_queue_status')
def stream_queue_status():
    # return "this is the data"
    def eventStream():
        while True:
            yield f'data: {aggregate_queue_status()}\n\n'
    return flask.Response(eventStream(), mimetype="text/event-stream")

if __name__ == '__main__':
    # To test: hugo.hpcc.ttu.edu:5000/sse_queue_status
    app.run(host= '0.0.0.0', port=5000, threaded=True, debug=True)