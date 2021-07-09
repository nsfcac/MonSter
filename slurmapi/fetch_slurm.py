# -*- coding: utf-8 -*-
"""
This module calls Slurm REST API to obtain detailed accouting information about
individual jobs or job steps and nodes metrics.

Job State:
https://slurm.schedmd.com/sacct.html#SECTION_JOB-STATE-CODES

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import time
import pytz
import logging
import requests
import psycopg2
import schedule
import hostlist
import subprocess

sys.path.append('../')

from pgcopy import CopyManager
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from sharings.utils import parse_config, init_tsdb_connection, gene_node_id_mapping

logging_path = './fetch_slurm.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

PREV_JOBS_OTHERS = []
PREV_JOBS_COMPLETED = []

INSERT_JOBS = []
UPDATE_JOBS = []

def main():
    # Read configuration file
    config = parse_config('../config.yml')
    config_slurm = config['slurm_rest_api']

    # Connect to TimescaleDB
    connection = init_tsdb_connection(config)

    # Get nodename-nodeid mapping dict
    node_id_mapping = gene_node_id_mapping(connection)

    # # Schedule fetch slurm

    schedule.every().minutes.at(":00").do(fetch_slurm, config_slurm, connection, node_id_mapping)

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            schedule.clear()
            break 

    # fetch_slurm(config_slurm, connection, node_id_mapping)


def fetch_slurm(config_slurm: dict, connection: str, node_id_mapping: dict) -> None:
    # Read token file, if it is out of date, get a new token
    token = read_token(config_slurm)
    timestamp = datetime.now(pytz.utc).replace(microsecond=0)

    # Get nodes data
    nodes_url = f"http://{config_slurm['ip']}:{config_slurm['port']}{config_slurm['slurm_nodes']}"
    nodes_data = call_slurm_api(config_slurm, token, nodes_url)

    # Get jobs data
    jobs_url = f"http://{config_slurm['ip']}:{config_slurm['port']}{config_slurm['slurm_jobs']}"
    jobs_data = call_slurm_api(config_slurm, token, jobs_url)

    ## Process slurm data
    if jobs_data and nodes_data:
        job_metrics = parse_jobs_metrics(jobs_data, node_id_mapping)
        node_jobs = get_node_jobs(jobs_data, node_id_mapping)
        node_metrics = parse_node_metrics(nodes_data, node_id_mapping)

        # print(json.dumps(job_metrics, indent=4))
        ## Dump metrics into TimescaleDB
        with psycopg2.connect(connection) as conn:
            dump_jobs_metrics(job_metrics, conn)
            dump_node_metrics(timestamp, node_metrics, conn)
            dump_node_jobs_metrics(timestamp, node_jobs, conn)
        

def read_token(config_slurm: dict) -> str:
    """
    Read token file, if it is out of date, get a new token
    """
    token = ""
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
    return token


def get_slurm_token(config_slurm: dict) -> list:
    """
    Get JWT token from Slurm. This requires the public key on this node to be 
    added to the target cluster headnode.
    """
    while True:
        try:
            # Setting command parameters
            slurm_rest_api_ip = config_slurm['ip']
            slurm_rest_api_port = config_slurm['port']
            slurm_rest_api_user = config_slurm['user']
            slurm_headnode = config_slurm['headnode']
            
            print("Get a new token...")
            # The command used in cli
            command = [f"ssh {slurm_headnode} 'scontrol token lifespan=3600'"]
            # Get the string from command line
            rtn_str = subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
            # Get token
            token = rtn_str.splitlines()[0].split('=')[1]
            timestamp = int(time.time())

            token_record = {
                'time': timestamp,
                'token': token
            }

            with open('./token.json', 'w') as f:
                json.dump(token_record, f, indent = 4)
            
            return token
        except Exception as err:
            print("Get Slurm token error!")
            time.sleep(60)
        else:
            break


def call_slurm_api(config_slurm: dict, token: str, url: str) -> dict:
    """
    Fetch slurm info via Slurm REST API
    """
    metrics = {}
    headers = {"X-SLURM-USER-NAME": config_slurm['user'], "X-SLURM-USER-TOKEN": token}
    adapter = HTTPAdapter(max_retries=3)
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(url, headers=headers)
            metrics = response.json()
        except Exception as err:
            logging.error(f"Fetch slurm metrics error: {err}")
    return metrics


def get_response_properties() -> None:
    job_properties = {}
    node_properties = {}
    with open('./data/openapi.json', 'r') as f:
        openapi = json.load(f)
        job_properties = openapi['components']['schemas']['v0.0.36_job_response_properties']['properties']
        node_properties = openapi['components']['schemas']['v0.0.36_node']['properties'] 
    with open('./data/job_properties.json', 'w') as f:
        json.dump(job_properties, f, indent = 4)
    with open('./data/node_properties.json', 'w') as f:
        json.dump(node_properties, f, indent = 4)


def get_node_jobs(jobs_metrics: dict, node_id_mapping:dict) -> dict:
    """
    Only process running jobs, and get nodes-job correlation
    """
    node_jobs = {}
    all_jobs = jobs_metrics['jobs']
    # Get job-nodes correlation
    job_nodes = {}
    for job in all_jobs:
        valid_flag = True
        if job['job_state'] == "RUNNING":
            job_id = job['job_id']
            nodes = job['nodes']
            # Get node ids
            hostnames = hostlist.expand_hostlist(nodes)
            
            # Check if hostname is in node_id_mapping. If not, ignore this job info.
            for hostname in hostnames:
                if hostname not in node_id_mapping:
                    valid_flag = False
                    break

            if valid_flag:
                node_ids = [node_id_mapping[i] for i in hostnames]
                node_ids.sort()
                # Get cpu counts for each node
                allocated_nodes = job['job_resources']['allocated_nodes']
                cpu_counts = [resource['cpus'] for node, resource in allocated_nodes.items()]
                job_nodes.update({
                    job_id: {
                        'nodes': node_ids,
                        'cpus': cpu_counts
                    }
                })
    # Get nodes-job correlation
    for job, nodes_cpus in job_nodes.items():
        for i, node in enumerate(nodes_cpus['nodes']):
            if node not in node_jobs:
                node_jobs.update({
                    node: {
                        'jobs':[job],
                        'cpus':[nodes_cpus['cpus'][i]]
                    }
                })
            else:
                node_jobs[node]['jobs'].append(job)
                node_jobs[node]['cpus'].append(nodes_cpus['cpus'][i])
    # node_jobs = sorted(node_jobs.items(), key=lambda item: item[0])
    # print(len(list(node_jobs.keys())))
    return node_jobs


def parse_jobs_metrics(jobs_metrics: dict, node_id_mapping: dict):
    """
    Parse jobs metrics
    """
    # global UPDATE_JOBS
    # global INSERT_JOBS

    jobs_info = []

    all_jobs = jobs_metrics['jobs']
    attributes = ['job_id', 'array_job_id', 'array_task_id', 'name','job_state', 
                  'user_id', 'user_name', 'group_id', 'cluster', 'partition', 
                  'command', 'current_working_directory', 'batch_flag', 
                  'batch_host', 'nodes', 'node_count', 'cpus', 'tasks', 
                  'tasks_per_node', 'cpus_per_task', 'memory_per_node', 
                  'memory_per_cpu', 'priority', 'time_limit', 'deadline', 
                  'submit_time', 'preempt_time', 'suspend_time', 
                  'eligible_time', 'start_time', 'end_time', 'resize_time', 
                  'restart_cnt', 'exit_code', 'derived_exit_code']
    
    for job in all_jobs:
        job_id = job['job_id']
        job_state = job['job_state']

        batch_host = job['batch_host']
        # batch_host_id = node_id_mapping.get(batch_host, -1)

        nodes = job['nodes']
        hostnames = hostlist.expand_hostlist(nodes)

        # node_ids = [node_id_mapping[i] for i in hostnames]

        metrics = []
        for attribute in attributes:
            if attribute == 'nodes':
                metrics.append(hostnames)
            else:
                # Some attributes values are larger than 2147483647, which is 
                # not INT4, and cannot saved in TSDB
                if type(job[attribute]) is int and job[attribute] > 2147483647:
                    metrics.append(2147483647)
                else:
                    metrics.append(job[attribute])
        tuple_metrics = tuple(metrics)

        jobs_info.append(tuple_metrics)
        # if job_state == "COMPLETED" and job_id not in UPDATE_JOBS:
        #     jobs_info['update'] .append(tuple_metrics)
        #     UPDATE_JOBS.append(job_id)
        
        # if job_state != "COMPLETED" and job_id not in INSERT_JOBS:
        #     jobs_info['insert'] .append(tuple_metrics)
        #     INSERT_JOBS.append(job_id)
            
    return jobs_info


def parse_node_metrics(nodes_metrics: dict, node_id_mapping: dict) -> dict:
    """
    Parse node metrics, get memory usage, state etc.
    Nodes metrics include some nodes that are not Nocona, Matador, GPU-Build or 
    Quanah cluster, therefore, we discard these nodes metrics
    """
    all_node_data = {}
    state_mapping = {
        'allocated': 1,
        'idle':0,
        'down': -1
    }
    all_nodes = nodes_metrics['nodes']
    for node in all_nodes:
        hostname = node['hostname']
        # Only process those nodes that are in node_id_mapping dict. 
        if hostname in node_id_mapping:
            node_id = node_id_mapping[hostname]
            # CPU load
            cpu_load = int(node['cpu_load'])
            # Some down nodes report cpu_load large than 2147483647, which is 
            # not INT4 and cannot saved in TSDB
            if cpu_load > 2147483647: 
                cpu_load = 2147483647
            # Memory usage
            free_memory = node['free_memory']
            real_memory = node['real_memory']
            memory_usage = ((real_memory - free_memory)/real_memory) * 100
            memory_used = real_memory - free_memory
            f_memory_usage = float("{:.2f}".format(memory_usage))
            # Status
            state = node['state']
            f_state = state_mapping[state]
            node_data = {
                'cpu_load': cpu_load,
                'memoryusage': f_memory_usage,
                'memory_used': memory_used,
                'state': f_state
            }
            all_node_data.update({
                node_id: node_data
            })
    return all_node_data


def gene_node_id_mapping(connection: str) -> dict:
    """
    Generate nodename-nodeid mapping dict
    """
    mapping = {}
    try:
        with psycopg2.connect(connection) as conn:
            cur = conn.cursor()
            query = "SELECT nodeid, hostname FROM nodes"
            cur.execute(query)
            for (nodeid, hostname) in cur.fetchall():
                mapping.update({
                    hostname: nodeid
                })
            cur.close()
            return mapping
    except Exception as err:
        logging.error(f"Faile to generate node-id mapping : {err}")


def dump_node_jobs_metrics(timestamp: object, 
                           node_jobs_metrics: dict, 
                           conn: object) -> None:
    """
    Dump slurm metrics into TimescaleDB
    """
    try:
        all_records = []
        target_table = 'slurm.node_jobs'
        cols = ('timestamp', 'nodeid', 'jobs', 'cpus')
        for node, job_info in node_jobs_metrics.items():
            all_records.append((timestamp, int(node), job_info['jobs'], job_info['cpus']))
        mgr = CopyManager(conn, target_table, cols)
        mgr.copy(all_records)
        conn.commit()
    except Exception as err:
        curs = conn.cursor()
        curs.execute("ROLLBACK")
        conn.commit()
        logging.error(f"Faile to dump_node_jobs_metrics : {err}")


def dump_node_metrics(timestamp: object, 
                      node_metrics: dict, 
                      conn: object) -> None:
    """
    Dump slurm metrics into TimescaleDB
    """
    schema = 'slurm'
    try:
        metric_names = list(list(node_metrics.values())[0].keys())

        for metric_name in metric_names:
            all_records = []
            target_table = f'{schema}.{metric_name}'
            cols = ('timestamp', 'nodeid', 'value')
            for node, node_data in node_metrics.items():
                all_records.append((timestamp, int(node), node_data[metric_name]))
            mgr = CopyManager(conn, target_table, cols)
            mgr.copy(all_records)
            conn.commit()
    except Exception as err:
        curs = conn.cursor()
        curs.execute("ROLLBACK")
        conn.commit()
        logging.error(f"Faile to dump_node_metrics : {err}")


def dump_jobs_metrics(job_metrics: dict, conn: object) -> None:
    """
    Dump jobs info into slurm.jobs
    """
    try:
        target_table = 'slurm.jobs'
        cols = ('job_id', 'array_job_id', 'array_task_id', 'name','job_state', 
                'user_id', 'user_name', 'group_id', 'cluster', 'partition', 
                'command', 'current_working_directory', 'batch_flag', 
                'batch_host', 'nodes', 'node_count', 'cpus', 'tasks', 
                'tasks_per_node', 'cpus_per_task', 'memory_per_node', 
                'memory_per_cpu', 'priority', 'time_limit', 'deadline', 
                'submit_time', 'preempt_time', 'suspend_time', 'eligible_time', 
                'start_time', 'end_time', 'resize_time', 'restart_cnt', 
                'exit_code', 'derived_exit_code')

        cur = conn.cursor()
        all_records = []

        for job in job_metrics:
            job_id = job[cols.index('job_id')]
            check_sql = f"SELECT EXISTS(SELECT 1 FROM slurm.jobs WHERE job_id={job_id})"
            cur.execute(check_sql)
            (job_exists, ) = cur.fetchall()[0]

            if job_exists:
                # Update
                job_state = job[cols.index('job_state')]
                start_time = job[cols.index('start_time')]
                end_time = job[cols.index('end_time')]
                resize_time = job[cols.index('resize_time')]
                restart_cnt = job[cols.index('restart_cnt')]
                exit_code = job[cols.index('exit_code')]
                derived_exit_code = job[cols.index('derived_exit_code')]
                update_sql = """ UPDATE slurm.jobs 
                                 SET job_state = %s, start_time = %s, end_time = %s, resize_time = %s, restart_cnt = %s, exit_code = %s, derived_exit_code = %s
                                 WHERE job_id = %s """
                cur.execute(update_sql, (job_state, start_time, end_time, resize_time, restart_cnt, exit_code, derived_exit_code, job_id))
            else:
                all_records.append(job)

        mgr = CopyManager(conn, target_table, cols)
        mgr.copy(all_records)
        conn.commit()
    except Exception as err:
        # Ref: https://stackoverflow.com/questions/2979369/databaseerror-current-transaction-is-aborted-commands-ignored-until-end-of-tra
        curs = conn.cursor()
        curs.execute("ROLLBACK")
        conn.commit()
        logging.error(f"Faile to dump_jobs_metrics : {err}")


if __name__ == '__main__':
    main()
