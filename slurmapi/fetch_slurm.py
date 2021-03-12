# -*- coding: utf-8 -*-
"""
This module calls Slurm REST API to obtain detailed accouting information about
individual jobs or job steps and nodes information.

Job State:
https://slurm.schedmd.com/sacct.html#SECTION_JOB-STATE-CODES

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import time
import logging
import requests
import psycopg2
import subprocess

sys.path.append('../')

from requests.adapters import HTTPAdapter
from sharings.utils import parse_config, parse_hostnames, init_tsdb_connection

logging_path = './fetch_slurm.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

def main():
    # Read configuration file
    config = parse_config('../config.yml')
    config_slurm = config['slurm_rest_api']

    # Connect to TimescaleDB
    connection = init_tsdb_connection(config)

    # Get nodename-nodeid mapping dict
    node_id_mapping = gene_node_id_mapping(connection)

    # print(hostnames)
    # print([node_id_mapping[item] for item in hostnames])

    # Read token file, if it is out of date, get a new token
    token = read_token(config_slurm)

    # Get jobs metrics
    jobs_url = f"http://{config_slurm['ip']}:{config_slurm['port']}{config_slurm['slurm_jobs']}"
    jobs_metrics = fetch_slurm(config_slurm, token, jobs_url)

    node_jobs = get_node_jobs(jobs_metrics, node_id_mapping)

    # with open('./data/node_jobs.json', 'w') as f:
    #     json.dump(node_jobs, f, indent=4)

    # nodes_url = f"http://{config_slurm['ip']}:{config_slurm['port']}{config_slurm['slurm_nodes']}"
    # # openapi_url = f"http://{config_slurm['ip']}:{config_slurm['port']}{config_slurm['openapi']}"
    # # queue_status = aggregate_queue_status(jobs_metrics)

    # # for status, job_list in queue_status.items():
    # #     print(f"{status} : {len(job_list)}")
    # # with open('./data/job_status.json', 'w') as f:
    # #     json.dump(job_status, f, indent=4)


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


def fetch_slurm(config_slurm: dict, token: str, url: str) -> dict:
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


def get_node_jobs(jobs_metrics: dict, node_id_mapping:dict) -> list:
    """
    Only process running jobs, and get nodes-job correlation
    """
    node_jobs = {}
    all_jobs = jobs_metrics['jobs']
    # Get job-nodes correlation
    job_nodes = {}
    for job in all_jobs:
        if job['job_state'] == "RUNNING":
            job_id = job['job_id']
            nodes = job['nodes']
            # Get node ids
            hostnames = parse_hostnames(nodes)
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
        loggin.error(f"Faile to generate node-id mapping : {err}")


if __name__ == '__main__':
    main()
