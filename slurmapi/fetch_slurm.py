import json
import time
from slurmapi import Slurm_Job, Slurm_Node, Slurm_Statistics

valid_job = [
            "submit_time", "start_time", "suspend_time", "end_time", 
            "run_time", "job_state", "nodes", "num_cpus", "num_nodes"
]  
valid_node = [
    "cpus", "cores", "sockets", "cores_per_socket", "free_mem",
    "real_memory", "cpu_load", "threads", "current_watts",
    "consumed_energy", "state"
]
valid_stat = [
    "jobs_submitted", "jobs_started", 
    "jobs_completed", "jobs_canceled", "jobs_failed"
]

metrics = {
    "job": valid_job,
    "node": valid_node,
    "statistics": valid_stat
}

def fetch_slurm(metrics: object) -> object:
    """
    Fetch metrics from slurm, and filter metrics according to the configuration.
    """
    slurm_info = {}
    
    try:
        job = Slurm_Job()
        job_data = job.get()
        epoch_time = int(time.time())
        job_info = process_job(metrics["job"], job_data, epoch_time)

        node = Slurm_Node()
        node_data = node.get()
        epoch_time = int(time.time())
        node_info = process_node(metrics["node"], node_data, epoch_time)

        stat = Slurm_Statistics()
        stat_data = stat.get()
        epoch_time = int(time.time())
        stat_info = process_node(metrics["statistics"], stat_data, epoch_time)

        slurm_info.update({
            "job_info": job_info,
            "node_info": node_info,
            "stat_info": stat_info
        })

    except Exception as err:
        print(err)
    return slurm_info


def process_job(job_metrics: list, job_data: object, time: int) -> list:
    job_info = []
    jobid_arr = job_data["data"].keys()
    for j in jobid_arr:
        j_data = job_data["data"][j]
        job_point = {
            "measurement": "job",
            "tags": {
                "job_id": j_data["job_id"],
                "user_id": j_data["user_id"]
            },
            "time": time,
            "fields": {}
        }
        for m in job_metrics:
            job_point["fields"].update({
                m: j_data[m]
            })
        job_info.append(job_point)
    return job_info


def process_node(node_metrics: list, node_data: object, time: int) -> list:
    node_info = []
    node_arr = node_data["data"].keys()
    for n in node_arr:
        n_data = node_data["data"][n]
        node_point = {
            "measurement": "job",
            "tags": {
                "node_hostname": n_data["node_hostname"],
                "node_addr": n_data["node_addr"],
                "partitions": n_data["partitions"]
            },
            "time": time,
            "fields": {}
        }
        for m in node_metrics:
            node_point["fields"].update({
                m: n_data[m]
            })
        node_info.append(node_point)
    return node_info

def process_stat(stat_metrics: list, stat_data: object, time: int) -> list:
    stat_info = []
    stat_data = stat_data["data"]
    stat_point = {
        "measurement": "statistics",
        "time": time,
        "fields": {}
    }
    for m in stat_metrics:
        stat_point["fields"].update({
            m: stat_data[m]
        })

    stat_info.append(stat_point)
    return stat_info

fetch_slurm(metrics)