import json
import logger
import hostlist


log = logger.get_logger(__name__)

def parse_jobs_metrics(jobs_data: dict):
    """parse_jobs_metrics Parse Jobs Metrics

    Parse jobs metrics get from Slurm API

    Args:
        jobs_data (dict): Job data get from Slurm APi

    Returns:
        list: Parsed jobs info
    """
    jobs_metrics = []

    all_jobs = jobs_data['jobs']
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
        nodes = job['nodes']
        hostnames = hostlist.expand_hostlist(nodes)

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
        jobs_metrics.append(tuple_metrics)
            
    return jobs_metrics


def parse_node_metrics(nodes_data: dict, node_id_mapping: dict):
    """parse_node_metrics Parse Node Metircs

    Parse Nodes metrics get from Slurm API

    Args:
        nodes_data (dict): Nodes data get from Slurm APi
        node_id_mapping (dict): Node-Id mapping

    Returns:
        dict: Parsed node metrics
    """
    all_node_metrics = {}
    state_mapping = {
        'allocated': 1,
        'idle':0,
        'down': -1
    }
    all_nodes = nodes_data['nodes']
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
            all_node_metrics.update({
                node_id: node_data
            })
    return all_node_metrics


def parse_node_jobs(jobs_metrics: dict, node_id_mapping:dict):
    """parse_node_jobs Parse Node-Jobs

    Parse nodes-job correlation

    Args:
        jobs_metrics (dict): Job metrics get from Slurm APi
        node_id_mapping (dict): Node-Id mapping

    Returns:
        dict: node-jobs correlation
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

    return node_jobs