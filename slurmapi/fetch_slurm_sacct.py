"""
This module calls sacct in Slurm to obtain detailed accouting information about
individual jobs or job steps. This module can only be used in Python 3.5 or above.

Sacct:
https://slurm.schedmd.com/sacct.html

Get all jobs that have been terminated.
sacct --allusers --starttime midnight --endtime now --state BOOT_FAIL,CANCELLED,COMPLETED,DEADLINE,FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT --fields=partition,nodelist,group,user,jobname,jobid,submit,start,end,exitcode,cputimeraw,tresusageintot,tresusageouttot,maxvmsize,alloccpus,ntasks,cluster,timelimitraw,reqmem,State -p > sacct_raw_parse.txt
sacct --allusers --starttime midnight --endtime now --state BOOT_FAIL,CANCELLED,COMPLETED,DEADLINE,FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT --fields=partition,nodelist,group,user,jobname,jobid,submit,start,end,exitcode,cputimeraw,tresusageintot,tresusageouttot,maxvmsize,alloccpus,ntasks,cluster,timelimitraw,reqmem,State > sacct_raw.txt

Jie Li (jie.li@ttu.edu)
"""
import io
import sys
import pytz
import json
import getopt
import hostlist
import subprocess
import pandas as pd
from datetime import datetime, timezone

sys.path.append('../')

from sharings.utils import parse_config, parse_hostnames, \
    init_tsdb_connection, gene_node_id_mapping

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

attributes_map = {
    'job_id': 'JobIDRaw', 
    'array_job_id': 'JobID', # jobID = ArrayJobID_ArrayTaskID
    'array_task_id': 'NaN', #jobID = ArrayJobID_ArrayTaskID
    'name': 'JobName',
    'job_state': 'State', 
    'user_id': 'UID', 
    'user_name': 'User',
    'group_id': 'GID',
    'cluster': 'Cluster', 
    'partition': 'Partition', 
    'command': 'NaN', 
    'current_working_directory': 'WorkDir', 
    'batch_flag': 'NaN', 
    'batch_host': 'NaN',
    'nodes': 'NodeList', 
    'node_count': 'NNodes',
    'cpus': 'NCPUS',
    'tasks': 'NaN', # ? cpus,
    'tasks_per_node': 'NaN', 
    'cpus_per_task': 'NaN', 
    'memory_per_node': 'NaN', 
    'memory_per_cpu': 'ReqMem',
    'priority': 'Priority',
    'time_limit': 'TimelimitRaw',
    'deadline': 'NaN', 
    'submit_time': 'Submit',
    'preempt_time':'NaN',
    'suspend_time': 'Suspended', 
    'eligible_time': 'Eligible',
    'start_time': 'Start',
    'end_time': 'End',
    'resize_time': 'NaN', 
    'restart_cnt': 'NaN',
    'exit_code': 'ExitCode',
    'derived_exit_code': 'DerivedExitCode'}


# def get_jobs_acct_by_id(jobids: list, attributes: list):
#     jobids_str = [str(id) for id in jobids]
#     jobs = ','.join(jobids_str)

#     command = [
#         "ssh monster@login.hpcc.ttu.edu " \
#         + "'sacct --jobs " + jobs \
#         + " --format=all --parsable'"
#     ]

#     rtn_str = subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')

#     # io.StringIO, in-memory-file-like object
#     df_raw = pd.read_csv(io.StringIO(rtn_str), sep='|')
#     # Remove Unnamed columns
#     df_raw = df_raw.loc[:, ~df_raw.columns.str.contains('^Unnamed')]
#     df_reduced = df_raw[attributes]
#     return df_reduced


def get_jobs_acct_df(starttime: str, endtime: str, attributes: list):
    command = [
        "ssh monster@login.hpcc.ttu.edu " \
        + "'sacct --starttime " + starttime \
        + " --endtime " + endtime \
        + " --format=all --parsable2 --allusers --partition nocona --state COMPLETED,RUNNING'"
    ]

    rtn_str = subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')

    # io.StringIO, in-memory-file-like object
    df_raw = pd.read_csv(io.StringIO(rtn_str), sep='|')
    # Remove Unnamed columns
    df_raw = df_raw.loc[:, ~df_raw.columns.str.contains('^Unnamed')]
    df_reduced = df_raw[attributes]

    return df_reduced


def process_jobs_acct_df(df, node_id_mapping: dict):
    # Generate a rename map
    rename_map = {}
    for i, j in attributes_map.items():
        if j!= 'NaN':
            rename_map.update({
                j: i
            })
    # Remove batch and entern info
    df = df[(df['JobName'] != 'batch') & (df['JobName'] != 'extern')]

    # Remove job step (which contains . in JobIDRaw)
    df = df[~df['JobIDRaw'].str.contains('\.')]

    # Derive array job id and task id
    df['array_job_id'] = df['JobID'].apply(lambda x: x.split('_')[0] if '_' in x else 0)
    df['array_task_id'] = df['JobID'].apply(lambda x: x.split('_')[1] if '_' in x else 0)
    
    # The job_id will replaced by JobIDRaw, therefore drop 'JobID
    df = df.drop(columns=['JobID'])
    df = df.rename(columns=rename_map)
    df['job_id'] = df['job_id'].astype(int)
    df['array_job_id'] = df['array_job_id'].astype(int)
    # Cannot handle task_id like [150-196]
    # df['array_task_id'] = df['array_task_id'].astype(int)
    df['user_id'] = df['user_id'].astype(int)
    df['group_id'] = df['group_id'].astype(int)
    df['priority'] = df['priority'].astype(int)
    df['time_limit'] = df['time_limit'].astype(int)

    df['eligible_time'] = df['eligible_time'].apply(lambda x: convert_to_epoch(x))
    df['submit_time'] = df['submit_time'].apply(lambda x: convert_to_epoch(x))
    df['start_time'] = df['start_time'].apply(lambda x: convert_to_epoch(x))
    df['end_time'] = df['end_time'].apply(lambda x: convert_to_epoch(x))

    df['nodes'] = df['nodes'].apply(lambda x: hostlist.expand_hostlist(x))

    # Todo: Add swap info
    df.reset_index(drop=True, inplace=True)
    
    return df


def convert_to_epoch(time_str: str):
    try:
        central = pytz.timezone('US/Central')
        time_t = datetime.strptime(time_str, DATETIME_FORMAT)
        time_tz = central.localize(time_t)
        time_epoch = int(time_tz.timestamp())
    except:
        time_epoch = 0
    return time_epoch


def fetch_slurm_sacct(starttime: str, endtime: str):
    config = parse_config('../config.yml')
    config_slurm = config['slurm_rest_api']

    # Connect to TimescaleDB
    connection = init_tsdb_connection(config)

    # Get nodename-nodeid mapping dict
    node_id_mapping = gene_node_id_mapping(connection)

    attributes = [ j for i, j in attributes_map.items() if j!= 'NaN' ]

    # df = get_jobs_acct_df(jobids, attributes)
    df = get_jobs_acct_df(starttime, endtime, attributes)

    df = process_jobs_acct_df(df, node_id_mapping)
    df = df.set_index('job_id')

    # Conver to json
    result = df.to_json(orient='index')
    parsed = json.loads(result)

    return parsed

if __name__ == '__main__':
    argv = sys.argv[1:]
    starttime = 'midnight'
    endtime = 'now'
    outfile = ''
    # starttime = '2021-06-18T06:00:00'
    # endtime = '2021-06-18T08:00:00'
    try:
        opts, args = getopt.getopt(argv, 's:e:o:', ['starttime =', 'endtime = ', 'outfile ='])
    except:
        print('Arguments Error!')

    for opt, arg in opts:
        if opt in ['-s', '--starttime']:
            starttime = arg
        elif opt in ['-e', '--endtime']:
            endtime = arg
        elif opt in ['-o', '--outfile']:
            outfile = arg
        else:
            break

    if not outfile:
        outfile = f'./samples/{starttime}_{endtime}.json'
    
    print(f'Get jobs between {starttime} and {endtime}...')
    jobs_data = fetch_slurm_sacct(starttime, endtime)

    with open(outfile, 'w') as f:
        json.dump(jobs_data, f, indent=4)
