# -*- coding: utf-8 -*-
"""
This module calls sacct in Slurm to obtain detailed accouting information about
individual jobs or job steps. This module can only be used in Python 3.5 or above.

Job State:
https://slurm.schedmd.com/sacct.html#SECTION_JOB-STATE-CODES

This module partially refers to 
https://gist.github.com/tcooper/a69d99cc71c73a96103e5d4a33281a84

Jie Li (jie.li@ttu.edu)
"""
import re
import sys
import json
import time
import datetime
import logging
import subprocess

sys.path.append('../../')

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger()

def fetch_slurm_job() -> list:
    # Setting command parameters
    accounting_fields = ['JobID', 'JobName', 'Partition', 'Account', 'User',
                         'NNodes', 'AllocCPUs', 'MaxVMSize', 'MaxRSS', 'Submit', 
                         'Timelimit', 'Start', 'End', 'Elapsed', 'State', 
                         'ExitCode', 'ReqTres', 'ReqMem','NodeList']
    job_states = ['CANCELLED', 'COMPLETED', 'FAILED', 'PREEMPTED', 'TIMEOUT']

    # Set start time and end time
    date_ = datetime.datetime.today() - datetime.timedelta(1)
    start_time = datetime.datetime(date_.year, date_.month, date_.day, 0, 0, 0)
    end_time = datetime.datetime(
        start_time.year, start_time.month, start_time.day, 23, 59, 59)
    
    # The command used in cli
    command = [
        "ssh monster@login.hpcc.ttu.edu " \
        + "'sacct  --allusers --starttime " \
        + start_time.strftime(DATETIME_FORMAT) + " --endtime " \
        + end_time.strftime(DATETIME_FORMAT) + " --state " \
        + ",".join(job_states) \
        + " --fields=" + ",".join(accounting_fields) \
        + " -p'"
    ]

    # Get strings from command line
    rtn_str = subprocess.run(command, shell=True, 
                             stdout=subprocess.PIPE).stdout.decode('utf-8')

    records = process_str(rtn_str)
    return records


def process_str(rtn_str: str) -> list:
    # Split strings by line, and discard the first line
    jobs_ar = {}
    rtn_str_arr = rtn_str.splitlines()[1:]
    for line in rtn_str_arr:
        elms = line.split("|")[:-1]
        matchObj = re.match(r'([\d_]+)', elms[0])
        if matchObj:
            job_id = matchObj.group(1)
            # First instance seen... the 'master' job_completion record which
            # include reqTres (field 16) and reqMem (field 17)...
            if job_id not in jobs_ar:
                jobs_ar[job_id] = elms
                jobs_ar[job_id][17] = requested_memory_to_full_number(
                                                        jobs_ar[job_id][17],
                                                        jobs_ar[job_id][5], 
                                                        jobs_ar[job_id][6])
            # Subsequent instances seen... the 'batch' entry or other job steps
            # which may include memory usage...
            else:
                for i in range(len(jobs_ar[job_id])):
                    if i in [7, 8]:
                        cur_value = human_to_full_number(jobs_ar[job_id][i]) or 0
                        jobs_ar[job_id][i] = \
                            str(round(
                                max([human_to_full_number(jobs_ar.get(job_id)[i]),
                                human_to_full_number(elms[i])])))

    for (k, v) in jobs_ar.items():
        processors_per_node = 0 # pylint: disable=invalid-name
        if int(v[5]):
            processors_per_node = int(v[6])/int(v[5])
        # Because ReqTRES always includes '=' we'll only ever drop into the first case
        if my_contains_any(v[16], '='):
            # v[16] is ReqGres and is inserted between processors_per_node= and
            # reqmem= and is unknown compound
            v[16] = v[16].replace(',',':')
            log_message = (
                '%s;E;%s;user=%s group=%s account=%s jobname=%s Exit_status=%s '
                'queue=%s Resource_List.nodect=%s:ppn=%d:%s:reqmem=%s:%s '
                'ctime=%s start=%s end=%s resources_used.walltime=%s '
                'resources_used.mem=%s resources_used.vmem=%s '
                'resources_requested.timelimit=%s'
            )
            print(
                log_message % ( # pylint: disable=invalid-name
                    slurm_date_to_pbs_date(v[9]), #Submit
                    k,
                    v[4], #User
                    v[3], #Account
                    v[3], #Account
                    v[1], #JobName
                    v[15].split(":")[0], #ExitCode
                    v[2], #Partition
                    v[5], #Nnodes
                    processors_per_node, #ProcessorsPerNode
                    v[16], #ReqTRES
                    v[17], #ReqMem
                    v[2], #partition for Resource_List
                    date_to_epoch(v[9]), #Submit
                    date_to_epoch(v[11]), #Start
                    date_to_epoch(v[12]), #End
                    v[13], #Elapsed
                    v[8], #MaxRSS
                    v[7], #MaxVMSize
                    v[10], #Timelimit
                )
            )
        else:
            logger.error("v[16]: %s does not have '=' characters", v[16])
            log_message = ( # pylint: disable=invalid-name
                '%s;E;%s;user=%s group=%s account=%s jobname=%s Exit_status=%s '
                'queue=%s Resource_List.nodect=%s:ppn=%d:reqmem=%s:%s '
                'ctime=%s start=%s end=%s resources_used.walltime=%s '
                'resources_used.mem=%s resources_used.vmem=%s '
                'resources_requested.timelimit=%s'
                )
            print(
                log_message % ( # pylint: disable=invalid-name
                    v[9], #Submit
                    k,
                    v[4], #User
                    v[3], #Account
                    v[3], #Account
                    v[1], #JobName
                    v[15].split(":")[0], #ExitCode
                    v[2], #Partition
                    v[5], #Nnodes
                    processors_per_node, #ProcessorsPerNode
                    v[17], #ReqMem
                    v[2], #partition for Resource_List
                    date_to_epoch(v[9]), #Submit
                    date_to_epoch(v[11]), #Start
                    date_to_epoch(v[12]), #End
                    v[13], #Elapsed
                    v[8], #MaxRSS
                    v[7], #MaxVMSize
                    v[10], #Timelimit
                )
            )
    return jobs_ar


def slurm_date_to_pbs_date(date):
    """Convert Slurm date format to PBS date format."""
    return datetime.datetime.strptime(date,
        DATETIME_FORMAT).strftime('%m-%d-%Y %H:%M:%S')


def date_to_epoch(date):
    """Convert date string to *NIX epoch."""
    return str(int(time.mktime(time.strptime(date, DATETIME_FORMAT))))


def my_contains_any(str, set): # pylint: disable=W0622
    """Finds any character from set in str."""
    for character in set:
        if character in str:
            return 1
    return 0


def human_to_full_number(human_size):
    """
    Refer to https://gist.github.com/tcooper/a69d99cc71c73a96103e5d4a33281a84
    Convert Slurm human readable memory request size to bytes value.
    """
    if not human_size:
        return 0
    sizes = ['K', 'M', 'G', 'T', 'P']
    postfix = human_size[-1]
    if postfix in sizes:
        size = float(human_size[:-1]) * 1024 ** (sizes.index(postfix) + 1)
    else:
        size = float(human_size)
    return size


def requested_memory_to_full_number(mem_request, nnodes, ncpus):
    """
    Refer to https://gist.github.com/tcooper/a69d99cc71c73a96103e5d4a33281a84
    Translate Slurm ReqMem value into bytes taking account of typical
    suffixes of 'c' (per core) and 'n' (per node).
    """
    if not mem_request:
        return 0
    codes = ['c', 'n']
    postfix = mem_request[-1]
    if postfix in codes:
        size = human_to_full_number(mem_request[:-1])
        if postfix in 'c':
            reqmem = size * float(ncpus)
        else:
            reqmem = size * float(nnodes)
    else:
        size = mem_request
    return int(reqmem)


def unfold_metrics(metric_str: str, in_out: str) -> dict:
    """
    Unfold the metrics under the same metric name(such as tresusageintot, tresusageouttot)
    """
    metric_dict = {}
    for item in metric_str.split(","):
        item_pair = item.split("=")

        if item_pair[0] == "fs/disk" or item_pair[0] == "energy":
            key_name = item_pair[0] + "_" + in_out
        else:
            key_name = item_pair[0]

        metric_dict.update({
            key_name: item_pair[1]
        })

    return metric_dict


if __name__ == '__main__':
    records = fetch_slurm_job()
    # with open('./jobs_ar.json', 'w') as f:
    #     json.dump(records, f)
