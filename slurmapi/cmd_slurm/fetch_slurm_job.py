# -*- coding: utf-8 -*-
"""
This module calls sacct in Slurm to obtain detailed accouting information about
individual jobs or job steps. This module can only be used in Python 3.5 or above.

Job State:
https://slurm.schedmd.com/sacct.html#SECTION_JOB-STATE-CODES

Get all jobs that have been terminated.
sacct --allusers --starttime midnight --endtime now --state BOOT_FAIL,CANCELLED,COMPLETED,DEADLINE,FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT --fields=partition,nodelist,group,user,jobname,jobid,submit,start,end,exitcode,cputimeraw,tresusageintot,tresusageouttot,maxvmsize,alloccpus,ntasks,cluster,timelimitraw,reqmem,State -p > sacct_raw_parse.txt
sacct --allusers --starttime midnight --endtime now --state BOOT_FAIL,CANCELLED,COMPLETED,DEADLINE,FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT --fields=partition,nodelist,group,user,jobname,jobid,submit,start,end,exitcode,cputimeraw,tresusageintot,tresusageouttot,maxvmsize,alloccpus,ntasks,cluster,timelimitraw,reqmem,State > sacct_raw.txt

Jie Li (jie.li@ttu.edu)
"""
import re
import sys
import json
import time
import datetime
import logging
import subprocess
from multiprocessing import Process, Queue

UGE_SLURM = {
    'qname': 'Partition',
    'hostname': 'NodeList',
    'group': 'Group',
    'owner': 'User',
    'job_name': 'JobName',
    'job_number': 'JobID',
    'account': 'Account',
    'priority': 'Priority',
    'submission_time': 'Submit',
    'start_time': 'Start',
    'end_time': 'End',
    'failed': 'NotAvail',
    'exit_status': 'ExitCode',
    'ru_wallclock': 'End & Start', # Difference between end_time and start_time
    'ru_utime': 'UserCPU',
    'ru_stime': 'SystemCPU',
    'ru_maxrss': 'MaxRSS', # maximum resident set size
    'ru_ixrss' :'NotAvail',
    'ru_ismrss' : 'NotAvail',
    'ru_idrss': 'NotAvail',
    'ru_isrss': 'NotAvail',
    'ru_minflt': 'NotAvail', 
    'ru_majflt': 'MaxPages', #Maximum number of page faults of all tasks in job.
    'ru_nswap': 'NotAvail',
    'ru_inblock': 'NotAvail',
    'ru_oublock': 'NotAvail',
    'ru_msgsnd': 'NotAvail',
    'ru_msgrcv': 'NotAvail',
    'ru_nsignals': 'NotAvail',
    'ru_nvcsw': 'NotAvail',
    'ru_nivcsw': 'NotAvail',
    'project': 'NotAvail',
    'department': 'NotAvail',
    'granted_pe': 'NotAvail',
    'slots': 'AllocCPUs',
    'task_number': 'NTasks',
    'cpu': 'CPUTimeRAW',
    'mem': 'TresUsageInTot',
    'io': 'TresUsageInTot & TresUsageOutTot',
    'category': 'NotAvail',
    'iow': 'NotAvail',
    'pe_taskid': 'NotAvail',
    'maxvmem': 'MaxVMSize',
    'arid': 'ReservationId',
    'ar_sub_time': 'Reserved'
}

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

def fetch_slurm_job() -> list:
    accounting_fields = get_accounting_fields(UGE_SLURM)
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

    # Convert job data in string to job data in json
    jobs_dict = convert_str_json(rtn_str, accounting_fields)

    # Print accounting log
    slurm_to_uge_acct(jobs_dict, accounting_fields)

    # analyze_step_number(rtn_str, accounting_fields)

    return


def get_accounting_fields(uge_slurm: dict) -> list:
    """
    Parse uge-slurm mapping and get accounting fields used in sacct
    """
    accounting_fields = []
    for k, v in uge_slurm.items():
        if v != "NotAvail":
            if "&" in v:
                for i in v.split(' & '):
                    if i not in accounting_fields:
                        accounting_fields.append(i)
            else:
                if v not in accounting_fields:
                    accounting_fields.append(v)
    return accounting_fields


def analyze_step_number(rtn_str: str, accounting_fields: list) -> dict:
    jobs_dict = {}
    rtn_str_arr = rtn_str.splitlines()[1:]
    job_id_index = accounting_fields.index('JobID')
    for line in rtn_str_arr:
        elms = line.split("|")[:-1]
        job_id = elms[job_id_index].split('.')[0]
        if job_id not in jobs_dict:
            jobs_dict[job_id] = []
        else:
            try:
                after_dot = int(elms[job_id_index].split('.')[1])
                jobs_dict[job_id].append(after_dot)
            except:
                pass
    return jobs_dict


def convert_str_json(rtn_str: str, accounting_fields: list) -> dict:
    """
    Convert job data in string to job data in json.
    Refer to https://gist.github.com/tcooper/a69d99cc71c73a96103e5d4a33281a84
    """
    jobs_dict = {}
    rtn_str_arr = rtn_str.splitlines()[1:]
    job_id_index = accounting_fields.index('JobID')
    for line in rtn_str_arr:
        elms = line.split("|")[:-1]
        match_obj = re.match(r'([\d_]+)', elms[job_id_index])
        if match_obj:
            job_id = match_obj.group(1)

            # First instance seen. the 'master' job_completion record 
            if job_id not in jobs_dict:
                jobs_dict[job_id] = elms

            # Subsequent instances seen. the 'batch' entry or other job steps
            else:
                for i in range(len(jobs_dict[job_id])):
                    if jobs_dict[job_id][i] == "" and elms[i] != "":
                        jobs_dict[job_id][i] = elms[i]
            # try:
            #     if int(jobs_dict[job_id][17]) > 1:
            #         print(job_id)
            # except:
            #     pass

    return jobs_dict


def slurm_to_uge_acct(jobs_dict: dict, accounting_fields: list) -> None:
    for job, metrics in jobs_dict.items():
        accounting = []
        tres_in_index = accounting_fields.index('TresUsageInTot')
        tres_out_index = accounting_fields.index('TresUsageOutTot')
        tres_in = parse_tres(jobs_dict[job][tres_in_index])
        tres_out = parse_tres(jobs_dict[job][tres_out_index])
        for k, v in UGE_SLURM.items():
            if v == "NotAvail":
                value = ""
            else:
                if k == "submission_time" or k == "start_time" or k == "end_time":
                    index = accounting_fields.index(v)
                    value = str(date_to_epoch(jobs_dict[job][index]))
                elif k == "exit_status":
                    index = accounting_fields.index(v)
                    value = jobs_dict[job][index].replace(':', '-')
                elif k == "ru_wallclock":
                    end_index = accounting_fields.index('End')
                    srt_index = accounting_fields.index('Start')
                    # ru_wallclock
                    value = str( date_to_epoch(jobs_dict[job][end_index]) \
                                 - date_to_epoch(jobs_dict[job][srt_index]) )
                elif k == "ru_utime" or k == "ru_stime" or k=='ar_sub_time':
                    cpu_time_index = accounting_fields.index(v)
                    value = str(time_to_seconds(jobs_dict[job][cpu_time_index]))
                elif k == "mem":
                    value = tres_in.get('mem', "")
                elif k == "io":
                    io_in = tres_in.get('fs/disk', 0)
                    io_out = tres_out.get('fs/disk', 0)
                    value = f"{io_in}-{io_out}"
                else:
                    index = accounting_fields.index(v)
                    value = jobs_dict[job][index]
            accounting.append(value)
        accounting_str = ":".join(accounting)
        print(accounting_str)
            

def date_to_epoch(date: str) -> int:
    """Convert date string to *NIX epoch."""
    return int(time.mktime(time.strptime(date, DATETIME_FORMAT)))


def parse_tres(tres: str) -> dict:
    tres_dict = {}
    if tres:
        tres_dict = dict(map(lambda x: x.split('='), tres.split(',')))
    return tres_dict


def time_to_seconds(cpu_time: str) -> int:
    """Convert CPU time to seconds"""
    day = h = m = s = 0
    seconds = 0
    hms_sec = 0
    if '-' in cpu_time:
        day = int(cpu_time.split('-')[0])
        hms = cpu_time.split('-')[1]
    else:
        hms = cpu_time

    hms_arr = hms.split(':')
    hms_arr.reverse()
    for i, j in enumerate(hms_arr):
        n = int(float(j))
        sec = n * 60 ** i
        hms_sec += sec

    seconds = day * 24 * 60 ** 2 + hms_sec
    return seconds


def generate_job_dict(fields: list, rtn_str_arr: list) -> dict:
    """
    Generate the job dict from string using multiprocesses.
    """
    job_dict_all = {}
    queue = Queue()
    procs = []
    for rtn_str in rtn_str_arr:
        p = Process(target=convert_str_json, args=(fields, rtn_str, queue))
        procs.append(p)
        p.start()
    
    for _ in procs:
        job_dict = queue.get()
        job_dict_all.update(job_dict)

    for p in procs:
        p.join()

    return job_dict_all


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


def merge_metrics(job_metircs: dict, batch_step_metrics: dict) -> dict:
    """
    Merge metrics under JobID with metrics under batch and jobstep, update the job name
    """
    merged_metrics = {}
    for key, value in batch_step_metrics.items():
        if value == "" or key == "JobName":
            merged_metrics.update({
                key: job_metircs[key]
            })
        else:
            merged_metrics.update({
                key: value
            })
    return merged_metrics


def merge_job_dict(job_dict_all: dict, job_id_raw: str, queue: object) -> dict:
    """
    Aggregate jobid with jobid.batch and jobid.step# , and unfold several metrics under the same 
    attribute, such as "tresusageintot", "tresusageouttot".
    """
    merged_data = {}
    # only keep resource statistics under batch and jobstep, discard extern
    if ".batch" in job_id_raw or "." in job_id_raw and ".extern" not in job_id_raw:
        # merge metrics
        job_id = job_id_raw.split('.')[0]
        merged_data = merge_metrics(job_dict_all[job_id], job_dict_all[job_id_raw])
        
        # Unfold metrics in treusageintot and tresusageoutot
        folded_metrics = merged_data.get("TresUsageInTot", None)
        if folded_metrics:
            unfolded_metrics = unfold_metrics(folded_metrics, "in")
            merged_data.update(unfolded_metrics)
            merged_data.pop("TresUsageInTot")
        
        folded_metrics = merged_data.get("TresUsageOutTot", None)
        if folded_metrics:
            unfolded_metrics = unfold_metrics(folded_metrics, "out")
            merged_data.update(unfolded_metrics)
            merged_data.pop("TresUsageOutTot")

        if ".batch" in job_id_raw:
            # Update the job id if it contains batch
            merged_data.update({
                "JobID": job_id
            })
        
        # Add unique ids, which is used as unique ids for the record
        merged_data.update({
            "_id": merged_data["JobID"]
        })

    queue.put(merged_data)


def aggregate_job_data(job_dict_all: dict) -> dict:
    """
    Aggregate job dict using multiprocesses.
    """
    aggregated_job_data = []
    job_id_raw_list = job_dict_all.keys()
    queue = Queue()
    procs = []
    for job_id_raw in job_id_raw_list:
        p = Process(target=merge_job_dict, args=(job_dict_all, job_id_raw, queue))
        procs.append(p)
        p.start()
    
    for _ in procs:
        job_data = queue.get()
        if job_data:
            aggregated_job_data.append(job_data)

    for p in procs:
        p.join()

    return aggregated_job_data


if __name__ == '__main__':
    fetch_slurm_job()