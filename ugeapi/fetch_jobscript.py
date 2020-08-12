# -*- coding: utf-8 -*-
"""
    This module talks to the quanah login node and gets the scripts of the submitted
    jobs. Since we are not able to call the UGE API from the quanah login node, we use
    Paramiko to SSH and transfer the script files from the quanah login node.

    Jie Li (jie.li@ttu.edu)
"""
import paramiko
import logging
import requests
from pathlib import Path
from requests.adapters import HTTPAdapter

import sys
sys.path.append('../')

from ugeapi.fetch_uge import fetch_jobs, fetch

all_joblist = []


def fetch_jobscript(uge_config: dict) -> list:
    """
    Get current running jobs and compare them to the stored job list, if the job
    script is not fetched, use Paramkio to copy the script file to local machine
    and then process the script. 
    """
    global all_joblist
    joblist = []
    try:
        # Initialize paramiko for SSH
        ssh_key = paramiko.RSAKey.from_private_key_file('~/.ssh/id_rsa.pub')
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname='quanah.hpcc.ttu.edu', username='monster', pkey=ssh_key)
        
        stdin,stdout,stderr=ssh_client.exec_command("ls")

        print(stdout.readlines())

        ssh_client.close()
        # # Get the new job ids
        # curr_joblist = fetch_jobs(uge_config)
        # for job_id in curr_joblist:
        #     if job_id not in all_joblist:
        #         joblist.append(job_id)
        
        # Copy jobs scripts from remote server (quanah)
    except Exception as err:
        logging.error(f"fetch Job script error: {err}")
        return None


def copy_jobscript(uge_config: dict, job_id: str, ssh_client: object) -> None:
    """
    Copy the target job scripts to local.
    """
    try:
        # Get job info
        api = uge_config["api"]
        job_info_url = f"http://{api['hostname']}:{api['port']}{api['job_list']}/{job_id}"
        job_info = fetch(uge_config, job_info_url)

        if not job_info:
            return None

        # Extract exec_host, work_dir, cmd from job info
        try:
            exec_host = job_info['queue'].split('@')[1]
        except Exception:
            return None

        try:
            for env in job_info['jobEnvironment']:
                if env['name'] == 'PWD':
                    work_dir = env['value']
        except Exception:
            pass
        cmd = job_info['command']

        # Do no proceed for the following typs of job
        if cmd and any(q_cmd in cmd.lower() for q_cmd in ["qlogin", "qrsh"]):
            return None

        # The location of UGE Spool Directory for the cluster
        uge_spool_dir = uge_config["spool_dirs"]
        if uge_spool_dir and exec_host:
            exec_host = exec_host.split('.')[0]
            # Find the job script directory
            job_script_path = f"{uge_spool_dir}/{exec_host}/{exec_host}/job_scripts/{job_id}"
        
    except Exception as err:
        logging.error(f"Copy Job script error: {err}")
        return None
    return


def fetch_script(uge_config: dict, job_id: str) -> str:
    """
    Get job script from spool directory or from the script file under user directory
    Refer to HPC_Provenance developed by Misha ahmadian (misha.ahmadian@ttu.edu)
    """
    job_script = ""
    try:
        api = uge_config["api"]
        job_info_url = f"http://{api['hostname']}:{api['port']}{api['job_list']}/{job_id}"

        job_info = fetch(uge_config, job_info_url)

        if not job_info:
            return None
        
        # Extract exec_host, work_dir, cmd from job info
        try:
            exec_host = job_info['queue'].split('@')[1]
        except Exception:
            return None

        try:
            for env in job_info['jobEnvironment']:
                if env['name'] == 'PWD':
                    work_dir = env['value']
        except KeyError:
            pass
        cmd = job_info['command']

        # Do no proceed for the following yteps of job
        if cmd and any(q_cmd in cmd.lower() for q_cmd in ["qlogin", "qrsh"]):
            return None
        
        # The location of UGE Spool Directory for the cluster
        uge_spool_dir = uge_config["spool_dirs"]
        # First check the spool directory for the script file. That's the most reliable version
        if uge_spool_dir and exec_host:
            exec_host = exec_host.split('.')[0]
            # Find the spool directory
            uge_job_script = Path(uge_spool_dir).joinpath(exec_host).joinpath("job_scripts").joinpath(job_id)
            # print(uge_job_script)
            if not uge_job_script.exists():
                uge_job_script = Path(uge_spool_dir).joinpath(exec_host).joinpath(exec_host)\
                    .joinpath("job_scripts").joinpath(job_id)
                if not uge_job_script.exists():
                    uge_job_script = None
            
            # Get Job submission script content
            if uge_job_script:
                with open(uge_job_script.as_posix(), mode='r') as job_script:
                    return job_script.read()

        # If job submission script was not available in UGE Spool directory, then it means
        # the job has been finished and may be able to find the script file under user directory
        if work_dir and cmd:
            script_file = cmd.split()[-1]
            # Look into user's directory and try to find the script file
            uge_job_script = Path(work_dir).joinpath(script_file)
            if uge_job_script.exists():
                with open(uge_job_script.as_poxis(), mode='r') as job_script:
                    return job_script.read()
    except Exception as err:
        logging.error(f"Fetch Job script error: {err}")
        return None