# -*- coding: utf-8 -*-
"""
    This module talks to the quanah login node and gets the scripts of the submitted
    jobs. Since we are not able to call the UGE API from the quanah login node, we use
    Paramiko to SSH and transfer the script files from the quanah login node.

    Jie Li (jie.li@ttu.edu)
"""
import json
import paramiko
import logging
import asyncio
import aiohttp
import multiprocessing

from aiohttp import ClientSession

import sys
sys.path.append('../')

from ugeapi.fetch_uge import fetch_jobs

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
        # # Get the new job ids
        curr_joblist = fetch_jobs(uge_config)
        for job_id in curr_joblist:
            if job_id not in all_joblist:
                joblist.append(job_id)

        if joblist:
            all_jobinfo = fetch_jobinfo(uge_config, joblist)
            # Initialize paramiko for SSH
            ssh_key = paramiko.RSAKey.from_private_key_file('/home/monster/.ssh/id_rsa')
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname='quanah.hpcc.ttu.edu', username='monster', pkey=ssh_key)
            
            # Copy jobs scripts from remote server (quanah)
            ftp_client = ssh_client.open_sftp()

            for job in all_jobinfo:
                job_id = job['id']
                job_info = job['info']
                saved_path = '/home/monster/MonSter/ugeapi/data/jobscripts/' + job_id

                try:
                    if '.' in job_id:
                        job_id = job_id.split('.')[0]
                        saved_path = '/home/monster/MonSter/ugeapi/data/jobscripts/' + job_id
                    script_path = get_script_path_exec(uge_config, job_id, job_info)
                    if script_path:
                        # print(f"job_id : {job_id}, script_path : {script_path}")
                        ftp_client.get(script_path, saved_path)
                except Exception as err:
                    print(f"job_id : {job_id}, exec: err : {err}")
                    try:
                        script_path = get_script_path_work(uge_config, job_info)
                        if script_path:
                            # print(f"job_id : {job_id}, script_path : {script_path}")
                            ftp_client.get(script_path, saved_path)
                    except Exception as err:
                        print(f"job_id : {job_id}, work: err : {err}")

            ftp_client.close()
            ssh_client.close()
    except Exception as err:
        logging.error(f"fetch Job script error: {err}")
        return None


def fetch_jobinfo(uge_config: dict, joblist: list) -> list:
    """
    Fetch all jobs info
    """
    all_jobinfo = []
    try:
        api = uge_config["api"]
        urls = [f"http://{api['hostname']}:{api['port']}{api['job_list']}/{job_id}" for job_id in joblist]

        connector = aiohttp.TCPConnector(verify_ssl= False)
        timeout = aiohttp.ClientTimeout(15, 45)

        # Asyncio fetch all jobs info
        loop = asyncio.get_event_loop()
        raw_jobinfo = loop.run_until_complete(asyncio_fetch_jobinfo(urls, connector, timeout))
        loop.close()

        # Process jobs info
        with multiprocessing.Pool() as pool:
            all_jobinfo = pool.map(process_jobinfo, raw_jobinfo)

    except Exception as err:
        logging.error(f"fetch_jobscript : fetch_jobinfo : {err}")
    return all_jobinfo


async def asyncio_fetch(url: str, session: ClientSession) -> dict:
    """
    Asyncio fetch each job info
    """
    json = {}
    job_id = url.split('/')[-1]
    retry = 0
    try:
        resp = await session.request(method='GET', url=url)
        resp.raise_for_status()
        json = await resp.json()
        return {"job": job_id, "info": json}
    except (TimeoutError):
        retry += 1
        if retry >= 3:
            logging.error(f"Timeout Error : cannot fetch data from {job_id} : {url}")
            return {"job": job_id, "info": json}
        return await asyncio_fetch(url, session)
    except Exception as err:
        logging.error(f"fetch_jobscript : asyncio_fetch : {err}")
        return {"job": job_id, "info": json}


async def asyncio_fetch_jobinfo(urls: list, connector: object, timeout: object) -> list:
    """
    Asyncio fetch all jobs info
    """
    try:
        async with ClientSession(connector = connector, 
                                 timeout = timeout) as session:
            tasks = []
            for i, url in enumerate(urls):
                tasks.append(asyncio_fetch(url=url, session=session))
            return await asyncio.gather(*tasks)
    except Exception as err:
        logging.error(f"fetch_jobscript : asyncio_fetch_jobinfo : {err}")


def process_jobinfo(job: dict) -> dict:
    """
    Process job info, extract exec_host, work_dir, cmd from job info
    """
    processed_jobinfo = {}

    job_id = job['job']
    job_info = job['info']

    exec_host = None
    work_dir = None
    cmd = None

    if job_info['queue']:
        exec_host = job_info['queue'].split('@')[1]

    for env in job_info['jobEnvironment']:
        if env['name'] == 'PWD':
            work_dir = env['value']

    cmd = job_info['command']

    # Do no proceed for the following typs of jobs
    if cmd and any(q_cmd in cmd.lower() for q_cmd in ["qlogin", "qrsh"]):
        return None

    job_info = {
        'exec_host': exec_host,
        'work_dir': work_dir,
        'cmd': cmd
    }

    processed_jobinfo = {
        'id': job_id,
        'info': job_info
    }
        
    return processed_jobinfo
    

def get_script_path_exec(uge_config: dict, job_id: str, job_info: str) -> str:
    """
    Generate job script path.
    """
    job_script_path = None
    try:
        exec_host = job_info['exec_host']
        cmd = job_info['cmd']

        # Do no proceed for the following typs of job
        if cmd and any(q_cmd in cmd.lower() for q_cmd in ["qlogin", "qrsh"]):
            return None

        # The location of UGE Spool Directory for the cluster
        uge_spool_dir = uge_config["spool_dirs"]
        if uge_spool_dir and exec_host:
            exec_host = exec_host.split('.')[0]
            # Find the job script directory
            job_script_path = f"{uge_spool_dir}/{exec_host}/{exec_host}/job_scripts/{job_id}"
            return job_script_path
            
    except Exception as err:
        logging.error(f"fetch_jobscript : get_script_path_exec : {err}")
    return None


def get_script_path_work(uge_config: dict, job_info: dict) -> str:
    """
    Generate job script path.
    """
    job_script_path = None
    try:
        work_dir = job_info['work_dir']
        cmd = job_info['cmd']

        # Do no proceed for the following typs of job
        if cmd and any(q_cmd in cmd.lower() for q_cmd in ["qlogin", "qrsh"]):
            return None

        if work_dir and cmd:
            script_file = cmd.split()[-1]
            # Look into user's directory and try to find the script file
            job_script_path = f"{work_dir}/{script_file}"
            return job_script_path
            
    except Exception as err:
        logging.error(f"fetch_jobscript : get_script_path_work : {err}")
    return None


def get_script_path(uge_config: dict, job_id: str) -> str:
    """
    Get job script from spool directory or from the script file under user directory
    Refer to HPC_Provenance developed by Misha ahmadian (misha.ahmadian@ttu.edu)
    """
    job_script = ""
    try:
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