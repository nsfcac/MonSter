import logging
import requests
from requests.adapters import HTTPAdapter
from pathlib import Path


def fetch_jobscript(uge_config: dict, job_id: str) -> str:
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
        
        print(job_info['queue'])
        
        # Extract exec_host, work_dir, cmd from job info
        try:
            exec_host = job_info['queue'].split('@')
        except Exception:
            return None

        print(exec_host)
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


def fetch(uge_config:dict, url: str) -> list:
    """
    Use requests to query the url
    """
    metrics = []
    adapter = HTTPAdapter(max_retries=uge_config["max_retries"])
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url, verify = uge_config["ssl_verify"],
                timeout = (uge_config["timeout"]["connect"], uge_config["timeout"]["read"])
            )
            metrics = response.json()
        except Exception as err:
            logging.error(f"fetch_jobscript : fetch error : {err}")
    return metrics