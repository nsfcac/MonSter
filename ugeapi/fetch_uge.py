import requests
import multiprocessing
from itertools import repeat
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter

config = {
    "host": "129.118.104.35",
    "port": "8182",
    "user": "username",
    "password": "password",
    "timeout": [2, 6],
    "max_retries": 3,
    "ssl_verify": False,
}

def fetch_uge(config: object) -> object:
    """
    Fetch metrics from UGE api
    """
    # Get cpu counts
    cpu_count = multiprocessing.cpu_count()
    uge_url = "http://" + config["host"] + ":" + config["port"]
    ugeapi_adapter = HTTPAdapter(config["max_retries"])

    with requests.Session() as session:
        # Get executing hosts and jobs running on the cluster
        exechosts = get_exechosts(uge_url, session, ugeapi_adapter)
        jobs = get_jobs(uge_url, session, ugeapi_adapter)

        # Get hosts detail in parallel 
        pool_host_args = zip(repeat(uge_url), repeat(session), repeat(ugeapi_adapter), exechosts)
        with multiprocessing.Pool(processes=cpu_count) as pool:
            host_data = pool.starmap(get_host_detail, pool_host_args)

        # Get jobs detail in parallel
        pool_job_args = zip(repeat(uge_url), repeat(session), repeat(ugeapi_adapter), jobs)
        with multiprocessing.Pool(processes=cpu_count) as pool:
            job_data = pool.starmap(get_job_detail, pool_job_args)

        #-----------------------------------------------------------------------
        # Get nodes detail in sequential
        # host_data = []
        # for host in exechosts:
        #     host_data.append(get_host_detail(uge_url, session, ugeapi_adapter, host))

        # Get jobs detail in sequential
        # job_data = []
        # for job in jobs:
        #     job_data.append(get_job_detail(uge_url, session, ugeapi_adapter, job))
        #-----------------------------------------------------------------------


def get_exechosts(uge_url: str, session: object, ugeapi_adapter: object) -> list:
    """
    Get executing hosts
    """
    exechosts = []
    exechosts_url = uge_url + "/exechosts" 
    session.mount(exechosts_url, ugeapi_adapter)
    try:
        exechosts_response = session.get(
            exechosts_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"][0], config["timeout"][1])
        )
        # exechosts = [get_hostip(h) for h in exechosts_response.json() if '-' in h]
        exechosts = [host for host in exechosts_response.json()]
    except ConnectionError as err:
        print(err)
    return exechosts


def get_host_detail(uge_url: str, session: object, ugeapi_adapter: object, host_id: str) -> object:
    """
    Get host details
    """
    host = {}
    host_url = uge_url + "/hostsummary" + "/" + host_id
    session.mount(host_url, ugeapi_adapter)
    try:
        host_response = session.get(
            host_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"][0], config["timeout"][1])
        )
        host = host_response.json()
    except ConnectionError as err:
        print(err)
    return host


def get_jobs(uge_url: str, session: object, ugeapi_adapter: object) -> list:
    """
    Get running job list
    """
    jobs = []
    jobs_url = uge_url + "/jobs" 
    session.mount(jobs_url, ugeapi_adapter)
    try:
        jobs_response = session.get(
            jobs_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"][0], config["timeout"][1])
        )
        jobs = [job for job in jobs_response.json()]
    except ConnectionError as err:
        print(err)
    return jobs


def get_job_detail(uge_url: str, session: object, ugeapi_adapter: object, job_id: str) -> object:
    """
    Get job details
    """
    job = {}
    job_url = uge_url + "/jobs" + "/" + job_id
    session.mount(job_url, ugeapi_adapter)
    try:
        job_response = session.get(
            job_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"][0], config["timeout"][1])
        )
        job = job_response.json()
    except ConnectionError as err:
        print(err)
    return job

fetch_uge(config)
