import requests
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter

from convert import get_hostip

config = {
    "host": "129.118.104.35",
    "port": "8182",
    "user": "username",
    "password": "password",
    "timeout": [2, 6],
    "max_retries": 3,
    "ssl_verify": False
}

ugeapi_adapter = HTTPAdapter(config["max_retries"])
session = requests.Session()

def fetch_uge(config: object, session: object, ugeapi_adapter: object) -> object:
    """
    Fetch metrics from UGE api
    """
    # Get executing hosts
    # exechost = get_exechosts(config, session, ugeapi_adapter)
    jobs = get_jobs(config, session, ugeapi_adapter)
    print(len(jobs))

def get_exechosts(config: object, session: object, ugeapi_adapter: object) -> object:
    """
    Get executing hosts
    """
    exechosts = []
    exechosts_url = "http://" + config["host"] + ":" + config["port"] + "/exechosts" 
    session.mount(exechosts_url, ugeapi_adapter)
    try:
        exechosts_response = session.get(
            exechosts_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"][0], config["timeout"][1])
        )
        exechosts = [get_hostip(h) for h in exechosts_response.json() if '-' in h]

    except ConnectionError as err:
        print(err)
    return exechosts

def get_jobs(config: object, session: object, ugeapi_adapter: object) -> object:
    """
    Get job list
    """
    jobs = []
    jobs_url = "http://" + config["host"] + ":" + config["port"] + "/jobs" 
    session.mount(jobs_url, ugeapi_adapter)
    try:
        jobs_response = session.get(
            jobs_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"][0], config["timeout"][1])
        )
        jobs = [job for job in jobs_response.json()]
        print(jobs)
    except ConnectionError as err:
        print(err)

fetch_uge(config, session, ugeapi_adapter)
