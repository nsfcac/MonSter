import json
import time
import requests
# import multiprocessing
import asyncio
import aiohttp

from itertools import repeat
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter 

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# from redfishapi.process_bmc import process_bmc

# For test single function
from process_bmc import process_bmc

config = {
    "user": "password",
    "password": "monster",
    "timeout": {
        "connect": 5,
        "total": 15
    },
    "max_retries": 1,
    "ssl_verify": False,
    "hostlist": "../hostlist"
}


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 11.57s
    """

    conn = aiohttp.TCPConnector(limit=0, limit_per_host=0, ssl=config["ssl_verify"])
    auth = aiohttp.BasicAuth(config["user"], password=config["password"])
    timeout = aiohttp.ClientTimeout(total=config["timeout"]["total"], connect=config["timeout"]["connect"])

    urls = generate_urls(hostlist)

    loop = asyncio.get_event_loop()

    future = asyncio.ensure_future(download_bmc(urls, conn, auth, timeout))
    loop.run_until_complete(future)

    return 


async def fetch(url: str, session:object) -> dict:
    async with session.get(url) as response:
        return await response.json()


async def download_bmc(urls: list, conn: object, auth: object, timeout: object) -> None:
    tasks = []
    async with aiohttp.ClientSession(connector= conn, auth=auth) as session:
        for url in urls:
            task = asyncio.ensure_future(fetch(url, session))
            tasks.append(task)
        
        responses =  await asyncio.gather(*tasks)

        print(json.dumps(responses, indent=4))


def generate_urls(hostlist:list) -> list:
    urls = []
    # Thermal URLS
    for host in hostlist:
        thermal_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/"
        urls.append(thermal_url)
    # Power
    for host in hostlist:
        power_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"
        urls.append(power_url)
    # BMC health
    for host in hostlist:
        bmc_health_url = "https://" + host + "/redfish/v1/Managers/iDRAC.Embedded.1"
        urls.append(bmc_health_url)
    # System health
    for host in hostlist:
        system_health_url = "https://" + host + "/redfish/v1/Systems/System.Embedded.1"
        urls.append(system_health_url)
    return urls


def get_hostlist(hostlist_dir: str) -> list:
    """
    Parse host IP from file
    """
    hostlist = []
    try:
        with open(hostlist_dir, "r") as hostlist_file:
            hostname_list = hostlist_file.read()[1:-1].split(", ")
            hostlist = [host.split(":")[0][1:] for host in hostname_list]
    except Exception as err:
        print(err)
    return hostlist


hostlist = get_hostlist(config["hostlist"])[:10]
# hostlist = ["10.101.1.1"]

fetch_bmc(config, hostlist)