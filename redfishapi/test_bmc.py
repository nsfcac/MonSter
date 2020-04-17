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
        "connect": 2,
        "total": 8
    },
    "max_retries": 3,
    "ssl_verify": False,
    "hostlist": "../hostlist"
}


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 11.57s
    """
    bmc_metrics = []
    try:
        conn = aiohttp.TCPConnector(limit=config["max_retries"], ssl=config["ssl_verify"])
        auth = aiohttp.BasicAuth(config["user"], config["password"])
        timeout = aiohttp.ClientTimeout(total=config["timeout"]["total"], connect=config["timeout"]["connect"])

        urls = generate_urls(hostlist)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(download_all_bmc(urls, conn, auth, timeout))

    except Exception as err:
        print("fetch_bmc ERROR: ", end = " ")
        print(err)
    
    return 


async def download_bmc(session: object, url: str) -> None:
    async with session.get(url) as response:
        return await response.json()


async def download_all_bmc(urls: list, conn: object, auth: object, timeout: object) -> None:
    async with aiohttp.ClientSession(connector= conn, auth=auth, timeout=timeout) as session:
        for url in urls:
            metric = await download_bmc(session, url)
            print(url)
            print(json.dumps(metric, indent=4))


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


# hostlist = get_hostlist(config["hostlist"])
hostlist = ["10.101.1.1"]

fetch_bmc(config, hostlist)