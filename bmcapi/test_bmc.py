import json
import time
import requests
import logging

import asyncio
import aiohttp
import async_timeout

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import tenacity

from itertools import repeat
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter 

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# AbstractEventLoop.set_debug()

logging.basicConfig(
    level=logging.DEBUG,
    filename='bmcapi.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

# from redfishapi.process_bmc import *

# For test single function
# from process_bmc import *

config = {
    "user": "password",
    "password": "monster",
    "timeout": 15,
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
    # timeout = aiohttp.ClientTimeout(total=config["timeout"]["total"], connect=config["timeout"]["connect"])

    urls = generate_urls(hostlist)

    loop = asyncio.get_event_loop()

    logging.info("Start fetching BMC metrics")

    future = asyncio.ensure_future(download_bmc(urls, conn, auth, config))
    bmc_metrics = loop.run_until_complete(future)
    loop.close()

    logging.info("Finish fetching BMC metrics")

    print(json.dumps(bmc_metrics, indent=4))

    return 


@tenacity.retry(stop=tenacity.stop_after_attempt(3),
                wait=tenacity.wait_random(min=1, max=2))
async def fetch(url: str, session:object, config: dict) -> dict:
    try:
        timeout = config["timeout"]
        with async_timeout.timeout(timeout):
            async with session.get(url) as response:
                return await response.json()
    except asyncio.TimeoutError:
        logging.error("Connection timeout: %s", url)


async def download_bmc(urls: list, conn: object, auth: object, config: dict) -> None:
    tasks = []
    try:
        async with aiohttp.ClientSession(connector= conn, auth=auth) as session:
            for url in urls:
                try:
                    task = asyncio.ensure_future(fetch(url, session, config))
                    tasks.append(task)
                except tenacity.RetryError:
                    logging.error("Cannot connect to remote BMC after 3 retries: %s", url)
                    task.append(None)
            
            responses =  await asyncio.gather(*tasks)
            return responses
    except:
        
        return None


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


hostlist = get_hostlist(config["hostlist"])
# hostlist = ["10.101.1.1"]

fetch_bmc(config, hostlist)