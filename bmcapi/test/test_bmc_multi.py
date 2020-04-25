import json
import time

import logging
import asyncio
from aiohttp import request

from aiomultiprocess import Pool

from process_bmc import process_bmc_metrics


logging.basicConfig(
    level=logging.DEBUG,
    filename='test_bmcapi.log',
    filemode='w',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

config = {
    "user": "password",
    "password": "monster",
    "timeout": 12,
    # "max_retries": 1,
    "ssl_verify": False,
    "hostlist": "../../hostlist"
}


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 11.57s
    """

    all_bmc_points = []

    # urls = generate_urls(hostlist)
    urls = ["https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/" for host in hostlist]

    loop = asyncio.get_event_loop()

    epoch_time = int(round(time.time() * 1000000000))

    # all_bmc_points = process_bmc_metrics(urls, bmc_metrics, epoch_time)
    # print(len(bmc_metrics))
    # print(json.dumps(all_bmc_points, indent=4))

    print(json.dumps(bmc_metrics, indent=4))

    # print(len(bmc_metrics))
    # valid = 0
    # for index, url in enumerate(urls):
    #     if bmc_metrics[index]:
    #         valid += 1
    # print("Valid metrics: ", valid)

    return


async def download_bmc(urls: list, conn: object, auth: object, config: dict) -> None:
    tasks = []
    try:    
       async with Pool() as pool:
           result = await pool.map(fetch, urls)
    except:
        return None


# def return_last_value(retry_state):
#     url = retry_state.args[0]
#     logging.error("Cannot connect to %s", url)
#     return None


# @tenacity.retry(stop=tenacity.stop_after_attempt(3),
#                 # wait=tenacity.wait_random(min=1, max=3),
#                 retry_error_callback=return_last_value,)     

async def fetch(url: str, config: dict) -> dict:
    async with request("GET", url) as response:
        return await response.json()


def generate_urls(hostlist:list) -> list:
    # For testing
    # curl --user password:monster https://10.101.1.1/redfish/v1/Chassis/System.Embedded.1/Thermal/ -k
    urls = []
    # Thermal URLS
    for host in hostlist:
        thermal_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/"
        urls.append(thermal_url)
    # # Power
    # for host in hostlist:
    #     power_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"
    #     urls.append(power_url)
    # # BMC health
    # for host in hostlist:
    #     bmc_health_url = "https://" + host + "/redfish/v1/Managers/iDRAC.Embedded.1"
    #     urls.append(bmc_health_url)
    # # System health
    # for host in hostlist:
    #     system_health_url = "https://" + host + "/redfish/v1/Systems/System.Embedded.1"
    #     urls.append(system_health_url)
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