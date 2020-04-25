import json
import time
import requests
import logging
import asyncio
import aiohttp
from aiohttp_retry import RetryClient
import async_timeout
import tenacity
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

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

    conn = aiohttp.TCPConnector(limit=0, limit_per_host=0, ssl=config["ssl_verify"])
    auth = aiohttp.BasicAuth(config["user"], password=config["password"])
    # timeout = aiohttp.ClientTimeout(total=60*5)

    # urls = generate_urls(hostlist)
    urls = ["https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/" for host in hostlist]

    loop = asyncio.get_event_loop()

    epoch_time = int(round(time.time() * 1000000000))

    future = asyncio.ensure_future(download_bmc(urls, conn, auth, config))
    bmc_metrics = loop.run_until_complete(future)

    # all_bmc_points = process_bmc_metrics(urls, bmc_metrics, epoch_time)
    # print(len(bmc_metrics))
    # print(json.dumps(all_bmc_points, indent=4))

    print(json.dumps(bmc_metrics, indent=4))

    print(len(bmc_metrics))
    # valid = 0
    # for index, url in enumerate(urls):
    #     if bmc_metrics[index]:
    #         valid += 1
    # print("Valid metrics: ", valid)

    return


async def download_bmc(urls: list, conn: object, auth: object, config: dict) -> None:
    tasks = []
    try:    
        async with RetryClient(connector= conn, auth=auth) as client:
        # async with aiohttp.ClientSession(connector= conn, auth=auth) as session:
            for url in urls:
                task = asyncio.ensure_future(fetch(url, client, config))
                tasks.append(task)
            
            responses =  await asyncio.gather(*tasks)
            return responses
    except:
        return None


# def return_last_value(retry_state):
#     url = retry_state.args[0]
#     logging.error("Cannot connect to %s", url)
#     return None


# @tenacity.retry(stop=tenacity.stop_after_attempt(3),
#                 # wait=tenacity.wait_random(min=1, max=3),
#                 retry_error_callback=return_last_value,)     

async def fetch(url: str, client:object, config: dict) -> dict:
    async with client.get(url, retry_attempts=3, retry_max_timeout=40) as response:
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