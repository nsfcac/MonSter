import json
import time
import logging

import requests
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter

import multiprocessing
from concurrent.futures import as_completed
# from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from requests_futures.sessions import FuturesSession

from process_bmc import process_bmc_metrics

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    "timeout": {
        "connect": 5,
        "read": 15
    },
    "max_retries": 3,
    "ssl_verify": False,
    "hostlist": "../../hostlist"
}


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 11.57s
    """

    # bmc_metrics = {}
    bmc_metrics = {}
    all_bmc_points = []

    cpu_count = multiprocessing.cpu_count()
    bmcapi_adapter = HTTPAdapter(config["max_retries"])
    urls = generate_urls(hostlist)

    with requests.Session() as session:
        future_session = FuturesSession(session=session, executor=ProcessPoolExecutor(max_workers=cpu_count))
        futures = [get_bmc_detail(config, url, future_session, bmcapi_adapter) for url in urls]
        for index, future in enumerate(as_completed(futures)):
            host_ip = urls[index].split("/")[2]
            resp = future.result()
            if resp:
                bmc_metrics[host_ip] = resp.json()
            else:
                bmc_metrics[host_ip] = None

    # print(bmc_metrics)

    print(json.dumps(bmc_metrics, indent=4))
    valid = 0
    for key, values in bmc_metrics.items():
        if values:
            valid += 1
    print("Valid metrics: ", valid)
    return


def generate_urls(hostlist:list) -> list:
    # For testing
    # curl --user password:monster https://10.101.1.1/redfish/v1/Chassis/System.Embedded.1/Thermal/ -k
    urls = []
    # Thermal URLS
    for host in hostlist:
        thermal_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/"
        urls.append(thermal_url)
    # Power
    for host in hostlist:
        power_url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"
        urls.append(power_url)
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


def get_bmc_detail(config: dict, bmc_url: str, session: object, bmcapi_adapter: object) -> None:
    """
    Get BMC detail
    """
    bmc_response = None
    session.mount(bmc_url, bmcapi_adapter)
    try:
        bmc_response = session.get(
            bmc_url, verify = config["ssl_verify"],
            auth=(config["user"], config["password"]),
            timeout = (config["timeout"]["connect"], config["timeout"]["read"])
        )
    except:
        logging.error("Cannot get BMC metrics from: %s", bmc_url)
    return bmc_response


hostlist = get_hostlist(config["hostlist"])
# hostlist = ["10.101.1.1"]

fetch_bmc(config, hostlist)