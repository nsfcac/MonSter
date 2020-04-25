import json
import time
import logging
import multiprocessing

from itertools import repeat
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth

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
    "timeout": {
        "connect": 15,
        "read": 40
    },
    "max_retries": 3,
    "ssl_verify": False,
    "hostlist": "../../hostlist"
}


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 11.57s
    """

    # bmc_metrics = []
    bmc_details = []
    # all_bmc_points = []

    cpu_count = multiprocessing.cpu_count()
    urls = generate_urls(hostlist)
    bmcapi_adapter = HTTPAdapter(config["max_retries"])

    with requests.Session() as session:
        # epoch_time = int(round(time.time() * 1000000000))
        get_bmc_detail_args = zip(repeat(config), urls, repeat(session), repeat(bmcapi_adapter))
        with multiprocessing.Pool(processes=cpu_count) as pool:
            bmc_details = pool.starmap(get_bmc_detail, get_bmc_detail_args)
        print(json.dumps(bmc_details, indent=4))

    valid = 0
    for detail in bmc_details:
        if detail:
            valid += 1
    print("Valid metrics: ", valid)

    return


def get_bmc_detail(config: dict, bmc_url: str, session: object, bmcapi_adapter: object) -> dict:
    session.mount(bmc_url, bmcapi_adapter)
    bmc_metric = {}
    try:
        bmc_response = session.get(
            bmc_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"]["connect"], config["timeout"]["read"]),
            auth=HTTPBasicAuth(config["user"], config["monster"])
        )
        bmc_metric = bmc_response.json()
    except Exception as err:
        print(err)
        logging.error("Cannot get BMC details from: %s", bmc_url)
    return bmc_metric

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


# hostlist = get_hostlist(config["hostlist"])
hostlist = ["10.101.1.1"]

fetch_bmc(config, hostlist)