import json
import time
import logging

import grequests

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
    "max_retries": 3,
    "ssl_verify": False,
    "hostlist": "../hostlist"
}


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 11.57s
    """

    bmc_metrics = {}
    all_bmc_points = []

    urls = generate_urls(hostlist)

    responses = (grequests.get(url) for url in urls)
    results = grequests.map(responses)

    print(json.dumps(results, indent=4))

    # print(json.dumps(all_bmc_points, indent=4))

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


hostlist = get_hostlist(config["hostlist"])
# hostlist = ["10.101.1.1"]

fetch_bmc(config, hostlist)