import json
import time
import logging
import multiprocessing
import threading
thread_local = threading.local()

from queue import Queue
from itertools import repeat
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth

from bmcapi.process_bmc import process_bmc_metrics

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def fetch_bmc(config: object, hostlist: list) -> object:
    """
    Fetch bmc metrics from Redfish, average query and process time is: 49.97s
    """

    all_bmc_points = []

    cores= multiprocessing.cpu_count()
    urls = generate_urls(hostlist)

    # Paritition urls
    urls_set = []
    urls_per_core = len(urls) // cores
    surplux_urls = len(urls) % cores
    increment = 1
    for i in range(cores):
        if(surplux_urls != 0 and i == (cores-1)):
            urls_set.append(urls[i * urls_per_core:])
        else:
            urls_set.append(urls[i * urls_per_core : increment * urls_per_core])
            increment += 1

    bmc_metrics = []
    epoch_time = int(round(time.time() * 1000000000))

    # Query BMC metrics
    with multiprocessing.Pool(processes=cores) as pool:
        responses = [pool.apply_async(get_bmc_thread, args = (config, bmc_urls)) for bmc_urls in urls_set]
    
        for response in responses:
            bmc_metrics += response.get()
    
    # Generate data points
    process_bmc_args = zip(bmc_metrics, repeat(epoch_time))
    with multiprocessing.Pool(processes=cores) as pool:
        bmc_points_set = pool.starmap(process_bmc_metrics, process_bmc_args)

    for points_set in bmc_points_set:
        all_bmc_points += points_set

    return all_bmc_points


def get_bmc_thread(config: dict, bmc_urls: list) -> list:
    q = Queue(maxsize=0)
    bmc_metrics = [{} for url in bmc_urls]
    try:
        for i in range(len(bmc_urls)):
            q.put((i, bmc_urls[i]))
        
        for i in range(len(bmc_urls)):
            worker = threading.Thread(target=get_bmc_detail, args=(q, config, bmc_metrics))
            worker.setDaemon(True)
            worker.start()
        
        q.join()

    except Exception as err:
        logging.error(err)
    
    return bmc_metrics

def get_bmc_detail(q: object, config: dict, bmc_metrics: list) -> None:
    while not q.empty():
        work = q.get()
        index = work[0]
        bmc_url = work[1]
        
        bmcapi_adapter = HTTPAdapter(max_retries=config["max_retries"])
        host_ip = bmc_url.split("/")[2]
        feature = bmc_url.split("/")[-2]
        details = {}
        try:
            session = get_session()
            session.mount(bmc_url, bmcapi_adapter)
            bmc_response = session.get(
                bmc_url, verify = config["ssl_verify"], 
                timeout = (config["timeout"]["connect"], config["timeout"]["read"]),
                auth=HTTPBasicAuth(config["user"], config["password"])
            )
            details = bmc_response.json()
        except:
            logging.error("Cannot get BMC metrics from: %s", bmc_url)

        metric = {
            "host": host_ip,
            "feature": feature,
            "details": details
        }
        
        bmc_metrics[index] = metric
        q.task_done()
    return True

def generate_urls(hostlist:list) -> list:
    # Testing cmd
    # curl --user root:nivipnut https://10.101.1.1/redfish/v1/Chassis/System.Embedded.1/Thermal/ -k
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


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session