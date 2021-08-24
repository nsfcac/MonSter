from idrac.ProcessThermal import ProcessThermal
from idrac.ProcessHealth import ProcessHealth
from idrac.ProcessPower import ProcessPower
from utils.parse_nodelist import parse_nodelist
from utils.parallel_fetch import parallel_fetch

from itertools import repeat
import multiprocessing
import logging


def fetch_metrics(idrac_config: dict) -> list:
    bmc_datapoints = []
    try:
        thermal_api = idrac_config["apis"]["thermal"]
        power_api = idrac_config["apis"]["power"]
        # bmc_health_api = idrac_config["apis"]["bmc_health"]
        # sys_health_api = idrac_config["apis"]["sys_health"]
        nodes = parse_nodelist(idrac_config["nodelist"])

        thermal_urls = ["https://" + node + thermal_api for node in nodes]
        power_urls = ["https://" + node + power_api for node in nodes]
        # bmc_health_urls = ["https://" + node +
        # #                    bmc_health_api for node in nodes]
        # sys_health_urls = ["https://" + node +
        #                    sys_health_api for node in nodes]

        cores = multiprocessing.cpu_count()

        # Parallel fetch metrics
        thermal_metrics = parallel_fetch(
            idrac_config, thermal_urls, nodes, cores)
        power_metrics = parallel_fetch(idrac_config, power_urls, nodes, cores)
        # bmc_health_metrics = parallel_fetch(
        #     idrac_config, bmc_health_urls, nodes, cores)
        # sys_health_metrics = parallel_fetch(
        #     idrac_config, sys_health_urls, nodes, cores)

        # Process metrics
        thermal_datapoints = parallel_process(thermal_metrics, "thermal")
        power_datapoints = parallel_process(power_metrics, "power")

        # Merge datapoint
        bmc_datapoints.extend(thermal_datapoints)
        bmc_datapoints.extend(power_datapoints)
        # bmc_datapoints.extend(bmc_health_metrics)
        # bmc_datapoints.extend(sys_health_metrics)

        return bmc_datapoints

    except Exception as err:
        logging.error(f"Fetch BMC metrics error : {err}")


def parallel_process(node_metrics: list, category: str) -> list:
    """
    Parallel process metrics,
    node_metrics refer to a list of {'node': node_id, 'metrics': metric}
    """
    flat_datapoints = []
    try:
        process_args = zip(node_metrics, repeat(category))
        with multiprocessing.Pool() as pool:
            datapoints = pool.starmap(process, process_args)
        flat_datapoints = [item for sublist in datapoints for item in sublist]
    except Exception as err:
        logging.error(f"fetch_bmc : parallel_process error : {err}")
    return flat_datapoints


def process(node_metrics: dict, category: str) -> list:
    """
    Process metrics accroding to its category,
    node_metrics refer to {'node': node_id, 'metrics': metric}
    """
    datapoints = []
    try:
        if category == "thermal":
            process = ProcessThermal(node_metrics)
        elif category == "power":
            process = ProcessPower(node_metrics)
        elif category == "bmc_health":
            process = ProcessHealth(node_metrics, "BMC")
        elif category == "sys_health":
            process = ProcessHealth(node_metrics, "System")
        else:
            return datapoints

        datapoints = process.get_datapoints()
    except Exception as err:
        logging.error(f"fetch_bmc : parallel_process : process error : {err}")
    return datapoints
