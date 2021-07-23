from utils.partition import partition
from utils.AsyncioRequests import AsyncioRequests
import multiprocessing
import logging


def parallel_fetch(idrac_config: dict, urls: list, nodes: list, cores: int) -> list:
    """
    Spread fetching across cores
    """
    flatten_metrics = []
    try:
        # Partition
        urls_group = partition(urls, cores)
        nodes_group = partition(nodes, cores)

        fetch_args = []
        for i in range(cores):
            urls = urls_group[i]
            nodes = nodes_group[i]
            fetch_args.append((idrac_config, urls, nodes))

        with multiprocessing.Pool() as pool:
            metrics = pool.starmap(fetch, fetch_args)

        flatten_metrics = [item for sublist in metrics for item in sublist]
    except Exception as err:
        logging.error(f"fetch_bmc : parallel_fetch error : {err}")

    return flatten_metrics


def fetch(idrac_config: dict, urls: list, nodes: list) -> list:
    """
    Use AsyncioRequests to query urls
    """
    idrac_metrics = []
    try:
        bmc = AsyncioRequests(auth=(idrac_config['user'],
                                    idrac_config['password']),
                              timeout=(idrac_config['timeout']['connect'],
                                       idrac_config['timeout']['read']),
                              max_retries=idrac_config['max_retries'])
        idrac_metrics = bmc.bulk_fetch(urls, nodes)
    except Exception as err:
        logging.error(f"fetch_bmc : parallel_fetch : fetch error : {err}")
    return idrac_metrics
