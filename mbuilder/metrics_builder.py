import json
import multiprocessing
from itertools import repeat

import hostlist

from mbuilder import mb_utils
from monster import utils


def metrics_builder(config,
                    start=None,
                    end=None,
                    interval=None,
                    aggregation=None,
                    nodelist=None,
                    metrics=None):
    results = {}
    tables = []

    connection          = utils.init_tsdb_connection(config)
    ip_hostname_mapping = utils.get_ip_hostname_map(connection)
    metrics_mapping     = mb_utils.get_metrics_map(config)
    nodelist            = hostlist.expand_hostlist(nodelist)

    # Convert IPs to hostnames of the nodes
    nodelist = [ip_hostname_mapping[ip] for ip in nodelist]

    # Parse the metrics
    if metrics:
        for metric in metrics:
            metric_name = metric.split('_')[0]
            source = metric.split('_')[1].lower()
            if metric_name in metrics_mapping[source].keys():
                table = metrics_mapping[source][metric_name]
                tables.append(f'{source}.{table}')

    # Parallelize the queries
    with multiprocessing.Pool(len(tables)) as pool:
        query_db_args = zip(repeat(connection),
                            repeat(start),
                            repeat(end),
                            repeat(interval),
                            repeat(aggregation),
                            repeat(nodelist),
                            tables)
        records = pool.starmap(mb_utils.query_db_wrapper, query_db_args)

    # Combine the results
    for table, record in zip(tables, records):
        results[table] = record

    # Refomat the results required by the frontend
    results = mb_utils.reformat_results(results)

    return results


if __name__ == "__main__":
    config = utils.parse_config()
    # For testing purposes
    start = '2024-09-30 20:30:00-05'
    end = '2024-09-30 21:30:00-05'
    interval = '5m'
    aggregation = 'max'
    nodelist = "10.101.1.[1-10]"
    # nodelist = "10.101.1.[1-60]"
    metrics = ['SystemPower_iDRAC', 'Fans_iDRAC', 'Temperatures_iDRAC', 'NodeJobsCorrelation_Slurm', 'JobsInfo_Slurm', 'MemoryUsage_Slurm', 'MemoryUsed_Slurm']
    # metrics = ['JobsInfo_Slurm']
    results = metrics_builder(config, start, end, interval, aggregation, nodelist, metrics)

    # Write the results to a file
    with open(f"../json/results-{start.split(' ')[0]}-{end.split(' ')[0]}.json", "w") as f:
        f.write(json.dumps(results, indent=2))
