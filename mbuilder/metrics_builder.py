"""
MIT License

Copyright (c) 2024 Texas Tech University

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

"""
This file is part of MetricBuilder.

Author:
    Jie Li, jie.li@ttu.edu
"""
import os
import sys
import json
import hostlist
import multiprocessing

from itertools import repeat

cur_dir = os.path.dirname(__file__)
monster_dir = os.path.join(cur_dir, '../monster')
mbuilder_dir = os.path.join(cur_dir, '../')
sys.path.append(monster_dir)
sys.path.append(mbuilder_dir)


import utils
import mbuilder.mb_utils as mb_utils

def metrics_builder(start=None, 
                    end=None, 
                    interval=None, 
                    aggregation=None, 
                    nodelist=None, 
                    metrics=None):
  results = {}
  tables = []
  
  connection          = utils.init_tsdb_connection()  
  ip_hostname_mapping = utils.get_ip_hostname_map(connection)
  metrics_mapping     = mb_utils.get_metrics_map()
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
  start = '2024-04-19 20:30:00-05'
  end = '2024-04-19 21:30:00-05'
  interval = '5m'
  aggregation = 'max'
  nodelist = "10.101.1.[1-60],10.101.2.[1-60],10.101.3.[1-56],10.101.4.[1-48],10.101.5.[1-24],10.101.6.[1-20],10.101.7.[1-3,5-60],10.101.8.[1-60],10.101.9.[1-60],10.101.10.[25-44]"
  # nodelist = "10.101.1.[1-60]"
  metrics = ['SystemPower_iDRAC', 'NodeJobsCorrelation_Slurm', 'JobsInfo_Slurm', 'MemoryUsed_Slurm']
  # metrics = ['JobsInfo_Slurm']
  results = metrics_builder(start, end, interval, aggregation, nodelist, metrics)
  
  # Write the results to a file
  with open(f"../json/results-{start.split(' ')[0]}-{end.split(' ')[0]}.json", "w") as f:
    f.write(json.dumps(results, indent=2))