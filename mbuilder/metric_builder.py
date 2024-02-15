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
import hostlist
import multiprocessing

from itertools import repeat

cur_dir = os.path.dirname(__file__)
monster_dir = os.path.join(cur_dir, '../monster')
sys.path.append(monster_dir)

import utils
import mb_utils 

def metric_builder(start=None, 
                   end=None, 
                   interval=None, 
                   aggregation=None, 
                   nodelist=None, 
                   metrics=None, 
                   compression=None):
  results = {}
  tables = []
  
  connection = utils.init_tsdb_connection()  
  nodelist   = hostlist.expand_hostlist(nodelist)
  ip_hostname_mapping = utils.get_ip_hostname_map(connection)
  metrics_mapping     = mb_utils.get_metrics_map()
  
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
  
  # Compress the results if needed
  if compression:
    results = mb_utils.compress_json(results)
  return results

if __name__ == "__main__":
  results = metric_builder()
  print(results)