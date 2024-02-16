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
import gzip

import pandas as pd
import sqlalchemy as db

import mb_sql
import mb_utils

cur_dir = os.path.dirname(__file__)
monster_dir = os.path.join(cur_dir, '../monster')
sys.path.append(monster_dir)

import utils

def get_metrics_map():
  metrics_mapping = utils.parse_config()['openapi']
  return metrics_mapping


def get_jobs_cpus(row, item):
  jobs = []
  cpus = []
  jobs_cpus_map = {}
  
  if row['jobs_copy']:
    for job_list, cpu_list in zip(row['jobs_copy'], row['cpus']):
      for i, job in enumerate(job_list):
        if job not in jobs_cpus_map:
          jobs_cpus_map[job] = cpu_list[i]

    # Order the dictionary by key
    jobs_cpus_map = dict(sorted(jobs_cpus_map.items()))
    
    jobs = list(jobs_cpus_map.keys())
    cpus = list(jobs_cpus_map.values())
  
  if item == 'jobs':
    return jobs
  elif item == 'cpus':
    return cpus
  else:
    return None
  

def query_db(connection: str, sql: str, nodelist: list):
  engine    = db.create_engine(connection)
  dataframe = pd.read_sql_query(sql, engine)
    
  # If the dataframe has a node colume, remove rows that are not in the nodelist
  if 'node' in dataframe.columns:
    dataframe = dataframe[dataframe['node'].isin(nodelist)]
    
  # If the dataframe has a time colume, convert it to epoch time
  if 'time' in dataframe.columns:
    dataframe['time'] = pd.to_datetime(dataframe['time'])
    dataframe['time'] = dataframe['time'].astype(int) // 10**9
    
  if 'jobs' in dataframe.columns:
    # Copy the jobs column to a new column, this is for the get_jobs_cpus function.
    # The get_jobs_cpus function will modify the jobs column, so we need to keep a 
    # copy of the original jobs column to avoid the error when applying the 
    # get_jobs_cpus function on the cpus column.
    dataframe['jobs_copy'] = dataframe['jobs']
    dataframe['jobs'] = dataframe.apply(lambda x: get_jobs_cpus(x, 'jobs'), axis=1)
    dataframe['cpus'] = dataframe.apply(lambda x: get_jobs_cpus(x, 'cpus'), axis=1)
    # Drop the jobs_copy column
    dataframe = dataframe.drop(columns=['jobs_copy'])
  
  # Convert the dataframe to a dictionary
  dataframe = dataframe.to_dict(orient='records')
  return dataframe


def query_db_wrapper(connection: str, start: str, end: str, interval: str,
                     aggregation: str, nodelist: list, table: str):
  metric = []
  if table =='slurm.jobs':
    sql = mb_sql.generate_slurm_jobs_sql(start, end)
    metric = mb_utils.query_db(connection, sql, nodelist)
  elif table == 'slurm.node_jobs':
    sql = mb_sql.generate_slurm_node_jobs_sql(start, end, interval)
    metric = mb_utils.query_db(connection, sql, nodelist)
  elif table == 'slurm.state':
    sql = mb_sql.generate_slurm_state_sql(start, end, interval)
    metric = mb_utils.query_db(connection, sql, nodelist)
  elif 'slurm' in table:
    slurm_metric = table.split('.')[1]
    sql = mb_sql.generate_slurm_metric_sql(slurm_metric, start, end, 
                                           interval, aggregation)
    metric = mb_utils.query_db(connection, sql, nodelist)
  elif 'idrac' in table:
    idrac_metric = table.split('.')[1]
    sql = mb_sql.generate_idrac_metric_sql(idrac_metric, start, end, 
                                           interval, aggregation)
    metric = mb_utils.query_db(connection, sql, nodelist)
  else:
    pass
  return metric


def compress_json(data):
  # Serialize the data structure to a JSON string
  json_string = json.dumps(data)
  
  # Compress the JSON string using gzip compression
  compressed_data = gzip.compress(json_string.encode('utf-8'))
  
  return compressed_data
  