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

import pandas as pd
import sqlalchemy as db

import mbuilder.mb_sql as mb_sql
import mbuilder.mb_utils as mb_utils

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
  record = {} 
  engine    = db.create_engine(connection)
  dataframe = pd.read_sql_query(sql, engine)
  
  # If the dataframe is not empty
  if not dataframe.empty:
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

    # Fill all NaN with 0
    with pd.option_context("future.no_silent_downcasting", True):
      dataframe = dataframe.fillna(0).infer_objects(copy=False)
    
    # Convert the dataframe to a dictionary
    record = dataframe.to_dict(orient='records')
  return record


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


def reformat_results(results):
  reformated_results = {}
  summary            = {}
  job_nodes_cpus     = {}
  node_time_records  = {}
  job_time_records   = {}
  node_system_power_track = {}
  node_memory_used_track  = {}
  all_system_power_track  = {}
  all_memory_used_track   = {}
  
  slurm_jobs = results.get('slurm.jobs', {})
  if slurm_jobs:
    # Get the nodes, CPUs, memory_per_cpu
    for item in slurm_jobs:
      job_nodes_cpus.update({item['job_id']: {'nodes': item['nodes'], 
                                              'used_cores': int(item['cpus']), 
                                              'memory_per_core': item['memory_per_cpu'],
                                              'cores_per_node': round(item['cpus']/item['node_count'])}})
      
  system_power = results.get('idrac.powercontrol', {})
  if system_power:
    for item in system_power:
      node_time_records[f"{item['node']}_{item['time']}"] = {'time': int(item['time']),
                                                             'node': item['node'],
                                                             'system_power': item['value'],
                                                             'system_power_diff': 0, # 'system_power_diff' is the difference between the current and the previous 'system_power'
                                                             'memory_used': 0,
                                                             'memory_used_diff': 0, # 'memory_used_diff' is the difference between the current and the previous 'memory_used
                                                             'used_cores': 0,
                                                             'jobs': [],
                                                             'cores': [],}
      if item['node'] not in node_system_power_track:
        node_system_power_track[item['node']] = {'power': [item['value']],
                                                 'time': [int(item['time'])]}
      else:
        node_system_power_track[item['node']]['power'].append(item['value'])
        node_system_power_track[item['node']]['time'].append(int(item['time']))
      
      if item['time'] not in all_system_power_track:
        all_system_power_track[item['time']] = [item['value']]
      else:
        all_system_power_track[item['time']].append(item['value'])

  for node, records in node_system_power_track.items():
    power = records['power']
    diff = [0]
    diff.extend([power[i] - power[i-1] for i in range(1, len(power))])
    records['diff'] = diff
    for i, time in enumerate(records['time']):
      node_time_records[f'{node}_{time}']['system_power_diff'] = diff[i]
      
  memory_used = results.get('slurm.memory_used', {})
  if memory_used:
    for item in memory_used:
      if f"{item['node']}_{item['time']}" in node_time_records:
        node_time_records[f"{item['node']}_{item['time']}"].update({'memory_used': item['value']})
      else:
        node_time_records[f"{item['node']}_{item['time']}"] = {'time': int(item['time']),
                                                               'node': item['node'],
                                                               'system_power': 0,
                                                               'system_power_diff': 0,
                                                               'memory_used': item['value'],
                                                               'memory_used_diff': 0,
                                                               'used_cores': 0,
                                                               'jobs': [],
                                                               'cores': [],}  
      if item['node'] not in node_memory_used_track:
        node_memory_used_track[item['node']] = {'memory': [item['value']],
                                                'time': [int(item['time'])]}
      else:
        node_memory_used_track[item['node']]['memory'].append(item['value'])
        node_memory_used_track[item['node']]['time'].append(int(item['time']))

      if item['time'] not in all_memory_used_track:
        all_memory_used_track[item['time']] = [item['value']]
      else:
        all_memory_used_track[item['time']].append(item['value'])

  for node, records in node_memory_used_track.items():
    memory = records['memory']
    diff = [0]
    diff.extend([memory[i] - memory[i-1] for i in range(1, len(memory))])
    records['diff'] = diff
    for i, time in enumerate(records['time']):
      node_time_records[f'{node}_{time}']['memory_used_diff'] = diff[i]
  
  node_jobs = results.get('slurm.node_jobs', {})
  if node_jobs:
    # Update the cpus field as it may not be correct
    for item in node_jobs:
      cores = []
      for job in item['jobs']:
        if job in job_nodes_cpus:
          cores.append(job_nodes_cpus[job]['cores_per_node'])
        else:
          # job_nodes_cpus could cannot find the job, append 0 to cores
          cores.append(0)
          
      if f"{item['node']}_{item['time']}" in node_time_records:
        node_time_records[f"{item['node']}_{item['time']}"].update({'jobs': item['jobs'], 
                                                                    'cores': cores, 
                                                                    'used_cores': sum(cores)})
      else:
        node_time_records[f"{item['node']}_{item['time']}"] = {'time': int(item['time']),
                                                               'node': item['node'],
                                                               'system_power': 0,
                                                               'memory_used': 0,
                                                               'used_cores': sum(cores),
                                                               'jobs': item['jobs'],
                                                               'cores': cores,}
  
  # Calculate the summary
  for time, records in all_system_power_track.items():
    summary[time] = {'time': time,
                     'average_system_power': round(sum(records) / len(records), 2),
                     'total_system_power': round(sum(records), 2),
                     'average_memory_used': 0,
                     'total_memory_used': 0}
  
  for time, records in all_memory_used_track.items():
    if time in summary:
      summary[time].update({'average_memory_used': round(sum(records) / len(records), 2),
                            'total_memory_used': round(sum(records), 2)})
    else:
      summary[time] = {'time': time,
                       'average_system_power': 0,
                       'total_system_power': 0,
                       'average_memory_used': round(sum(records) / len(records), 2),
                       'total_memory_used': round(sum(records), 2)}
    

  # Calculate the power consumption for each job
  for key, value in node_time_records.items():
    this_node = key.split('_')[0]
    timestamp = key.split('_')[1]
    this_node_power = value['system_power']
    this_node_cores = value['used_cores']
    for i, job in enumerate(value['jobs']):
      if this_node_cores != 0:
        this_node_power_part = round(this_node_power * (value['cores'][i] / this_node_cores), 2)
      else:
        this_node_power_part = 0
      if f'{job}_{timestamp}' not in job_time_records:
        if this_node_cores != 0:
          power_per_core = round(this_node_power_part / this_node_cores, 2)
        else:
          power_per_core = 0
        if job not in job_nodes_cpus:
          memory_per_core = 0
          memory_used = 0
        else:
          memory_per_core = job_nodes_cpus[job].get('memory_per_core', 0)
          memory_used = job_nodes_cpus[job].get('memory_per_core', 0) * job_nodes_cpus[job].get('used_cores', 0)
        job_time_records[f'{job}_{timestamp}'] = {
          'time': int(timestamp),
          'job_id': job,
          'data': [{
            'node': this_node,
            'power': this_node_power_part,
            'cores': value['cores'][i],
          }],
          'power': this_node_power_part,
          'cores': value['cores'][i],
          'power_per_core': power_per_core,
          'memory_per_core': memory_per_core,
          'memory_used': memory_used
        }
      else:
        job_time_records[f'{job}_{timestamp}']['data'].append({
          'node': this_node,
          'power': this_node_power_part,
          'cores': value['cores'][i],
        })
        job_time_records[f'{job}_{timestamp}']['power'] += this_node_power_part
        job_time_records[f'{job}_{timestamp}']['cores'] += value['cores'][i]
        if job_time_records[f'{job}_{timestamp}']['cores'] != 0:
          job_time_records[f'{job}_{timestamp}']['power_per_core'] = round(job_time_records[f'{job}_{timestamp}']['power'] / job_time_records[f'{job}_{timestamp}']['cores'], 2)
        else:
          job_time_records[f'{job}_{timestamp}']['power_per_core'] = 0
    
  slurm_jobs = results.get('slurm.jobs', {})
  if slurm_jobs:
    reformated_results['job_details'] = slurm_jobs

  reformated_results['nodes']   = list(node_time_records.values())
  reformated_results['jobs']    = list(job_time_records.values())
  reformated_results['summary'] = list(summary.values())

  return reformated_results

  