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
This file is part of MonSter.

Author:
    Jie Li, jie.li@ttu.edu
"""

import sql
import logger

import multiprocessing
import aiohttp
import asyncio
import hostlist
from aiohttp import BasicAuth
from itertools import repeat

log = logger.get_logger(__name__)


async def base_fetch(url: str, session: aiohttp.ClientSession, max_retries=3, retry_delay=2):
  timeout = aiohttp.ClientTimeout(total=45)
  
  for attempt in range(max_retries):
    try:
      async with session.get(url, timeout=timeout, verify_ssl=False) as response:
        response.raise_for_status()
        return await response.json()
    except Exception as err:
      if attempt + 1 == max_retries:
        log.error(f"Cannot fetch data from {url} : {err}")
        return {}
      await asyncio.sleep(retry_delay)
  

async def fetch_all(urls: list, username: str, password:str):
  auth = BasicAuth(username, password)
  async with aiohttp.ClientSession( auth=auth ) as session:
    tasks = []
    for url in urls:
      task = asyncio.create_task(base_fetch(url, session))
      tasks.append(task)
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    return responses
  

def run_fetch_all(urls: list, username: str, password:str):  
  return asyncio.run(fetch_all(urls, username, password))
  

def extract_metadata(system_info: dict, bmc_info: dict, node: str):  
  bmc_ip_addr    = node
  system_metrics = system_info
  bmc_metrics    = bmc_info
  
  general   = ["UUID", "SerialNumber", "HostName", "Model", "Manufacturer"]
  processor = ["ProcessorModel", "ProcessorCount", "LogicalProcessorCount"]
  memory    = ["TotalSystemMemoryGiB"]
  bmc       = ["BmcModel", "BmcFirmwareVersion"]
  metrics   = {}
  
  try:
    # Update service tag
    if system_metrics:
      service_tag = system_metrics.get("SKU", None)
    else:
      service_tag = None

    metrics.update({
      "ServiceTag": service_tag
    })

    # Update System metrics
    if system_metrics:
      for metric in general:
        metrics.update({
          metric: system_metrics.get(metric, None)
        })
      for metric in processor:
        if metric.startswith("Processor"):
          metrics.update({
            metric: system_metrics.get("ProcessorSummary", {}).get(metric[9:], None)
          })
        else:
          metrics.update({
            metric: system_metrics.get("ProcessorSummary", {}).get(metric, None)
          })
      for metric in memory:
        metrics.update({
          metric: system_metrics.get("MemorySummary", {}).get("TotalSystemMemoryGiB", None)
        })
    else:
      for metric in general + processor + memory:
        metrics.update({
          metric: None
        })

    metrics.update({
      "Bmc_Ip_Addr": bmc_ip_addr
    })

    # Update BMC metrics
    if bmc_metrics:
      for metric in bmc:
        metrics.update({
          metric: bmc_metrics.get(metric[3:], None)
        })
    else:
      for metric in bmc:
        metrics.update({
          metric: None
        })
    
    # Update Status
    if (not system_metrics and not bmc_metrics):
      metrics.update({
        "Status": "BMC unreachable"
      })
    else:
      metrics.update({
        "Status": system_metrics.get("Status", {}).get("Health", None)
      })
    
    # Sanity check for hostname
    hostname = metrics.get("HostName", None)
    if not hostname or (not hostname.startswith("cpu")):
      new_hostname = bmc_ip_addr.replace("10.101.", "cpu-").replace(".", "-")
      print(f"Hostname {hostname} is not valid, set to {new_hostname}")
      metrics.update({
        "HostName": new_hostname
      })
      
    return metrics
  except Exception as err:
    log.error(f"Cannot extract info from system and bmc: {err}")


def parallel_extract_metadata(system_info_list: list, 
                     bmc_info_list: list,
                     nodelist: list):
  info = []
  process_args = zip(system_info_list, 
                      bmc_info_list,
                      nodelist)
  with multiprocessing.Pool() as pool:
    info = pool.starmap(extract_metadata, process_args)

  return info


def extract_fqdd_source(redfish_report:list, metrics:list):
  fqdd = []
  source = []
  for i in redfish_report:
    for metric in metrics:
      for item in i.get(metric, []):
        f_value = item.get("Name", "None").replace(" ", "_" )
        s_value = item.get("@odata.type", "None")
        if {"fqdd": f_value} not in fqdd:  
          fqdd.append({
            "fqdd": f_value
          })
        if {"source": s_value} not in source:
          source.append({
            "source": s_value
          })
  return (fqdd, source)


def process_all_idracs_13g(idrac_api: list, timestamp, idrac_metrics: list, 
                           nodelist: list, redfish_report: list,
                           nodeid_map: dict, source_map: dict, fqdd_map: dict):
  processed_records = {}
  # Breakdown the redfish report by API
  idrac_reports = []
  for i in range(len(idrac_api)):
    idrac_reports.append(redfish_report[i * len(nodelist) : (i+1) * len(nodelist)])
    
  for idrac_metric in idrac_metrics:
    table_name = f"idrac.{idrac_metric.lower()}"
    for reports in idrac_reports:
      records = parallel_process_idrac_13g(timestamp, idrac_metric, nodelist, 
                                           reports, nodeid_map, source_map, fqdd_map)
      if table_name not in processed_records:
        processed_records[table_name] = records
      else:
        processed_records[table_name].extend(records)
  
  return processed_records


def parallel_process_idrac_13g(timestamp, idrac_metric: str, nodelist: list, 
                               reports: list, nodeid_map: dict, source_map: dict, 
                               fqdd_map: dict):
  records = []
  process_args = zip(repeat(timestamp), repeat(idrac_metric), nodelist, reports,
                     repeat(nodeid_map), repeat(source_map), repeat(fqdd_map))
  with multiprocessing.Pool() as pool:
    records = pool.starmap(process_node_idrac_13g, process_args)
  
  # Remove empty lists
  records = [item for sublist in records for item in sublist]
  return records


def process_node_idrac_13g(timestamp, idrac_metric: str, node: str, report: list,
                           nodeid_map: dict, source_map: dict, fqdd_map: dict):
  records = []
  # The first item corresponds to the fqdd field
  # The second item corresponds to the source field
  # The third item corresponds to the value field
  field_map = {'Fans'        : ['FanName', '@odata.type', 'Reading'], 
               'Temperatures': ['Name',    '@odata.type', 'ReadingCelsius'], 
               'PowerControl': ['Name',    '@odata.type', 'PowerConsumedWatts']}
  fqdd_field   = field_map.get(idrac_metric)[0]
  source_field = field_map.get(idrac_metric)[1]
  value_field  = field_map.get(idrac_metric)[2]
  
  if report:
    try:
      for item in report.get(idrac_metric, []):
        fqdd   = item.get(fqdd_field, "None").replace(" ", "_" )
        source = item.get(source_field, "None")
        value  = int(item.get(value_field, 0))
        records.append((timestamp, nodeid_map[node], source_map[source], fqdd_map[fqdd], value))
    except Exception as err:
      log.error(f"Cannot process idrac metrics: {err}")
  
  return records
  
  
def process_job_metrics_slurm(jobs_metrics: list):
  jobs_info = []
  attributes = sql.job_info_column_names
  for job in jobs_metrics:
    if job['nodes']:
      hostnames = hostlist.expand_hostlist(job['nodes'])
    else:
      hostnames = []
    info = []
    for attribute in attributes:
      if attribute == 'nodes':
        info.append(hostnames)
      else:
        info.append(job.get(attribute, None))
    jobs_info.append(tuple(info))
    
  return jobs_info


def process_node_metrics_slurm(nodes_metrics: list, 
                               hostname_id_map: dict,
                               timestamp):
    nodes_info = {}
    for node in nodes_metrics:
      node_id = hostname_id_map[node['hostname']]

      state = node['state']
      if state == 'down':            
        cpu_load       = 0
        memory_used    = 0
        f_memory_usage = 0.0
      else:
        cpu_load = int(node['cpu_load'])
        # Some down nodes report cpu_load large than 2147483647, which is 
        # not INT4 and cannot saved in TSDB, we set it to -1
        if cpu_load > 2147483647: 
            cpu_load = -1
            
        # Memory usage
        free_memory = node['free_memory']
        real_memory = node['real_memory']
        memory_usage = ((real_memory - free_memory)/real_memory) * 100
        memory_used = real_memory - free_memory
        f_memory_usage = float("{:.2f}".format(memory_usage))
            
      # Check if reason is already in the previous record.
      node_data = {
          'cpu_load'   : cpu_load,
          'memoryusage': f_memory_usage,
          'memory_used': memory_used,
          'state'      : state,
      }
      nodes_info.update({
          node_id: node_data
      })
      
    # Convert nodes_info to lists of tuples, each tuple is a record to be inserted
    # Each list corresponds to the key in node_data
    cpu_load_record    = []
    memoryusage_record = []
    memory_used_record = []
    state_record       = []
    for node_id, node_data in nodes_info.items():
      cpu_load_record.append((timestamp, int(node_id), node_data['cpu_load']))
      memoryusage_record.append((timestamp, int(node_id), node_data['memoryusage']))
      memory_used_record.append((timestamp, int(node_id), node_data['memory_used']))
      state_record.append((timestamp, int(node_id), node_data['state']))
    
    records = {
      'cpu_load'   : cpu_load_record,
      'memoryusage': memoryusage_record,
      'memory_used': memory_used_record,
      'state'      : state_record,
    }
    
    return records
  

def process_node_job_correlation(jobs_metrics: list, 
                                 hostname_id_map: dict,
                                 timestamp):
  nodes_jobs = {}
  for job in jobs_metrics:
    if job['job_state'] == "RUNNING":
      job_id = job['job_id']
      cpus   = round(job['allocated_cores']/job['allocated_hosts'])
      allocated_nodes = job['job_resources']['allocated_nodes']
      for item in allocated_nodes:
        nodeid = hostname_id_map[item['nodename']]
        # cpus   = item['cpus_used'] # There is a bug in the Slurm API, cpus_used is sometimes 0
        if nodeid not in nodes_jobs:
          nodes_jobs.update({
            nodeid: {
              'jobs': [job_id],
              'cpus': [cpus]
            }
          })
        else:
          nodes_jobs[nodeid]['jobs'].append(job_id)
          nodes_jobs[nodeid]['cpus'].append(cpus)
  
  # Convert nodes_jobs to lists of tuples, each tuple is a record to be inserted
  records = [(timestamp, int(nodeid), nodes_jobs[nodeid]['jobs'], nodes_jobs[nodeid]['cpus']) for nodeid in nodes_jobs]

  return records
  