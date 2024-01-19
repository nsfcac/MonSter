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

import logger

import multiprocessing
import aiohttp
import asyncio
from aiohttp import BasicAuth
from itertools import repeat

log = logger.get_logger(__name__)


def partition(arr:list, cores: int):
  groups = []
  arr_len = len(arr)
  arr_per_core = arr_len // cores
  arr_surplus = arr_len % cores

  increment = 1
  for i in range(cores):
    if(arr_surplus != 0 and i == (cores-1)):
      groups.append(arr[i * arr_per_core:])
    else:
      groups.append(arr[i * arr_per_core : increment * arr_per_core])
      increment += 1
  return groups


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
  bmc_ip_addr = node
  system_metrics = system_info
  bmc_metrics = bmc_info
  
  general = ["UUID", "SerialNumber", "HostName", "Model", "Manufacturer"]
  processor = ["ProcessorModel", "ProcessorCount", "LogicalProcessorCount"]
  memory = ["TotalSystemMemoryGiB"]
  bmc = ["BmcModel", "BmcFirmwareVersion"]
  metrics = {}
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


def process_all_idracs(idrac_api: list, timestamp, idrac_metrics: list, 
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
      records = parallel_process_idrac(timestamp, idrac_metric, nodelist, reports, 
                                       nodeid_map, source_map, fqdd_map)
      if table_name not in processed_records:
        processed_records[table_name] = records
      else:
        processed_records[table_name].extend(records)
  
  return processed_records


def parallel_process_idrac(timestamp, idrac_metric: str, 
                           nodelist: list, reports: list, 
                           nodeid_map: dict, source_map: dict, fqdd_map: dict):
  records = []
  process_args = zip(repeat(timestamp), repeat(idrac_metric), nodelist, reports,
                     repeat(nodeid_map), repeat(source_map), repeat(fqdd_map))
  with multiprocessing.Pool() as pool:
    records = pool.starmap(process_node_idrac, process_args)
  
  # Remove empty lists
  records = [item for sublist in records for item in sublist]
  return records


def process_node_idrac(timestamp, idrac_metric: str, node: str, report: list,
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
  
  
  

  