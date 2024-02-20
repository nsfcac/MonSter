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
import process

import asyncio
import psycopg2

log = logger.get_logger(__name__)


def get_nodes_metadata(nodelist: list, username: str, password: str):
  system_base_url = "/redfish/v1/Systems/System.Embedded.1"
  bmc_base_url    = "/redfish/v1/Managers/iDRAC.Embedded.1"

  system_urls = ["https://" + node + system_base_url for node in nodelist]
  bmc_urls    = ["https://" + node + bmc_base_url for node in nodelist]

  # Fetch system info
  system_info = process.run_fetch_all(system_urls, 
                                      username, 
                                      password)
  # Fetch bmc info
  bmc_info = process.run_fetch_all(bmc_urls, 
                                    username,
                                    password)

  # Process system and bmc info
  return process.parallel_extract_metadata(system_info, bmc_info, nodelist)
  

def get_fqdd_source_13g(nodelist: list, api: list, metrics: list, 
                        username: str, password: str):
  for node in nodelist:
    urls = [f"https://{node}{url}" for url in api]
    redfish_report = process.run_fetch_all(urls, username, password)
    if redfish_report:
      return process.extract_fqdd_source_13g(redfish_report, metrics)


def get_fqdd_source_15g(nodelist: list, username: str, password: str):
  for node in nodelist:
    url = f"https://{node}/redfish/v1/TelemetryService"
    telemetry_service = process.single_fetch(url, username, password)
    if telemetry_service:
      return process.extract_fqdd_source_15g(telemetry_service)
    

def get_metric_definitions_13g(idrac_metrics: list):
  metric_definitions = []
  unit_map = {'Fans': 'RPM', 'Temperatures': 'Cel', 'PowerControl': 'Watts'}
  for metric in idrac_metrics:
    metric_definitions.append({'Id': metric, 
                               'MetricDataType': 'Integer',
                               'Units': unit_map.get(metric, None)})
  return metric_definitions


def get_metric_definitions_15g(nodelist: list, username: str, password: str):
  metric_definitions = []
  metric_definition_urls = get_metric_definition_urls_15g(nodelist, username, password)
  available_fields = ['Id', 'Name', 'Description', 'MetricType', \
                      'MetricDataType', 'Units', 'Accuracy', \
                      'SensingInterval', 'DiscreteValues']
  for node in nodelist:
    for url in metric_definition_urls:
      metric_definition = {}
      ext_url = f"https://{node}{url}"
      response = process.single_fetch(ext_url, username, password)
      if response:
        for field in available_fields:
          field_value = response.get(field, None)
          metric_definition[field] = field_value
          metric_definitions.append(metric_definition)
    return metric_definitions


def get_metric_definition_urls_15g(nodelist: list, username: str, password: str):
  metric_definition_urls = []
  for node in nodelist:
    url = f'https://{node}/redfish/v1/TelemetryService/MetricDefinitions'
    response = process.single_fetch(url, username, password)
    if response:
      members = response.get('Members', [])
      metric_definition_urls = [m.get('@odata.id', None) for m in members]
      return metric_definition_urls


def get_idrac_metrics_13g(api:list, timestamp, idrac_metrics: list, 
                          nodelist: list, username: str, password: str,
                          nodeid_map: dict, source_map: dict, fqdd_map: dict):
  urls = [f"https://{node}{url}" for url in api for node in nodelist]
  redfish_report = process.run_fetch_all(urls, username, password)
  if redfish_report:
    processed_records = process.process_all_idracs_13g(api, timestamp, idrac_metrics,
                                                       nodelist, redfish_report,
                                                       nodeid_map, source_map, fqdd_map)
    return processed_records
  

def get_idrac_metrics_15g(nodelist: list, username: str, password: str, 
                          connection: str, nodeid_map: dict, source_map: dict, 
                          fqdd_map: dict, metric_dtype_mapping: dict):
  with psycopg2.connect(connection) as conn:
    while True:
      asyncio.run(listen_process_write_idrac_15g(nodelist, username, password, 
                                                conn, nodeid_map, source_map, 
                                                fqdd_map, metric_dtype_mapping))
    

async def listen_process_write_idrac_15g(nodelist: list, username: str, password: str, 
                                         conn: object, nodeid_map: dict, source_map: dict, 
                                         fqdd_map: dict,metric_dtype_mapping: dict):
  buf_size = 1024 * 1024 * 10
  # Metrics read queue
  mr_queue = asyncio.Queue(maxsize=buf_size)
  # Metrics process queue
  mp_queue = asyncio.Queue(maxsize=buf_size)
  
  listen_task  = [asyncio.create_task(process.listen_idrac_15g(node, username, password, mr_queue)) for node in nodelist]
  process_task = [asyncio.create_task(process.process_idrac_15g(mr_queue, mp_queue))]
  write_task   = [asyncio.create_task(process.write_idrac_15g(conn, nodeid_map, source_map, fqdd_map, metric_dtype_mapping, mp_queue))]
  
  tasks = listen_task + process_task + write_task
  await asyncio.gather(*tasks)
  