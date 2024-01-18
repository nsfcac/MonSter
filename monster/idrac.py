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

log = logger.get_logger(__name__)


def get_nodes_metadata(nodelist: list, username: str, password: str):
  bmc_base_url = "/redfish/v1/Managers/iDRAC.Embedded.1"
  system_base_url = "/redfish/v1/Systems/System.Embedded.1"

  system_urls = ["https://" + node + system_base_url for node in nodelist]
  bmc_urls = ["https://" + node + bmc_base_url for node in nodelist]

  # Fetch system info
  system_info = process.run_fetch_all(system_urls, 
                                      username, 
                                      password)
  # Fetch bmc info
  bmc_info = process.run_fetch_all(bmc_urls, 
                                    username,
                                    password)

  # Process system and bmc info
  return process.parallel_extract(system_info, bmc_info, nodelist)
  

def get_fqdd_source_13g(nodelist: list, api: list, metrics: list, 
                        username: str, password: str):
  for node in nodelist:
    urls = [f"https://{node}{url}" for url in api]
    redfish_report = process.run_fetch_all(urls, username, password)
    if redfish_report:
      return process.extract_fqdd_source(redfish_report, metrics)


def get_metric_definitions_13g(idrac_metrics: list):
  metric_definitions = []
  unit_map = {'Fans': 'RPM', 'Temperatures': 'Cel', 'PowerControl': 'Watts'}
  for metric in idrac_metrics:
    metric_definitions.append({'Id': metric, 
                               'MetricDataType': 'Integer',
                               'Units': unit_map.get(metric, None)})
  return metric_definitions
