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

import utils
import slurm
import process

import time
import schedule
import psycopg2
import urllib3
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def monit_slurm():
  timestamp = datetime.utcnow().replace(microsecond=0)
  
  connection      = utils.init_tsdb_connection()
  nodelist        = utils.get_nodelist()
  ip_hostname_map = utils.get_ip_hostname_map(connection)
  hostname_id_map = utils.get_hostname_id_map(connection)
  hostname_list   = [ip_hostname_map[ip] for ip in nodelist]
  partition       = utils.get_partition()
  slurm_config    = utils.get_slurm_config()
  
  jobs_metrics  = slurm.get_slurm_jobs_metrics(slurm_config, partition)
  nodes_metrics = slurm.get_slurm_nodes_metrics(slurm_config, hostname_list)
  
  # Extract job information
  jobs_info = process.process_job_metrics_slurm(jobs_metrics)
    
  # Extract node information
  nodes_info = process.process_node_metrics_slurm(nodes_metrics, 
                                                  hostname_id_map,
                                                  timestamp)
  # Extract node-job correlation
  nodes_jobs = process.process_node_job_correlation(jobs_metrics,
                                                    hostname_id_map,
                                                    timestamp)
  
  with psycopg2.connect(connection) as conn:
    slurm.dump_slurm_jobs_info(conn, jobs_info)
    slurm.dump_slurm_nodes_info(conn, nodes_info)
    slurm.dump_slurm_nodes_jobs(conn, nodes_jobs)
    

if __name__ == "__main__":
  schedule.every().minutes.at(":00").do(monit_slurm)
  while True:
    schedule.run_pending()
    time.sleep(1)