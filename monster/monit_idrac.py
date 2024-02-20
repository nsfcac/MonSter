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
import idrac

import time
import schedule
import psycopg2
import multiprocessing
from datetime import datetime
from pgcopy import CopyManager

def monit_idrac_13g():
  cols = ('timestamp', 'nodeid', 'source', 'fqdd', 'value')
  connection         = utils.init_tsdb_connection()
  username, password = utils.get_idrac_auth()
  nodelist           = utils.get_nodelist()
  idrac_api          = utils.get_idrac_api()
  idrac_metrics      = utils.get_idrac_metrics()
  
  with psycopg2.connect(connection) as conn:
    nodeid_map = utils.get_nodeid_map(conn)
    fqdd_map   = utils.get_fqdd_source_map(conn, 'fqdd')
    source_map = utils.get_fqdd_source_map(conn, 'source')
    timestamp  = datetime.utcnow().replace(microsecond=0)
    processed_records = idrac.get_idrac_metrics_13g(idrac_api, timestamp, idrac_metrics,
                                                    nodelist, username, password,
                                                    nodeid_map, source_map, fqdd_map)
    for tabel, records in processed_records.items():
      mgr = CopyManager(conn, tabel, cols)
      mgr.copy(records)
    conn.commit()
    

def monit_idrac_15g():
  connection         = utils.init_tsdb_connection()
  username, password = utils.get_idrac_auth()
  nodelist           = utils.get_nodelist()
  
  cores = multiprocessing.cpu_count()
  if (len(nodelist) < cores):
    cores = len(nodelist)
  nodelist_chunks = utils.partition_list(nodelist, cores)
  
  with psycopg2.connect(connection) as conn:
    nodeid_map           = utils.get_nodeid_map(conn)
    fqdd_map             = utils.get_fqdd_source_map(conn, 'fqdd')
    source_map           = utils.get_fqdd_source_map(conn, 'source')
    metric_dtype_mapping = utils.get_metric_dtype_mapping(conn)
    
  with multiprocessing.Pool(cores) as pool:
    pool.starmap(idrac.get_idrac_metrics_15g, [(nodelist_chunk, username, password, 
                                                connection, nodeid_map, source_map, 
                                                fqdd_map, metric_dtype_mapping) 
                                                for nodelist_chunk in nodelist_chunks])
    

if __name__ == '__main__':
  idrac_model = utils.get_idrac_model()
  if idrac_model == "13G":
    schedule.every().minutes.at(":00").do(monit_idrac_13g)
    while True:
      schedule.run_pending()
      time.sleep(1)
  elif idrac_model == "15G":
    monit_idrac_15g()