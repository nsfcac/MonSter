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

import psycopg2
from datetime import datetime
from pgcopy import CopyManager

def monit_idrac():
  cols = ('timestamp', 'nodeid', 'source', 'fqdd', 'value')
  connection = utils.init_tsdb_connection()
  username, password = utils.get_idrac_auth()
  nodelist = utils.get_nodelist()
  idrac_api = utils.get_idrac_api()
  idrac_model = utils.get_idrac_model()
  idrac_metrics = utils.get_idrac_metrics()
  
  with psycopg2.connect(connection) as conn:
    nodeid_map = utils.get_nodeid_map(conn)
    fqdd_map   = utils.get_fqdd_source_map(conn, 'fqdd')
    source_map = utils.get_fqdd_source_map(conn, 'source')
    
    timestamp = datetime.utcnow()
    if idrac_model == "13G":
      processed_records = idrac.get_idrac_metrics_13g(idrac_api, timestamp, idrac_metrics,
                                                      nodelist, username, password,
                                                      nodeid_map, source_map, fqdd_map)
      for tabel, records in processed_records.items():
        mgr = CopyManager(conn, tabel, cols)
        mgr.copy(records)
    
    conn.commit()

if __name__ == '__main__':
  monit_idrac()