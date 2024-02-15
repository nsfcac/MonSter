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
import logger
import idrac
import schema
import sql

import psycopg2

log = logger.get_logger(__name__)


def init_tsdb():
  """init_tsdb Initialize TimeScaleDB

  Initialize TimeScaleDB
  """
  connection = utils.init_tsdb_connection()
  username, password = utils.get_idrac_auth()
  nodelist = utils.get_nodelist()
  idrac_api = utils.get_idrac_api()
  idrac_model = utils.get_idrac_model()
  idrac_metrics = utils.get_idrac_metrics()
    
  utils.print_status('Getting', 'nodes' , 'metadata')
  nodes_metadata = idrac.get_nodes_metadata(nodelist, username, password)
  
  utils.print_status('Getting', 'fqdd and source' , 'information')
  if idrac_model == '15G':
    pass
  elif idrac_model == '13G':
    fqdd_source_metadata = idrac.get_fqdd_source_13g(nodelist, idrac_api, idrac_metrics, 
                                                     username, password)
  
  utils.print_status('Getting', 'metric' , 'definitions')
  if idrac_model == '15G':
    pass
  elif idrac_model == '13G':
    metric_definitions = idrac.get_metric_definitions_13g(idrac_metrics)

  idrac_table_schemas = schema.build_idrac_table_schemas(metric_definitions)
  slurm_table_schemas = schema.build_slurm_table_schemas()
    
  with psycopg2.connect(connection) as conn:
    cur = conn.cursor()

    # Create node metadata table
    utils.print_status('Creating', 'TimeScaleDB' , 'tables')
    metadata_sql = sql.generate_metadata_table_sql(nodes_metadata, 'nodes')
    cur.execute(metadata_sql)
    sql.write_nodes_metadata(conn, nodes_metadata)
    
    # Create source and fqdd tables
    source_table_sql = sql.generate_source_table_sql()
    cur.execute(source_table_sql)
    
    fqdd_table_sql = sql.generate_fqdd_table_sql()
    cur.execute(fqdd_table_sql)
    
    for i, table in enumerate(['fqdd', 'source']):
      sql.write_fqdd_source_metadata(conn, fqdd_source_metadata[i], table)
        
    # Create schema for idrac
    idrac_sqls = sql.generate_metric_table_sqls(idrac_table_schemas, 'idrac')
    cur.execute(idrac_sqls['schema_sql'])

    # Create schema for slurm
    slurm_sqls = sql.generate_metric_table_sqls(slurm_table_schemas, 'slurm')
    cur.execute(slurm_sqls['schema_sql'])

    # Create idrac and slurm tables
    all_sqls = idrac_sqls['tables_sql'] + slurm_sqls['tables_sql']
    for s in all_sqls:
      table_name = s.split(' ')[5]
      cur.execute(s)

      # Create hypertable
      create_hypertable_sql = "SELECT create_hypertable(" + "'" \
          + table_name + "', 'timestamp', if_not_exists => TRUE);"
      cur.execute(create_hypertable_sql)
    
    # Create table for jobs info
    slurm_job_sql = sql.generate_slurm_job_table_sql('slurm')
    cur.execute(slurm_job_sql['schema_sql'])
    for s in slurm_job_sql['tables_sql']:
      table_name = s.split(' ')[5]
      cur.execute(s)
    
    # Create table for metric definitions
    if idrac_model == '15G':
      metric_def_sql = sql.generate_metric_def_table_sql()
      cur.execute(metric_def_sql)
      sql.write_metric_definitions(conn, metric_definitions)
    
    conn.commit()
    cur.close()
  utils.print_status('Finish', 'tables' , 'initialization')

if __name__ == '__main__':
  init_tsdb()