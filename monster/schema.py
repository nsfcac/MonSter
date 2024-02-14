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
import utils

log = logger.get_logger(__name__)


def build_idrac_table_schemas(metric_definitions: list):
  table_schemas = {}
  for metric in metric_definitions:
    table_name = metric['Id']
    metric_type = metric['MetricDataType']
    metric_unit = metric.get('Units', None)

    # For network metrics, use BIG INT for storing the metric readings
    if metric_unit == 'By' or metric_unit == 'Pkt':
        value_type = 'BIGINT'
    else:
        value_type = utils.data_type_mapping.get(metric_type, 'TEXT')
    
    column_names = ['Timestamp', 'NodeID', 'Source', 'FQDD', 'Value']
    column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'INT', \
                    'INT', value_type]
    
    table_schemas.update({
        table_name: {
            'column_names': column_names,
            'column_types': column_types,
        }
    })
  return table_schemas


def build_slurm_table_schemas():
    table_schemas = {}
    add_tables = {
      'memoryusage':{
        'add_columns': ['Value'],
        'add_types': ['REAL']
      },
      'memory_used':{
        'add_columns': ['Value'],
        'add_types': ['INT']
      },
      'cpu_load':{
        'add_columns': ['Value'],
        'add_types': ['INT']
      },
      'state':{
        'add_columns': ['Value'],
        'add_types': ['TEXT']
      },
      'node_jobs':{
        'add_columns': ['Jobs', 'CPUs'],
        'add_types': ['INTEGER[]', 'INTEGER[]']
      },
    }
    for table_name, detail in add_tables.items():
      column_names = ['Timestamp', 'NodeID']
      column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL']
      column_names.extend(detail['add_columns'])
      column_types.extend(detail['add_types'])

      table_schemas.update({
        table_name: {
          'column_names': column_names,
          'column_types': column_types
        }
      })

    return table_schemas