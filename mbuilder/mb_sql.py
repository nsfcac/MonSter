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

from dateutil.parser import parse


def generate_slurm_jobs_sql(start: str, end: str):
  start_epoch = int(parse(start).timestamp())
  end_epoch   = int(parse(end).timestamp())
  sql = f"SELECT * FROM slurm.jobs WHERE start_time < {start_epoch} AND end_time > {end_epoch};"
  return sql


def generate_slurm_node_jobs_sql(start: str, end: str, interval: str):
  sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
          nodes.hostname as node, jsonb_agg(jobs) AS jobs, jsonb_agg(cpus) AS cpus \
          FROM slurm.node_jobs \
          JOIN nodes \
          ON slurm.node_jobs.nodeid = nodes.nodeid \
          WHERE timestamp >= '{start}' \
          AND timestamp <= '{end}' \
          GROUP BY time, node \
          ORDER BY time;"
  return sql


def generate_slurm_state_sql(start: str, end: str, interval: str):
  sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
          nodes.hostname as node, jsonb_agg(value) AS value \
          FROM slurm.state \
          JOIN nodes \
          ON slurm.state.nodeid = nodes.nodeid \
          WHERE timestamp >= '{start}' \
          AND timestamp <= '{end}' \
          GROUP BY time, node \
          ORDER BY time;"
  return sql


def generate_idrac_metric_sql(table: str, 
                              start: str, 
                              end: str, 
                              interval: str, 
                              aggregation: str):
  sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
          nodes.hostname as node, fqdd.fqdd AS label, {aggregation}(value) AS value \
          FROM idrac.{table} \
          JOIN nodes \
          ON idrac.{table}.nodeid = nodes.nodeid \
          JOIN fqdd \
          ON idrac.{table}.fqdd = fqdd.id \
          WHERE timestamp >= '{start}' \
          AND timestamp < '{end}' \
          GROUP BY time, node, label \
          ORDER BY time;"
  return sql


def generate_slurm_metric_sql(table: str, 
                              start: str, 
                              end: str, 
                              interval: str, 
                              aggregation: str):
  sql = f"SELECT time_bucket_gapfill('{interval}', timestamp) AS time, \
          nodes.hostname as node, {aggregation}(value) AS value \
          FROM slurm.{table} \
          JOIN nodes \
          ON slurm.{table}.nodeid = nodes.nodeid \
          WHERE timestamp >= '{start}' \
          AND timestamp < '{end}' \
          GROUP BY time, node \
          ORDER BY time;"
  return sql


  
  