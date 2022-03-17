"""
MIT License

Copyright (c) 2022 Texas Tech University

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
import re
import json
import multiprocessing
from sqlite3 import connect
from datetime import datetime, timedelta

import sys
sys.path.insert(0, '../monster')

import sql
from tqdm import tqdm
import pandas as pd
import sqlalchemy as db
from itertools import repeat

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

def get_id_node_mapping(connection: str):
    """get_id_node_mapping Get ID-Node Mapping

    Get ID-Node Mapping

    Args:
        connection (str): connection string

    """
    engine = db.create_engine(connection)
    connect = engine.connect()
    # Only select quanah nodes
    mapping_sql = "SELECT nodeid, hostname FROM nodes WHERE nodeid < 468;"
    mapping_df = pd.read_sql_query(mapping_sql,con=connect)
    mapping = pd.Series(mapping_df.hostname.values, index=mapping_df.nodeid).to_dict()
    connect.close()
    return mapping


def get_metric_fqdd_mapping(connection: str):
    """get_metric_fqdd_mapping Get Metric-FQDD Mapping

    Get metric-fqdd mapping. If finding the metric_fqdd table in tsdb, query
    directly; build metric-fqdd table otherwise.

    Args:
        connection (str): connection string
    """
    engine = db.create_engine(connection)
    metadata = db.MetaData()
    connect = engine.connect()

    mapping = {}

    metric_list = get_avail_metrics(connect, metadata, engine)
    
    # Check if the metric_fqdd table exists
    if db.inspect(engine).has_table('metric_fqdd'):
        metric_fqdd = db.Table('metric_fqdd',
                                metadata,
                                autoload=True,
                                autoload_with=engine)
        query = db.select([metric_fqdd])
        result_proxy = connect.execute(query)
        result = result_proxy.fetchall()
        for i in result:
            mapping.update({
                i[1]: i[2]
            })
    else:
        # Create the metric_fqdd table
        metric_fqdd = db.Table('metric_fqdd', metadata,
                      db.Column('id', db.Integer, primary_key=True, autoincrement=True),
                      db.Column('metric_id', db.Text),
                      db.Column('fqdd',db.ARRAY(db.Text)))
        metadata.create_all(engine)

        # print("Collecting FQDD info...")
        # for metric in tqdm(metric_list):
        for metric in metric_list:
            fqdd = get_avail_metric_fqdd(connect, metadata, engine, metric)
            if fqdd:
                mapping.update({
                    metric: fqdd
                })

        for metric, fqdd in mapping.items():
            entry = db.insert(metric_fqdd).values(metric_id=metric, fqdd=fqdd)
            result=connect.execute(entry)

    connect.close()

    return mapping


def get_avail_metrics(connect: object, 
                      metadata: object, 
                      engine: object):
    """get_avail_metrics Get Available Metrics

    Get available metrics from the table metrics_definition

    Args:
        connect (object): SqlAlchemy engine connect
        metadata (object): SqlAlchemy metadata
        engine (object): SqlAlchemy engine
    """
    result = []
    metrics_definition = db.Table('metrics_definition',
                                  metadata,
                                  autoload=True,
                                  autoload_with=engine)
    query = db.select([metrics_definition])
    result_proxy = connect.execute(query)
    result = result_proxy.fetchall()
    metric_list = [i[1] for i in result]
    return metric_list


def get_avail_metric_fqdd(connect: object, 
                          metadata: object, 
                          engine: object,
                          metric: str):
    """get_avail_metric_fqdd Get Available Metric FQDD

    Get available fqdd of a metric based on the metrics collected in the last
    half hour.

    Args:
        connect (object): SqlAlchemy engine connect
        metadata (object): SqlAlchemy metadata
        engine (object): SqlAlchemy engine
        metric: metric name
    """
    fqdd = []
    metric = metric.lower()
    table = db.Table(metric,
                     metadata,
                     autoload=True,
                     autoload_with=engine,
                     schema = 'idrac')

    # Find unique fqdd values
    query = db.select([table.columns.fqdd.distinct()]).limit(20)
    result_proxy = connect.execute(query)
    result = result_proxy.fetchall()

    if result:
        fqdd = [i[0] for i in result if i[0]]

    return fqdd


def query_tsdb_parallel(request: object, id_node_mapping: dict, connection: str):
    """query_tsdb_parallel Query TSDB in Parallel

    Query TSDB based on the flask request.

    Args:
        request (object): flask request
        id_node_mapping (dict): Node-ID mapping
        connection (str): tsdb connection
    """

    results = []
    
    # Request details
    time_range = request.get('range')
    interval = request.get('interval')
    targets = request.get('targets')

    # Extract time range (from, to), metrics
    start = time_range.get('from')
    end = time_range.get('to')

    # TO DO: add aggregation function in the targets
    targets_length = len(targets)
    with multiprocessing.Pool(targets_length) as pool:
        query_tsdb_args = zip(targets, 
                              repeat(id_node_mapping), 
                              repeat(connection),
                              repeat(start),
                              repeat(end),
                              repeat(interval))
        results = pool.starmap(query_tsdb, query_tsdb_args)

    # Aggregate results
    agg_results = aggregate_results(results)
    return agg_results


def aggregate_results(results: list):
    aggregated_data = {
        'nodes_info': {},
        'jobs_info': {}
    }

    for record in results:
        if record['type'] == 'jobs':
            aggregated_data['jobs_info'] = record['result']
        else:
            for node, metrics in record['result'].items():
                if node not in aggregated_data['nodes_info']:
                    aggregated_data['nodes_info'].update({
                        node: {}
                    })
                    aggregated_data['nodes_info'][node] = metrics
                else:
                    for sensor, readings in metrics.items():
                        aggregated_data['nodes_info'][node].update({
                            sensor: readings
                        })

    return aggregated_data


def query_tsdb(target: dict, 
               id_node_mapping: dict, 
               connection: str,
               start: str,
               end: str,
               interval: str):
    """query_tsdb Query TSDB for each target

    Args:
        target (dict): target of data source request
        id_node_mapping (dict): Node-ID mapping
        connection (str): tsdb connection
        start (str): start of time range
        end (str): end of time range
        interval (str): aggregation interval
    """
    
    # Initialize sqlalchemy connection
    engine = db.create_engine(connection)
    req_metric = target.get('metric', '')
    req_type = target.get('type', '')
    nodes = target.get('nodes', '')
    results = []

    if req_metric and req_type == 'metrics' and len(req_metric.split(' | ')) == 3:
        partition = req_metric.split(' | ')[0]
        metric = req_metric.split(' | ')[1]
        fqdd = req_metric.split(' | ')[2]
        metrics = query_filter_metrics(engine,
                                        metric, 
                                        fqdd,
                                        nodes,
                                        id_node_mapping, 
                                        start,
                                        end,
                                        interval,
                                        partition)
        # results = metrics
        results = {'type': 'metrics', 'result': metrics}

    if req_type == 'jobs':
        users = target.get('users', '')
        if not users:
            users = get_users(engine, start, end)
        jobs = query_filter_jobs(engine, users, start, end, id_node_mapping)
        
        # results = jobs
        results = {'type': 'jobs', 'result': jobs}

    if req_type == 'node_core':
        node_core = query_node_core(engine, 
                                    start, 
                                    end, 
                                    interval, 
                                    id_node_mapping)
        # results = node_core
        results = {'type': 'node_core', 'result': node_core}

    engine.dispose()
    return results
    

def query_filter_metrics(engine: object,
                         metric: str,
                         fqdd: str,
                         nodes: list,
                         id_node_mapping: dict,
                         start: str,
                         end: str,
                         interval: str,
                         partition: str,
                         aggregate: str = 'max'):
    """query_filter_metrics Query Filter Metrics

    Query and filter metrics from TSDB

    Args:
        engine (object): sqlalchemy engine
        metric (str): metric name
        fqdd (str): fqdd name
        nodes (list): target nodes
        id_node_mapping (dict): id-node mapping
        start (str): start of time range
        end (str): end of time range
        interval (str): aggregation interval
        partition (str): partition name. 
        aggregate (str, optional): aggregation function. Defaults to 'max'.
    
    """
    if partition == 'slurm':
        sql_str = sql.generate_slurm_sql(metric, 
                                         start, 
                                         end, 
                                         interval, 
                                         aggregate)
    else:
        sql_str = sql.generate_idrac_sql(metric,
                                         fqdd,
                                         start, 
                                         end, 
                                         interval, 
                                         aggregate,
                                         'idrac8')
    
    df = pd.read_sql_query(sql_str, con=engine)

    # Replace Nan with None
    df.fillna('',inplace=True)

    # Filter nodes
    if nodes:
        fi_df = df[df['nodeid'].isin(nodes)].copy()
    else:
        fi_df = df

    # Add label in slurm metrics
    if partition == 'slurm':
        fi_df['label'] = metric

    # Convert node id to node name
    fi_df['nodeid'] = fi_df['nodeid'].apply(lambda x: id_node_mapping[x])
    fi_df['label'] = fi_df['label'].apply(lambda x: f'{metric}|{x}')

    # Pivot the table
    df_p = fi_df.pivot(index='time', columns=['nodeid', 'label'], values='value')
    
    # Flatten the table
    df_p.columns = [ '|'.join([str(c) for c in c_list]) for c_list in df_p.columns.values ]
    
    metrics = metrics_df_to_response(df_p)

    return metrics


def metrics_df_to_response(df: object):
    """metrics_df_to_response Metrics DF to Response

    Convert dataframe to response in format required by Grafana

    Args:
        df (dataframe): dataframe

    """
    # Convert index to column
    df.reset_index(inplace=True)

    # Convert datetime to epoch time
    df['time'] = df['time'].apply(lambda x: int(x.timestamp() ))

    df_dict = df.to_dict(orient='list')
    df_json = process_metric_df_dict(df_dict)

    # print(df_json)
    return df_json


def process_metric_df_dict(df_dict: dict):
    df_json = {}
    
    for key, values in df_dict.items():
        if key != 'time':
            node = key.split('|')[0]
            sensor = key.split('|')[2].replace(' ', '_')

            if sensor == 'System_Power_Control':
                sensor = 'power_consumption'

            df_json.update({
                node: {
                    sensor: values
                }
            })

    return df_json


def get_users(engine: object, start: str, end: str):
    """get_users Get Users

    Get users from the slurm jobs info

    Args:
        engine (object): sqlalchemy engine
        start (str): start time
        end (str): end time

    """    
    sql_str = sql.generate_slurm_jobs_sql(start, end)
    df = pd.read_sql_query(sql_str,con=engine)
    users = df['user_name'].unique()
    
    # Remove duplicated users
    all_users = list(set(users))
    return all_users


def query_filter_jobs(engine: object,
                      users: list,
                      start: str,
                      end: str,
                      id_node_mapping: dict):
    """query_filter_jobs Query Filter jobs

    Query and filter jobs from TSDB

    Args:
        engine (object): sqlalchemy engine
        users (list): selected users
        start (str): start of time range
        end (str): end of time range
        id_node_mapping (dict): id-node mapping

    """
    sql_str = sql.generate_slurm_jobs_sql(start, end)
    df = pd.read_sql_query(sql_str,con=engine)

    # Node hostname
    nodes = list(id_node_mapping.values())

    # Filter jobs based on users and the batch_host that falls in the nodes and 
    fi_df = df[df['batch_host'].isin(nodes) & df['user_name'].isin(users)].copy()

    jobs = jobs_df_to_response(fi_df)
    return jobs


def jobs_df_to_response(df: object):
    """jobs_df_to_response Jobs DF to Response

    Convert dataframe to response in format required by Grafana

    Args:
        df (dataframe): dataframe

    Returns:
        response (dict): response
    """
    columns = []
    selected_columns = ['job_id', 'name', 'user_id', 'user_name', 'batch_host', 
                        'nodes', 'node_count', 'cpus', 'start_time', 'end_time']
    selected_df = df[selected_columns].copy()
    df_json = selected_df.set_index('job_id').to_dict(orient='index')

    return df_json


def query_node_core(engine: object,
                    start: str,
                    end: str,
                    interval: str,
                    id_node_mapping: dict):
    """query_node_core Query Node Core

    Query Node-Core info from TSDB

    Args:
        engine (object): sqlalchemy engine
        start (str): start of time range
        end (str): end of time range
        interval (str): time interval for aggregation

    """
    sql_str = sql.generate_node_jobs_sql(start, end, interval)
    df = pd.read_sql_query(sql_str,con=engine)
    node_jobs = node_jobs_df_to_response(df, id_node_mapping)
    return node_jobs


def node_jobs_df_to_response(df: object, id_node_mapping: dict):
    """node_jobs_df_to_response Node-Jobs DF to Response

    Convert dataframe to response in format required by Grafana

    Args:
        df (dataframe): dataframe

    Returns:
        response (dict): response
    """

    node_jobs = {}
    try:
        df['time'] = df['time'].apply(
            lambda x: int(x.timestamp())
        )
        df['fl_jobs'] = df.apply(
            lambda df_row: flatten_array(df_row, 'jobs'), axis = 1)
        df['fl_cpus'] = df.apply(
            lambda df_row: flatten_array(df_row, 'cpus'), axis = 1)
        df.drop(columns = ['jobs', 'cpus'], 
                        inplace = True)
        df.rename(columns={'fl_jobs': 'jobs', 'fl_cpus': 'cpus'}, 
                        inplace = True)

        grouped_df = df.groupby(['nodeid'])#[['time', 'jobs', 'cpus']]
        for key, item in grouped_df:
            jobs = grouped_df.get_group(key)['jobs'].tolist()
            cpus = grouped_df.get_group(key)['cpus'].tolist()
            node_name = id_node_mapping[key]
            node_jobs.update({
                node_name:{
                    'job_id': jobs,
                    'cores': cpus
                }
            })
    except Exception as err:
        pass

    return node_jobs


def flatten_array(df_row: object, column: str):
    """flatten_array Flatten Array

    Flatten array of array for jobs and cores info

    Args:
        df_row (object): dataframe row
        column (str): column name

    """
    jobs = []
    cpus = []
    job_id_array = df_row['jobs']
    cpus_array = df_row['cpus']
    try:
        if job_id_array:
            # Flatten array
            fl_job_id_array = [item for sublist in job_id_array for item in sublist]
            fl_cpus_array = [item for sublist in cpus_array for item in sublist]

            # Only keep unique jobs
            for i, job in enumerate(fl_job_id_array):
                if job not in jobs:
                    jobs.append(str(job))
                    cpus.append(str(fl_cpus_array[i]))
    except:
        # print(f"{df_row['time']} - {df_row['nodeid']}")
        pass
    if column == 'jobs':
        str_jobs = '{' + (', ').join(jobs) + '}'
        return str_jobs
    else:
        str_cpus = '{' + (', ').join(cpus) + '}'
        return str_cpus


def gen_epoch_timelist(start: str, end: str, interval: str):
    delta = time_delta(interval)
    time_list = [int(dt.timestamp()) for dt in datetime_range(
        start, end, delta
    )]
    return time_list


def datetime_range(start: str, end: str, interval: str):
    """Generate time interval array"""
    start = datetime.strptime(start, DATETIME_FORMAT)
    end = datetime.strptime(end, DATETIME_FORMAT)
    current = start
    while current <= end:
        yield current
        current += interval


def time_delta(timeInterval: str) -> str:
    """Validate time interval and generate timedelta object"""
    time_valid = re.compile('[1-9][0-9]*[s, m, h, d, w]')
    if not time_valid.match(timeInterval):
        return None
    if "s" in timeInterval:
        num = int(timeInterval.split('s')[0])
        delta = timedelta(seconds = num)
    elif "m" in timeInterval:
        num = int(timeInterval.split('m')[0])
        delta = timedelta(minutes = num)
    elif "h" in timeInterval:
        num = int(timeInterval.split('h')[0])
        delta = timedelta(hours = num)
    elif "d" in timeInterval:
        num = int(timeInterval.split('d')[0])
        delta = timedelta(days = num)
    else:
        num = int(timeInterval.split('w')[0])
        delta = timedelta(weeks = num)
    return delta