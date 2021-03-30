"""
This module export data in TimeScaleDB to csv file
"""
import sys
import csv
import json
import psycopg2
sys.path.append('../')

from pgcopy import CopyManager
from sharings.utils import init_tsdb_connection


def main():
    # Read configuratin file
    config = {
        'timescaledb':{
            'host': '0.0.0.0',
            'port': 5432,
            'user': 'monster',
            'password': 'redraider',
            'database': 'redraider'
            # 'database': 'demo'
        }
    }

    # Create TimeScaleDB connection
    connection = init_tsdb_connection(config)
    
    id_host = {}
    aggregated_metrics = {}
    with psycopg2.connect(connection) as conn:
        # Get nodeid-hostname mapping
        sql = "SELECT nodeid, hostname FROM nodes where nodeid > 488"
        fields = ['nodeid', 'hostname']
        mapping = query_tsdb(conn, sql, fields)['rows']
        for item in mapping:
            id_host.update({
                item[0]: item[1]
            })
        
        # print(json.dumps(id_host, indent=4))
        all_hosts = [int(i) for i in list(id_host.keys())]
        for host in all_hosts:
            query_host_data(host, id_host, conn, aggregated_metrics)
        
        with open('./data/nocona_aggregated.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            headers = aggregated_metrics.keys()
            csvwriter.writerow(headers)

            for index, time in enumerate(aggregated_metrics['time']):
                rows = []
                for header in headers:
                    this_entry = aggregated_metrics[header][index]
                    rows.append(this_entry)
                csvwriter.writerow(rows)
                    
                
        # with open('./data/aggregated.json', 'w') as jsonfile:
        #     json.dump(aggregated_metrics, jsonfile, indent=4)

def query_host_data(nodeid: int, id_host: dict, conn, 
                    aggregated_metrics: dict) -> None:
    hostname = id_host[nodeid]

    # Node-jobs
    sql = f"select time_bucket_gapfill('1 minutes', timestamp) as time, array_agg(jobs) as jobs, array_agg(cpus) as cpus from slurm.node_jobs where nodeid = {nodeid} and timestamp >= '2021-03-21 15:00:00-05' and timestamp <= '2021-03-23 15:00:00-05' group by time order by time;"
    fields = ['time', 'jobs', 'cpus']
    tsdb_data = query_tsdb(conn, sql, fields)

    this_jobs = hostname + '_jobs'
    this_cpus = hostname + '_cpus'

    if 'time' not in aggregated_metrics:
        aggregated_metrics.update({
            'time': [],
            this_jobs: [],
            this_cpus: []
        })
    else:
        aggregated_metrics.update({
            this_jobs: [],
            this_cpus: []
        })

    # Convert datetime to epoch time
    timestamps = []
    this_jobs_data = []
    this_cpus_data = []
    for rows_data in tsdb_data['rows']:
        try:
            jobs = rows_data[1][0]
            cpus = rows_data[2][0]
        except:
            jobs = []
            cpus = []
        timestamps.append(int(rows_data[0].timestamp()))
        this_jobs_data.append(jobs)
        this_cpus_data.append(cpus)

    if not aggregated_metrics['time']:
        aggregated_metrics['time'] = timestamps
    aggregated_metrics[this_jobs] = this_jobs_data
    aggregated_metrics[this_cpus] = this_cpus_data

    # Node-power
    sql = f"select time_bucket_gapfill('1 minutes', timestamp) as time, max(value) as value from idrac9.systempowerconsumption where nodeid = {nodeid} and timestamp >= '2021-03-21 15:00:00-05' and timestamp <= '2021-03-23 15:00:00-05' group by time order by time;"
    fields = ['time', 'power']
    tsdb_data = query_tsdb(conn, sql, fields)

    this_power = hostname + '_power'

    aggregated_metrics.update({
        this_power: []
    })

    this_power_data = []
    for rows_data in tsdb_data['rows']:
        try:
            power = int(rows_data[1])
        except:
            power = None
        this_power_data.append(power)

    aggregated_metrics[this_power] = this_power_data



def export_csv(nodeid: int, id_host: dict, conn) -> None:
    folder = 'rack_' + id_host[nodeid].split('-')[1]
    filename = id_host[nodeid] + '.csv'
    path = f'./data/node_jobs/{folder}/{filename}'
    sql = f"select time_bucket_gapfill('1 minutes', timestamp) as time, array_agg(jobs) as jobs, array_agg(cpus) as cpus from slurm.node_jobs where nodeid = {nodeid} and timestamp >= '2021-03-21 15:00:00-05' and timestamp <= '2021-03-23 15:00:00-05' group by time order by time;"
    tsdb_data = query_tsdb(conn, sql)

    # Convert datetime to epoch time
    rows_data_epoch = []
    for rows_data in tsdb_data['rows']:
        try:
            jobs = rows_data[1][0]
            cpus = rows_data[2][0]
        except:
            jobs = []
            cpus = []
        # rows_data_epoch.append([int(rows_data[0].timestamp()) - 21600, value])
        rows_data_epoch.append([int(rows_data[0].timestamp()), jobs, cpus])

    
    with open(path, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(tsdb_data['fields'])
        # print(len(rows_data_epoch))
        for item in rows_data_epoch:
            csvwriter.writerow(item)


def query_tsdb(conn:object, sql: str, fields: list) -> None:
    """
    Query TimeScaleDB
    """
    tsdb_data = {
        'fields':[],
        'rows':[]
    }
    try:
        cur = conn.cursor()
        cur.execute(sql)
        # fields = ['time', 'value']
        # fields = ['time', 'jobs', 'cpus']
        rtn = cur.fetchall()
        rows = [list(item) for item in rtn]
        # print(rows)
        tsdb_data.update({
            'fields': fields,
            'rows': rows
        })
    except Exception as err:
        print(err)
    return tsdb_data    


if __name__ == '__main__':
    main()