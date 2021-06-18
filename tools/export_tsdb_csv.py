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
from slurmapi.fetch_slurm_sacct import fetch_slurm_sacct

START = '2021-06-18 06:00:00-05'
END = '2021-06-18 08:00:00-05'

def change_time_fmt(time_str: str) -> str:
    """
    Change time format '2021-06-17 06:00:00-05' to '2021-06-17T08:00:00'
    """
    splt_char = '-'
    temp = time_str.split(splt_char)
    kept = splt_char.join(temp[:3])
    new_time_str = kept.replace(' ', 'T')
    return new_time_str


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
    nodes_data = {}
    jobs_data = {}
    all_jobs_id = []
    with psycopg2.connect(connection) as conn:
        # Get nodeid-hostname mapping, currently only get Nocona nodes
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
            query_host_data(host, id_host, conn, aggregated_metrics, 
                            nodes_data, all_jobs_id)
        

        starttime = change_time_fmt(START)
        endtime = change_time_fmt(END)

        jobs_data = fetch_slurm_sacct(starttime, endtime)

        aggregated_metrics.update({
            'nodes_info': nodes_data,
            'jobs_info': jobs_data
        })

        starttime_name = starttime.replace(':', '_')
        endtime_name = endtime.replace(':', '_')
        with open(f'./data/aggregated_metrics_{starttime_name}_{endtime_name}.json', 'w') as jsonfile:
            json.dump(aggregated_metrics, jsonfile, indent=4)


# def query_jobs_data(jobid: int, id_host: dict, conn, jobs_data: dict) -> dict:
#     sql = f"select * from slurm.jobs where job_id = {jobid};"
#     cols = ['job_id', 'array_job_id', 'array_task_id', 'name','job_state', 
#                 'user_id', 'user_name', 'group_id', 'cluster', 'partition', 
#                 'command', 'current_working_directory', 'batch_flag', 
#                 'batch_host', 'nodes', 'node_count', 'cpus', 'tasks', 
#                 'tasks_per_node', 'cpus_per_task', 'memory_per_node', 
#                 'memory_per_cpu', 'priority', 'time_limit', 'deadline', 
#                 'submit_time', 'preempt_time', 'suspend_time', 'eligible_time', 
#                 'start_time', 'end_time', 'resize_time', 'restart_cnt', 
#                 'exit_code', 'derived_exit_code']
#     cur = conn.cursor()
#     job_data = {}
#     try:
#         cur.execute(sql)
#         rtn = cur.fetchall()
#         for index, item in enumerate(rtn[0]):
#             if cols[index] == 'batch_host':
#                 item_value = id_host[item]
#             elif cols[index] == 'nodes':
#                 item_value = [id_host[i] for i in item]
#             else:
#                 item_value = item
#             job_data.update({
#                 cols[index]: item_value
#             })
#         jobs_data.update({
#             jobid: job_data
#         })
#     except Exception as err:
#         print(err)
#     return


def query_host_data(nodeid: int, id_host: dict, conn, 
                    aggregated_metrics: dict, 
                    nodes_data: dict, all_jobs_id: list) -> None:
    hostname = id_host[nodeid]
    node_data = {}

    # Node-jobs
    sql = f"select time_bucket_gapfill('5m', timestamp) as time, array_agg(distinct job) as jobs from slurm.node_jobs, unnest(jobs) as job where nodeid = {nodeid} and timestamp >= '{START}' and timestamp < '{END}' group by time order by time;"
    # select time_bucket_gapfill('5m', timestamp) as time, array_agg(distinct job) as jobs from slurm.node_jobs, unnest(jobs) as job where nodeid = 553 and timestamp >= '2021-06-17 06:00:00-05' and timestamp < '2021-06-17 08:00:00-05' group by time order by time;
    # print(sql)
    fields = ['time', 'jobs']
    tsdb_data = query_tsdb(conn, sql, fields)

    if 'time_stamp' not in aggregated_metrics:
        aggregated_metrics.update({
            'time_stamp': [],
        })

    # Convert datetime to epoch time
    timestamps = []
    this_jobs_data = []
    # this_cpus_data = []
    for rows_data in tsdb_data['rows']:
        try:
            jobs = rows_data[1]
            # cpus = rows_data[2]
        except:
            jobs = []
            # cpus = []
        timestamps.append(int(rows_data[0].timestamp()))
        this_jobs_data.append(jobs)
        # this_cpus_data.append(cpus)

        # # Put jobs in all_jobs_id:
        # for job in jobs:
        #     if job not in all_jobs_id:
        #         all_jobs_id.append(job)

    if not aggregated_metrics['time_stamp']:
        aggregated_metrics['time_stamp'] = timestamps

    node_data['jobs'] = this_jobs_data
    # node_data['cpus'] = this_cpus_data

    # Node-power
    sql = f"select time_bucket_gapfill('5m', timestamp) as time, max(value) as value from idrac9.systempowerconsumption where nodeid = {nodeid} and timestamp >= '{START}' and timestamp < '{END}' group by time order by time;"
    # print(sql)
    fields = ['time', 'power']
    tsdb_data = query_tsdb(conn, sql, fields)

    # this_power = hostname + '_power'

    # aggregated_metrics.update({
    #     this_power: []
    # })

    this_power_data = []
    for rows_data in tsdb_data['rows']:
        try:
            power = int(rows_data[1])
        except:
            power = None
        this_power_data.append(power)

    node_data['power'] = this_power_data

    # memory-power
    sql = f"select time_bucket_gapfill('5m', timestamp) as time, max(value) as value from idrac9.totalmemorypower where nodeid = {nodeid} and timestamp >= '{START}' and timestamp < '{END}' group by time order by time;"
    # print(sql)
    fields = ['time', 'mem_power']
    tsdb_data = query_tsdb(conn, sql, fields)

    # this_mem_power = hostname + '_mem_power'

    # aggregated_metrics.update({
    #     this_mem_power: []
    # })

    this_mem_power_data = []
    for rows_data in tsdb_data['rows']:
        try:
            mem_power = int(rows_data[1])
        except:
            mem_power = None
        this_mem_power_data.append(mem_power)

    node_data['mem_power'] = this_mem_power_data

    # memory-usage
    sql = f"select time_bucket_gapfill('5m', timestamp) as time, max(value) as value from slurm.memoryusage where nodeid = {nodeid} and timestamp >= '{START}' and timestamp < '{END}' group by time order by time;"
    # print(sql)
    fields = ['time', 'mem_usage']
    tsdb_data = query_tsdb(conn, sql, fields)

    # this_mem_usage = hostname + '_mem_usage'

    # aggregated_metrics.update({
    #     this_mem_usage: []
    # })

    this_mem_usage_data = []
    for rows_data in tsdb_data['rows']:
        try:
            mem_usage = float(rows_data[1])
        except:
            mem_usage = None
        this_mem_usage_data.append(mem_usage)

    node_data['mem_usage'] = this_mem_usage_data


    # cpu-power
    sql = f"select time_bucket_gapfill('5m', timestamp) as time, max(value) as value from idrac9.totalcpupower where nodeid = {nodeid} and timestamp >= '{START}' and timestamp < '{END}' group by time order by time;"
    # print(sql)
    fields = ['time', 'cpu_power']
    tsdb_data = query_tsdb(conn, sql, fields)

    this_cpu_power_data = []
    for rows_data in tsdb_data['rows']:
        try:
            cpu_power = int(rows_data[1])
        except:
            cpu_power = None
        this_cpu_power_data.append(cpu_power)

    node_data['cpu_power'] = this_cpu_power_data

    # cpu-usage
    sql = f"select time_bucket_gapfill('5m', timestamp) as time, max(value) as value from idrac9.cpuusage where nodeid = {nodeid} and timestamp >= '{START}' and timestamp < '{END}' group by time order by time;"
    # print(sql)
    fields = ['time', 'cpu_usage']
    tsdb_data = query_tsdb(conn, sql, fields)

    # this_mem_usage = hostname + '_mem_usage'

    # aggregated_metrics.update({
    #     this_mem_usage: []
    # })

    this_cpu_usage_data = []
    for rows_data in tsdb_data['rows']:
        try:
            cpu_usage = float(rows_data[1])
        except:
            cpu_usage = None
        this_cpu_usage_data.append(cpu_usage)

    node_data['cpu_usage'] = this_cpu_usage_data

    # Update nodes data
    nodes_data.update({
        hostname: node_data
    })



def export_csv(nodeid: int, id_host: dict, conn) -> None:
    folder = 'rack_' + id_host[nodeid].split('-')[1]
    filename = id_host[nodeid] + '.csv'
    path = f'./data/node_jobs/{folder}/{filename}'
    sql = f"select time_bucket_gapfill('5m', timestamp) as time, array_agg(jobs) as jobs, array_agg(cpus) as cpus from slurm.node_jobs where nodeid = {nodeid} and timestamp >= '{START}' and timestamp < '{END}' group by time order by time;"
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
        conn.rollback()
        print(f"{err} - {sql}")
    return tsdb_data    


if __name__ == '__main__':
    main()