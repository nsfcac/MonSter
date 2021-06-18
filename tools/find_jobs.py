# -*- coding: utf-8 -*-
"""
This module is used to find jobs running on nodes that matches specified 
cpu_load, memory used and power consumption given a time range.
"""
import re
import sys
import argparse
import datetime
import psycopg2

sys.path.append('../')

from sharings.utils import bcolors, parse_config, init_tsdb_connection

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

def main():
    # Read configuratin file
    config = parse_config('../config.yml')
    # Create TimeScaleDB connection
    connection = init_tsdb_connection(config)

    # Get user imput parameters
    args = get_argument()

    start = args.start
    end = args.end
    cpuload = args.cpuload
    memory_str = args.memory
    power_nocona = args.power_nocona
    power_quanah = args.power_quanah

    if not check_sanity(start, end, memory_str):
        return
    else:
        find_jobs(start, end, cpuload, memory_str, power_nocona, power_quanah, connection)


def find_jobs(start: str, end: str, cpuload: int, memory_str: str, 
              power_nocona: int, power_quanah: int, connection: str) -> list:
    memory = convert_memory(memory_str)

    if start != "now() - interval '5 minutes'":
        start = f"'{start}'"
    if end != "now()":
        end = f"'{end}'"

    with psycopg2.connect(connection) as conn:
        cur = conn.cursor()

        print(f"--> Finding nodes that {bcolors.HEADER}CPU LOAD <= {cpuload}{bcolors.ENDC} and {bcolors.HEADER}MEMORY USED <= {memory_str}{bcolors.ENDC}...")

        # print(f"Finding nodes that cpu load is less than {cpuload}...")
        cpu_load_nodelist = []
        cpu_load_sql = f"SELECT nodeid from slurm.cpu_load WHERE value <= {cpuload} AND timestamp >= {start} AND timestamp < {end}"
        cur.execute(cpu_load_sql)
        for (nodeid, ) in cur.fetchall():
            if nodeid not in cpu_load_nodelist:
                cpu_load_nodelist.append(nodeid)
        
        # print(f"Finding nodes that memory used is less than {memory}...")
        memory_nodelist = []
        memory_sql = f"SELECT nodeid from slurm.memory_used WHERE value <= {memory} AND timestamp >= {start} AND timestamp < {end}"
        cur.execute(memory_sql)
        for (nodeid, ) in cur.fetchall():
            if nodeid not in memory_nodelist:
                memory_nodelist.append(nodeid)

        # Find nodes appear in those two list
        all_nodelist = []
        for node in cpu_load_nodelist:
            if node in memory_nodelist:
                all_nodelist.append(node)
        
        all_nodelist.sort()
        print(f"--> {bcolors.OKBLUE}{len(all_nodelist)}{bcolors.ENDC} nodes are found.")
        
        # Find jobs running on those nodes
        all_jobs = []
        nodelist = []

        # Get nodeid-hostname mapping
        mapping = {}
        node_name_query = "SELECT nodeid, hostname FROM nodes"
        cur.execute(node_name_query)
        for (nodeid, hostname) in cur.fetchall():
            mapping.update({
                nodeid: hostname
            })
        
        no_record_nodes = []
        print(f"--> Selecting nodes that all cores are used, and finding corresponding jobs...")
        for node in all_nodelist:
            node_jobs_sql = f"SELECT jobs, cpus from slurm.node_jobs WHERE nodeid = {node} AND timestamp >= {start} AND timestamp < {end}"
            # print(node_jobs_sql)
            cur.execute(node_jobs_sql)
            rows = cur.fetchall()

            # If TSDB does not have corresponding records
            if not rows:
                no_record_nodes.append(mapping[node])

            for (jobs, cpus) in rows:
                # Count total cpus used in this node
                total_cpus = 0
                flag = False
                for cpu in cpus:
                    total_cpus += cpu

                if node > 489:
                    if total_cpus == 128:
                        flag = True
                elif node > 467:
                    if total_cpus == 40:
                        flag = True
                else:
                    if total_cpus == 36:
                        flag = True

                if flag:
                    if node not in nodelist:
                        nodelist.append(node)
                    for job in jobs:
                        if job not in all_jobs:
                            all_jobs.append(job)
        
        nodelist.sort()

        nodelist = [mapping[nodeid] for nodeid in nodelist]

        print(f"--> Nodes are: {bcolors.OKCYAN}{nodelist}{bcolors.ENDC}")
        print(f"--> Jobs are: {bcolors.OKGREEN}{all_jobs}{bcolors.ENDC}")
        
        if no_record_nodes:
            print(f"--> These nodes DO NOT have job record: {bcolors.WARNING}{no_record_nodes}{bcolors.ENDC}")
        
        print(f"--> Finding nodes that {bcolors.HEADER}POWER CONSUMPTION >= {power_nocona}(Nocona){bcolors.ENDC}...") # | {bcolors.HEADER}{power_quanah}(Quanah){bcolors.ENDC}
        noncona_nodelist = []
        noncona_pwr_sql = f"SELECT nodeid from idrac9.systempowerconsumption WHERE value >= {power_nocona} AND timestamp >= {start} AND timestamp < {end}"
        # print(noncona_pwr_sql)
        cur.execute(noncona_pwr_sql)
        for (nodeid, ) in cur.fetchall():
            if nodeid not in noncona_nodelist:
                noncona_nodelist.append(nodeid)
        
        print(f"--> {bcolors.OKBLUE}{len(noncona_nodelist)}{bcolors.ENDC} nodes (Nocona) are found.")

        all_jobs = []
        no_record_nodes = []
        for node in noncona_nodelist:
            node_jobs_sql = f"SELECT jobs, cpus from slurm.node_jobs WHERE nodeid = {node} AND timestamp >= {start} AND timestamp < {end}"
            # print(node_jobs_sql)
            cur.execute(node_jobs_sql)
            rows = cur.fetchall()
            if not rows:
                no_record_nodes.append(mapping[node])
            for (jobs, cpus) in rows:
                for job in jobs:
                    if job not in all_jobs:
                        all_jobs.append(job)
        
        noncona_nodelist.sort()
        noncona_nodelist = [mapping[nodeid] for nodeid in noncona_nodelist]

        print(f"--> Nodes are: {bcolors.OKCYAN}{noncona_nodelist}{bcolors.ENDC}")
        print(f"--> Jobs are: {bcolors.OKGREEN}{all_jobs}{bcolors.ENDC}")
        
        if no_record_nodes:
            print(f"--> These nodes DO NOT have job record: {bcolors.WARNING}{no_record_nodes}{bcolors.ENDC}")
        cur.close()

    return


def check_sanity(start: str, end: str, memory: str):
    sanity = True
    if start != "now() - interval '5 minutes'":
        try: 
            start_time = datetime.datetime.strptime(start, DATETIME_FORMAT)
        except:
            sanity = False
            print("Incorrect start time format. It should be YYYY-MM-DD HH:MM:SS")
            return sanity
    if end != "now()":
        try: 
            end_time = datetime.datetime.strptime(end, DATETIME_FORMAT)
        except:
            sanity = False
            print("Incorrect end time format. It should be YYYY-MM-DD HH:MM:SS")
            return sanity
    if convert_memory(memory) == -1:
        sanity = False
        return sanity
    
    # st = datetime.datetime.strptime(start, DATETIME_FORMAT).timestamp()
    # et = datetime.datetime.strptime(end, DATETIME_FORMAT).timestamp()
    # delta = et - st
    # if delta <= 0:
    #     sanity = False
    #     print("End time should late than start time.")

    return sanity


def convert_memory(memory: str) -> int:
    """
    Convert memory string to memory in MB
    """
    memory_used = -1
    memory_num = re.split("[mMgG]", memory, 1)[0]
    try:
        if 'g' in memory or 'G' in memory:
            memory_used = int(memory_num) *  1024
        else:
            memory_used = int(memory_num) 

    except Exception:
        print("Incorrect memory format. It should be numbers[m|M|g|G]")
    return memory_used
    

def get_argument():
    parser = argparse.ArgumentParser(
        prog=' find_jobs',
        usage = '%(prog)s [-s STARTTIME] [-e ENDTIME] [-c CPULOAD] [-m MEMORYUSED] | [-pn NOCONAPOWER] | [-pq QUANAOPOWER]',
        description='Find jobs based on cpu load and memory used or power consumption.')
    
    parser.add_argument('-s',
                        '--start', 
                        type = str, 
                        default = "now() - interval '5 minutes'",
                        help = 'start time')
    
    parser.add_argument('-e',
                        '--end', 
                        type = str, 
                        default = "now()",
                        help = 'end time')

    parser.add_argument('-c',
                        '--cpuload', 
                        type = int, 
                        default = 2,
                        help = 'maximum cpu load')
    
    parser.add_argument('-m',
                        '--memory', 
                        type = str,
                        default = '6G',
                        help = 'maximum memory used, in MB or GB')

    parser.add_argument('-pn',
                        '--power_nocona', 
                        type = int, 
                        default = 700,
                        help = 'minimum power consumption in Nocona')
    
    parser.add_argument('-pq',
                        '--power_quanah', 
                        type = int,
                        default=300, 
                        help='minimum power consumption in Quanah')

    args = parser.parse_args()

    return args

if __name__ == '__main__':
    main()