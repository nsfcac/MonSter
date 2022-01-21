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

import utils
import dump
import logger
import process
import rparser
import asyncio
import aiohttp
import psycopg2
import multiprocessing

from aiohttp import ClientSession

log = logger.get_logger(__name__)


async def decode_message(ip: str, line: str):
    """decode_message Decode Message

    Decode each line in the idrac streaming data

    Args:
        ip (str): ip address of idrac
        line (str): each line in the idrac streaming data

    Returns:
        tuple: ip info and its corresponding json-format metrics
    """
    if line:
        try:
            decoded_line = line.decode('utf-8', 'ignore')
            if '{' in decoded_line:
                decoded_line = decoded_line.strip('data: ')

                # Use the customized parser
                report = rparser.report_parser(decoded_line)
                # report = json.loads(decoded_line)
                data = (ip, report)
                return data
        except Exception as err:
                log.error(f"Fail to decode ({ip}): {err}")


async def listen_idrac(ip: str,
                       username: str,
                       password: str,
                       mr_queue: asyncio.Queue):
    """listen_idrac Listen iDRAC

    Listen to iDRAC streaming

    Args:
        ip (str): ip address of idrac
        username (str): username for idrac authentication
        password (str): password for idrac authentication
        mr_queue (asyncio.Queue): Queue for metrics reading
    """
    async with ClientSession( connector = aiohttp.TCPConnector(ssl=False, 
                                                        force_close=False, 
                                                        limit=None), 
                            auth=aiohttp.BasicAuth(username, password),
                            timeout = aiohttp.ClientTimeout(total= 0) ) as session:
        url = f"https://{ip}/redfish/v1/SSE?$filter=EventFormatType%20eq%20MetricReport"
        while True:
            try:
                async with session.get(url) as resp:
                    async for line in resp.content:
                        data = await decode_message(ip, line)
                        if data:
                            # print(f"Producing {ip}")
                            await mr_queue.put(data)
                        # Force task switch
                        await asyncio.sleep(0)
            except Exception as err:
                log.error(f"Cannot collect metrics from ({ip}): {err}")


async def process_idrac(mr_queue: asyncio.Queue,
                        mp_queue: asyncio.Queue):
    """process_idrac Process iDRAC

    Process iDRAC streaming data

    Args:
        mr_queue (asyncio.Queue): Queue for metrics reading
        mp_queue (asyncio.Queue): Queue for metrics processing
    """
    while True:
        data = await mr_queue.get()
        ip = data[0]
        report = data[1]
        
        report_id = report.get('Id', None)
        metric_values = report.get('MetricValues', None)

        if report_id and metric_values:
            processed_metrics = process.process_idrac(ip, report_id, metric_values)
            if processed_metrics:
                metric_tuple = (ip, processed_metrics)
                await mp_queue.put(metric_tuple)
            mr_queue.task_done()


async def write_idrac(metric_dtype_mapping: dict, 
                      ip_id_mapping: dict, 
                      conn: object, 
                      mp_queue: asyncio.Queue):
    """write_idrac Write iDRAC

    Write processed iDRAC metrics to database

    Args:
        metric_dtype_mapping (dict): metric-datatype mapping
        ip_id_mapping (dict): ip-id mapping
        conn (object): psycopg2 connection object
        mp_queue (asyncio.Queue): Queue for metrics processing
    """
    while True:
        metric_tuple = await mp_queue.get()
        ip = metric_tuple[0]
        processed_metrics = metric_tuple[1]
        
        dump.dump_idrac(ip, processed_metrics, metric_dtype_mapping, ip_id_mapping, conn)
        mp_queue.task_done()


async def monitor_idrac(buf_size: int,
                        conn: object, 
                        username: str, 
                        password: str, 
                        nodelist: list,
                        ip_id_mapping: dict,
                        metric_dtype_mapping: dict):
    """monitor_idrac Monitor iDRAC

    Monitoring iDRAC wrapper function

    Args:
        buf_size (int): queue buffer size
        conn (object): psycopg2 connection object
        username (str): username for idrac authentication
        password (str): password for idrac authentication
        nodelist (list): list of target nodes/iDRACs
        ip_id_mapping (dict): ip-id mapping
        metric_dtype_mapping (dict): metric-datatype mapping
    """
    # Metric Read Queue
    mr_queue = asyncio.Queue(maxsize=buf_size)

    # Metric Process Queue
    mp_queue = asyncio.Queue(maxsize=buf_size)
    
    listener = [asyncio.create_task(listen_idrac(ip, username, password, mr_queue)) for ip in nodelist]
    processor = [asyncio.create_task(process_idrac(mr_queue, mp_queue))]
    writer = [asyncio.create_task(write_idrac(metric_dtype_mapping, ip_id_mapping, conn, mp_queue))]

    tasks = listener + processor + writer
    await asyncio.gather(*tasks)


def asyncio_run(buf_size: int,
                username: str, 
                password: str, 
                nodelist: list):
    """asyncio_run Asyncio Run

    Asyncio run the idrac data collection code

    Args:
        buf_size (int): queue buffer size
        username (str): username for idrac authentication
        password (str): password for idrac authentication
        nodelist (list): list of target nodes/iDRACs
    """
    with psycopg2.connect(connection) as conn:
        ip_id_mapping = utils.get_ip_id_mapping(conn)
        metric_dtype_mapping = utils.get_metric_dtype_mapping(conn)
        while True:
            asyncio.run(monitor_idrac(buf_size,
                                      conn, 
                                      username, 
                                      password, 
                                      nodelist,
                                      ip_id_mapping,
                                      metric_dtype_mapping))


def parallel_monitor_idrac(cores: int,
                           buf_size: int, 
                           username: str, 
                           password: str,
                           nodelist: list):
    """parallel_monitor_idrac Parallel Monitor iDRAC

    Running data collection code in parallel. The tasks are evenly distributed 
    to the cores of the monitoring node to improve the performance.

    Args:
        cores (int): number of cores available
        buf_size (int): queue buffer size
        username (str): username for idrac authentication
        password (str): password for idrac authentication
        nodelist (list): list of target nodes/iDRACs
    """
    args = []
    
    if len(nodelist) < cores:
        asyncio_run(buf_size, username, password, nodelist)
    else:
        node_list_groups = utils.partition(nodelist, cores)

        for i in range(cores):
            nodes = node_list_groups[i]
            args.append((buf_size, username, password, nodes))
        
        with multiprocessing.Pool() as pool:
            pool.starmap(asyncio_run, args)

    return


if __name__ == '__main__':
    buf_size = 240
    connection = utils.init_tsdb_connection()
    username, password = utils.get_idrac_auth()
    nodelist = utils.get_nodelist()

    cores = multiprocessing.cpu_count()
    
    parallel_monitor_idrac(cores, buf_size, username, password, nodelist)
        