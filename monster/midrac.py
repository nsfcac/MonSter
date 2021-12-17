import utils
import dump
import logger
import process
import rparser
import asyncio
import aiohttp
import psycopg2
import multiprocessing

from async_retrying import retry
from aiohttp import ClientSession
from aiohttp_sse_client import client as sse_client

log = logger.get_logger(__name__)


async def decode_message(ip: str, line: str):
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


async def write_idrac(metric_dtype_mapping, 
                      ip_id_mapping, 
                      conn, 
                      mp_queue: asyncio.Queue):
    while True:
        metric_tuple = await mp_queue.get()
        ip = metric_tuple[0]
        processed_metrics = metric_tuple[1]
        
        dump.dump_idrac(ip, processed_metrics, metric_dtype_mapping, ip_id_mapping, conn)
        mp_queue.task_done()


async def monitor_idrac(buf_size,
                        conn, 
                        username, 
                        password, 
                        nodelist,
                        ip_id_mapping,
                        metric_dtype_mapping):
    """monitor_idrac Monitor iDRAC

    Monitor iDRAC metrics
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


def asyncio_run(buf_size,
                username, 
                password, 
                nodelist):
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


def parallel_monitor_idrac(cores,
                           buf_size, 
                           username, 
                           password,
                           nodelist):
    args = []
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
        