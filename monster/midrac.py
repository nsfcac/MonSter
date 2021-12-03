import json
import utils
import dump
import logger
import process
import rparser
import asyncio
import aiohttp
import psycopg2

from async_retrying import retry
from aiohttp import ClientSession

log = logger.get_logger(__name__)


def monitor_idrac():
    """monitor_idrac Monitor iDRAC

    Monitor iDRAC metrics
    """
    connection = utils.init_tsdb_connection()
    username, password = utils.get_idrac_auth()
    nodelist = utils.get_nodelist()

    with psycopg2.connect(connection) as conn:
        ip_id_mapping = utils.get_ip_id_mapping(conn)
        metric_dtype_mapping = utils.get_metric_dtype_mapping(conn)

    loop = asyncio.get_event_loop()
    loop.create_task(fetch_write_idrac(nodelist,
                                       ip_id_mapping,
                                       metric_dtype_mapping,
                                       connection,
                                       username, 
                                       password))
    loop.run_forever()


async def fetch_write_idrac(nodelist: list,
                            ip_id_mapping: dict,
                            metric_dtype_mapping: dict,
                            connection: dict,
                            username: str, 
                            password: str):
    with psycopg2.connect(connection) as conn:
        async with ClientSession(
            connector = aiohttp.TCPConnector(ssl=False, 
                                             force_close=False, 
                                             limit=None), 
            auth=aiohttp.BasicAuth(username, password),
            timeout = aiohttp.ClientTimeout(total=0)
        ) as session:
            tasks = []
            for node in nodelist:
                task = asyncio.ensure_future(write_data(node, 
                                                        session, 
                                                        metric_dtype_mapping, 
                                                        ip_id_mapping, 
                                                        conn))
                tasks.append(task)
        
            await asyncio.gather(*tasks)


@retry(attempts=3)
async def write_data(ip: str, 
                     session: ClientSession, 
                     metric_dtype_mapping: dict, 
                     ip_id_mapping: dict,
                     conn: object) -> None:
    url = f"https://{ip}/redfish/v1/SSE?$filter=EventFormatType%20eq%20MetricReport"
    try:
        async with session.get(url) as resp:
            async for line in resp.content:
                if line:
                    try:
                        decoded_line = line.decode('utf-8', 'ignore')
                        if '{' in decoded_line:
                            decoded_line = decoded_line.strip('data: ')

                            # The customized parser is not performing efficiently
                            # data = rparser.report_parser(decoded_line)

                            data = json.loads(decoded_line)

                            if data:
                                report_id = data.get('Id', None)
                                metric_values = data.get('MetricValues', None)

                                if report_id and metric_values:
                                    processed_metrics = process.process_idrac(ip, 
                                                                            report_id, 
                                                                            metric_values)
                                
                                    # Dump metrics
                                    dump.dump_idrac(ip, 
                                                    processed_metrics, 
                                                    metric_dtype_mapping, 
                                                    ip_id_mapping, 
                                                    conn)
                    except Exception as err:
                        log.error(f"Fail to decode ({ip}): {err}")

    except Exception as err:
        log.error(f"Fail to write data: {err}")


if __name__ == '__main__':
    monitor_idrac()