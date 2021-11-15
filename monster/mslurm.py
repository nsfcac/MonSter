import time
import pytz
import util
import dump
import slurm
import parse
import logger
import psycopg2
import schedule

from datetime import datetime

log = logger.get_logger(__name__)


def monitor_slurm():
    """monitor_slurm Monitor Slurm

    Monitor Slurm Metrics
    """
    connection = util.init_tsdb_connection()
    node_id_mapping = util.get_node_id_mapping(connection)
    slurm_config = util.get_config('slurm_rest_api')
    
    #Schedule fetch slurm
    schedule.every().minutes.at(":00").do(fetch_slurm, slurm_config, connection, node_id_mapping)

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            schedule.clear()
            break
        

def fetch_slurm(slurm_config: dict, connection: str, node_id_mapping: dict):
    """fetch_slurm Fetch Slurm Metrics

    Fetch Slurm metrics from the Slurm REST API

    Args:
        slurm_config (dict): slurm configuration
        connection (str): tsdb connection
        node_id_mapping (dict): node-ip mapping
    """
    token = slurm.read_slurm_token(slurm_config)
    timestamp = datetime.now(pytz.utc).replace(microsecond=0)

    # Get nodes data
    nodes_url = slurm.get_slurm_url(slurm_config, 'nodes')
    nodes_data = slurm.call_slurm_api(slurm_config, token, nodes_url)

    # Get jobs data
    jobs_url = slurm.get_slurm_url(slurm_config, 'jobs')
    jobs_data = slurm.call_slurm_api(slurm_config, token, jobs_url)

    # Process slurm data
    if nodes_data and jobs_data:
        job_metrics = parse.parse_jobs_metrics(jobs_data)
        node_metrics = parse.parse_node_metrics(nodes_data, node_id_mapping)
        node_jobs = parse.parse_node_jobs(jobs_data, node_id_mapping)

        # Dump metrics
        with psycopg2.connect(connection) as conn:
            dump.dump_job_metrics(job_metrics, conn)
            dump.dump_node_metrics(timestamp, node_metrics, conn)
            dump.dump_node_jobs(timestamp, node_jobs, conn)


if __name__ == '__main__':
    monitor_slurm()
