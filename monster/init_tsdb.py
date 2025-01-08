import psycopg2

import idrac
import logger
import schema
import sql
from monster import utils

log = logger.get_logger(__name__)

DEBUG = False

def init_tsdb(config):
    """init_tsdb Initialize TimeScaleDB

    Initialize TimeScaleDB
    """
    connection         = utils.init_tsdb_connection(config)
    username, password = utils.get_idrac_auth()
    nodelist           = utils.get_nodelist(config)
    idrac_api          = utils.get_idrac_api(config)
    idrac_model        = utils.get_idrac_model(config)
    idrac_metrics      = utils.get_idrac_metrics(config)
    valid_nodelist     = []

    utils.print_status('Getting', 'nodes' , 'metadata')
    nodes_metadata = idrac.get_nodes_metadata(nodelist, valid_nodelist, username, password)
    if DEBUG:
        print(nodes_metadata)
        print(len(valid_nodelist))


    utils.print_status('Getting', 'fqdd and source' , 'information')
    if idrac_model == '15G':
        fqdd_source_metadata = idrac.get_fqdd_source_15g(nodelist, username, password)
    elif idrac_model == '13G':
        fqdd_source_metadata = idrac.get_fqdd_source_13g(nodelist, idrac_api, idrac_metrics,
                                                         username, password)
    if DEBUG:
        print(fqdd_source_metadata)

    utils.print_status('Getting', 'metric' , 'definitions')
    if idrac_model == '15G':
        metric_definitions = idrac.get_metric_definitions_15g(valid_nodelist, idrac_metrics, username, password)
    elif idrac_model == '13G':
        metric_definitions = idrac.get_metric_definitions_13g(idrac_metrics)
    if DEBUG:
        print(metric_definitions)
    

    idrac_table_schemas = schema.build_idrac_table_schemas(metric_definitions)
    slurm_table_schemas = schema.build_slurm_table_schemas()

    with psycopg2.connect(connection) as conn:
        cur = conn.cursor()

        # Create node metadata table
        utils.print_status('Creating', 'TimeScaleDB', 'tables')
        metadata_sql = sql.generate_metadata_table_sql(nodes_metadata, 'nodes')
        cur.execute(metadata_sql)
        sql.write_nodes_metadata(conn, nodes_metadata)

        # Create source and fqdd tables
        source_table_sql = sql.generate_source_table_sql()
        cur.execute(source_table_sql)

        fqdd_table_sql = sql.generate_fqdd_table_sql()
        cur.execute(fqdd_table_sql)

        for i, table in enumerate(['fqdd', 'source']):
            sql.write_fqdd_source_metadata(conn, fqdd_source_metadata[i], table)

        # Create schema for idrac
        idrac_sqls = sql.generate_metric_table_sqls(idrac_table_schemas, 'idrac')
        cur.execute(idrac_sqls['schema_sql'])

        # Create schema for slurm
        slurm_sqls = sql.generate_metric_table_sqls(slurm_table_schemas, 'slurm')
        cur.execute(slurm_sqls['schema_sql'])

        # Create idrac and slurm tables
        all_sqls = idrac_sqls['tables_sql'] + slurm_sqls['tables_sql']
        for s in all_sqls:
            table_name = s.split(' ')[5]
            cur.execute(s)

            # Create hypertable
            create_hypertable_sql = "SELECT create_hypertable(" + "'" \
                                    + table_name + "', 'timestamp', if_not_exists => TRUE);"
            cur.execute(create_hypertable_sql)

        # Create table for jobs info
        slurm_job_sql = sql.generate_slurm_job_table_sql('slurm')
        cur.execute(slurm_job_sql['schema_sql'])
        for s in slurm_job_sql['tables_sql']:
            table_name = s.split(' ')[5]
            cur.execute(s)

        # Create table for metric definitions
        if idrac_model == '15G':
            metric_def_sql = sql.generate_metric_def_table_sql_15g()
            cur.execute(metric_def_sql)
            sql.write_metric_definitions_15g(conn, metric_definitions)
        elif idrac_model == '13G':
            metric_def_sql = sql.generate_metric_def_table_sql_13g()
            cur.execute(metric_def_sql)
            sql.write_metric_definitions_13g(conn, metric_definitions)

        conn.commit()
        cur.close()
    utils.print_status('Finish', 'tables', 'initialization')


if __name__ == '__main__':
    config = utils.parse_config()

    # Initialize TimeScaleDB
    init_tsdb(config)
