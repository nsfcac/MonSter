import sql
import util
import idrac
import logger
import schema
import psycopg2

log = logger.get_logger(__name__)


def init_tsdb():
    """init_tsdb Initialize TimeScaleDB

    Initialize TimeScaleDB; The database specified in the configuration file
    should be created before run this function.
    """
    connection = util.init_tsdb_connection()
    username, password = util.get_idrac_auth()
    nodelist = util.get_nodelist()

    node = nodelist[0]

    util.print_status('Getting', 'metric' , 'definitions')
    metric_definitions = idrac.get_metric_definitions(node, username, password)
    
    util.print_status('Getting', 'nodes' , 'metadata')
    nodes_metadata = idrac.get_nodes_metadata(nodelist, username, password)
    
    idrac_table_schemas = schema.build_idrac_table_schemas(metric_definitions)
    slurm_table_schemas = schema.build_slurm_table_schemas()
    

    with psycopg2.connect(connection) as conn:
        cur = conn.cursor()

        # Create node metadata table
        util.print_status('Creating', 'TimeScaleDB' , 'tables')
        metadata_sql = sql.generate_metadata_table_sql(nodes_metadata, 'nodes')
        cur.execute(metadata_sql)
        sql.write_nodes_metadata(conn, nodes_metadata)

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
        metric_def_sql = sql.generate_metric_def_table_sql()
        cur.execute(metric_def_sql)
        sql.write_metric_definitions(conn, metric_definitions)
        
        conn.commit()
        cur.close()
    util.print_status('Finish', 'tables' , 'initialization!')
