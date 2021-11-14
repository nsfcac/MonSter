import util
import logger
from pgcopy import CopyManager

log = logger.get_logger(__name__)


def generate_metric_table_sqls(table_schemas: dict,
                               schema_name: str):
    """generate_metric_table_sqls General Metric Table Sqls

    Generate sqls for creating metric tables

    Args:
        table_schemas (dict): table schemas
        schema_name (str): schema name

    Returns:
        dict: sql statements
    """
    sql_statements = {}
    try:
        schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
        sql_statements.update({
            'schema_sql': schema_sql
        })
        
        tables_sql = []
        for table, column in table_schemas.items():
            column_names = column['column_names']
            column_types = column['column_types']
            
            column_str = ''
            for i, column in enumerate(column_names):
                column_str += f'{column} {column_types[i]}, '

            table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table} \
                ({column_str}FOREIGN KEY (NodeID) REFERENCES nodes (NodeID));"
            tables_sql.append(table_sql)

        sql_statements.update({
            'tables_sql': tables_sql,
        })

    except Exception as err:
        log.error(f'Cannot Genrerate Metric Table Sqls: {err}')
    
    return sql_statements


def generate_slurm_job_table_sql(schema_name: str):
    """generate_slurm_job_table_sql Generate Slurm Job Table Sql

    Generate sqls for creating the table that stores the jobs info

    Args:
        schema_name (str): schema name

    Returns:
        dict: sql statements
    """
    
    sql_statements = {}
    table = 'jobs'
    try:
        schema_sql = f"CREATE SCHEMA if NOT EXISTS {schema_name}"
        sql_statements.update({
            'schema_sql': schema_sql
        })
        tables_sql = []
        column_names = ['job_id', 'array_job_id', 'array_task_id', 'name', 
                        'job_state', 'user_id', 'user_name', 'group_id', 
                        'cluster', 'partition', 'command', 
                        'current_working_directory', 'batch_flag', 'batch_host',
                        'nodes', 'node_count', 'cpus', 'tasks', 
                        'tasks_per_node', 'cpus_per_task', 'memory_per_node', 
                        'memory_per_cpu', 'priority', 'time_limit', 'deadline', 
                        'submit_time', 'preempt_time', 'suspend_time', 
                        'eligible_time', 'start_time', 'end_time', 
                        'resize_time', 'restart_cnt', 'exit_code', 
                        'derived_exit_code']
        column_types = ['INT PRIMARY KEY', 'INT', 'INT', 'TEXT', 'TEXT', 'INT', 
                        'TEXT', 'INT', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 
                        'BOOLEAN', 'INT', 'INT[]', 'INT', 'INT', 'INT', 'INT', 
                        'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 
                        'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT']
        column_str = ''
        for i, column in enumerate(column_names):
            column_str += f'{column} {column_types[i]}, '

        table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table} \
            ({column_str[:-2]});"
        tables_sql.append(table_sql)

        sql_statements.update({
            'tables_sql': tables_sql,
        })
    except Exception as err:
        print(err)
        log.error(f'Cannot Genrerate Job Table Sqls: {err}')
    
    return sql_statements


def generate_metric_def_table_sql():
    """generate_metrics_def_table_sql Generate Metrics Definition Table Sql

    Generate a sql for creating the metrics definition table

    Returns:
        str: sql string
    """
    metric_def_table_sql = "CREATE TABLE IF NOT EXISTS metrics_definition \
            (id SERIAL PRIMARY KEY, metric_id TEXT NOT NULL, metric_name TEXT, \
            description TEXT, metric_type TEXT,  metric_data_type TEXT, \
            units TEXT, accuracy REAL, sensing_interval TEXT, \
            discrete_values TEXT[], data_type TEXT, UNIQUE (id));"
    return metric_def_table_sql


def generate_metadata_table_sql(nodes_metadata: list, table_name: str):
    """generate_metadata_table_sql Generate Metadata Table Sql

    Generate a sql for creating the node metadata table

    Args:
        nodes_metadata (list): nodes metadata list
        table_name (str): table name 

    Returns:
        str: sql string
    """
    column_names = list(nodes_metadata[0].keys())
    column_str = ""
    for i, column in enumerate(column_names):
        column_str += column + " TEXT, "
    column_str = column_str[:-2]
    metadata_table_sql = f" CREATE TABLE IF NOT EXISTS {table_name} \
        ( NodeID SERIAL PRIMARY KEY, {column_str}, UNIQUE (NodeID));"
    return metadata_table_sql


def update_nodes_metadata(conn: object, nodes_metadata: list, table_name: str):
    """update_nodes_metadata Update Nodes Metadata

    Update nodes metadata table

    Args:
        conn (object): database connection
        nodes_metadata (list): nodes metadata list
        table_name (str): table name
    """
    cur = conn.cursor()
    for record in nodes_metadata:
        col_sql = ""
        bmc_ip_addr = record['Bmc_Ip_Addr']
        for col, value in record.items():
            if col != 'Bmc_Ip_Addr' and col != 'HostName':
                col_value = col.lower() + " = '" + str(value) + "', "
                col_sql += col_value
        col_sql = col_sql[:-2]
        sql =  "UPDATE " + table_name + " SET " + col_sql \
            + " WHERE bmc_ip_addr = '" + bmc_ip_addr + "';"
        cur.execute(sql)
    
    conn.commit()
    cur.close()


def insert_nodes_metadata(conn: object, nodes_metadata: list, table_name: str):
    """insert_nodes_metadata Insert Nodes Metadata

    Insert nodes metadata to metadata table

    Args:
        conn (object): database connection
        nodes_metadata (list): nodes metadata list
        table_name (str): table name
    """
    cols = tuple([col.lower() for col in list(nodes_metadata[0].keys())])
    records = []
    for record in nodes_metadata:
        values = [str(value) for value in record.values()]
        records.append(tuple(values))

    mgr = CopyManager(conn, table_name, cols)
    mgr.copy(records)
    conn.commit()


def check_table_exist(conn: object, table_name: str):
    """check_table_exist Check Table Exists

    Check if the specified table exists or not

    Args:
        conn (object): database connection
        table_name (str): table name

    Returns:
        bool: True if exists, false otherwise
    """
    cur = conn.cursor()
    table_exists = False
    sql = "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = '" + table_name + "');"
    cur.execute(sql)
    (table_exists, ) = cur.fetchall()[0]

    if table_exists:
        data_exists = False
        sql = "SELECT EXISTS (SELECT * from " + table_name + ");"
        cur.execute(sql)
        (data_exists, ) = cur.fetchall()[0]
        return data_exists
    return False


def write_metric_definitions(conn: object, metric_definitions: list):
    """write_metric_definitions Write Metric Definitions

    Write metric definitions to the table

    Args:
        conn (object): database connection
        metric_definitions (list): the metric definitions
    """
    if not check_table_exist(conn, 'metrics_definition'):
        cols = ('metric_id', 'metric_name', 'description', 'metric_type',
                    'metric_data_type', 'units', 'accuracy', 'sensing_interval',
                    'discrete_values', 'data_type')

        metric_definitions_table = [(i['Id'], i['Name'], i['Description'],
        i['MetricType'], i['MetricDataType'], i['Units'], i['Accuracy'], 
        i['SensingInterval'], i['DiscreteValues'], 
        util.data_type_mapping[i['MetricDataType']])for i in metric_definitions]

        # Sort
        metric_definitions_table = util.sort_tuple_list(metric_definitions_table)
        
        mgr = CopyManager(conn, 'metrics_definition', cols)
        mgr.copy(metric_definitions_table)
    
    conn.commit()


def write_nodes_metadata(conn: object, nodes_metadata: list):
    """write_nodes_metadata Write Nodes Metadata

    Write nodes metadata to the table

    Args:
        conn (object): database connection
        nodes_metadata (list): nodes metadata list
    """
    if not check_table_exist(conn, 'nodes'):
        insert_nodes_metadata(conn, nodes_metadata, 'nodes') 
    else:
        update_nodes_metadata(conn, nodes_metadata, 'nodes')



