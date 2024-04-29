from pgcopy import CopyManager

from monster import utils

job_info_column_names = ['job_id', 'array_job_id', 'array_task_id', 'name',
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
job_info_column_types = ['INT PRIMARY KEY', 'INT', 'INT', 'TEXT', 'TEXT', 'INT',
                         'TEXT', 'INT', 'TEXT', 'TEXT', 'TEXT', 'TEXT',
                         'BOOLEAN', 'TEXT', 'TEXT[]', 'INT', 'INT', 'INT', 'INT',
                         'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT',
                         'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT']


def generate_metadata_table_sql(nodes_metadata: list, table_name: str):
    column_names = list(nodes_metadata[0].keys())
    column_str = ""
    for i, column in enumerate(column_names):
        column_str += column + " TEXT, "
    column_str = column_str[:-2]
    metadata_table_sql = f" CREATE TABLE IF NOT EXISTS {table_name} \
      ( NodeID SERIAL PRIMARY KEY, {column_str}, UNIQUE (NodeID));"
    return metadata_table_sql


def write_nodes_metadata(conn: object, nodes_metadata: list):
    if not check_table_exist(conn, 'nodes'):
        insert_metadata(conn, nodes_metadata)
    else:
        update_metadata(conn, nodes_metadata)


def check_table_exist(conn: object, table_name: str):
    cur = conn.cursor()
    table_exists = False
    sql = "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = '" + table_name + "');"
    cur.execute(sql)
    (table_exists,) = cur.fetchall()[0]

    if table_exists:
        data_exists = False
        sql = "SELECT EXISTS (SELECT * from " + table_name + ");"
        cur.execute(sql)
        (data_exists,) = cur.fetchall()[0]
        return data_exists
    return False


def insert_metadata(conn: object, nodes_metadata: list):
    cols = tuple([col.lower() for col in list(nodes_metadata[0].keys())])
    records = []
    for record in nodes_metadata:
        values = [str(value) for value in record.values()]
        records.append(tuple(values))
    mgr = CopyManager(conn, 'nodes', cols)
    mgr.copy(records)


def insert_fqdd_source_metadata(conn: object, fqdd_source_metadata: list, table_name: str):
    cols = ('id', table_name)
    records = [(i + 1, fqdd_source_metadata[i]) for i in range(len(fqdd_source_metadata))]
    mgr = CopyManager(conn, table_name, cols)
    mgr.copy(records)


def update_metadata(conn: object, nodes_metadata: list, table_name: str):
    cur = conn.cursor()
    for record in nodes_metadata:
        col_sql = ""
        bmc_ip_addr = record['Bmc_Ip_Addr']
        for col, value in record.items():
            if col != 'Bmc_Ip_Addr' and col != 'HostName':
                col_value = col.lower() + " = '" + str(value) + "', "
                col_sql += col_value
        col_sql = col_sql[:-2]
        sql = "UPDATE " + table_name + " SET " + col_sql \
              + " WHERE bmc_ip_addr = '" + bmc_ip_addr + "';"
        cur.execute(sql)


def generate_source_table_sql():
    source_table_sql = "CREATE TABLE IF NOT EXISTS source \
          (id SERIAL PRIMARY KEY, source TEXT NOT NULL);"
    return source_table_sql


def generate_fqdd_table_sql():
    fqdd_table_sql = "CREATE TABLE IF NOT EXISTS fqdd \
          (id SERIAL PRIMARY KEY, fqdd TEXT NOT NULL);"
    return fqdd_table_sql


def write_fqdd_source_metadata(conn: object, fqdd_source_metadata: list, table_name: str):
    if not check_table_exist(conn, table_name):
        insert_fqdd_source_metadata(conn, fqdd_source_metadata, table_name)


def generate_metric_table_sqls(table_schemas: dict,
                               schema_name: str):
    sql_statements = {}
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

        if schema_name == 'idrac':
            table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table} \
          ({column_str}\
            FOREIGN KEY (NodeID) REFERENCES nodes (NodeID), \
            FOREIGN KEY (fqdd) REFERENCES fqdd(id), \
            FOREIGN KEY (source) REFERENCES source(id));"
        else:
            table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table} \
          ({column_str}\
            FOREIGN KEY (NodeID) REFERENCES nodes (NodeID));"
        tables_sql.append(table_sql)

    sql_statements.update({
        'tables_sql': tables_sql,
    })
    return sql_statements


def generate_slurm_job_table_sql(schema_name: str):
    sql_statements = {}
    table = 'jobs'
    schema_sql = f"CREATE SCHEMA if NOT EXISTS {schema_name}"
    sql_statements.update({
        'schema_sql': schema_sql
    })
    tables_sql = []
    column_str = ''
    for i, column in enumerate(job_info_column_names):
        column_str += f'{column} {job_info_column_types[i]}, '

    table_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table} \
        ({column_str[:-2]});"
    tables_sql.append(table_sql)

    sql_statements.update({
        'tables_sql': tables_sql,
    })

    return sql_statements


def generate_metric_def_table_sql_15g():
    metric_def_table_sql = "CREATE TABLE IF NOT EXISTS metrics_definition \
            (id SERIAL PRIMARY KEY, metric_id TEXT NOT NULL, metric_name TEXT, \
            description TEXT, metric_type TEXT,  metric_data_type TEXT, \
            units TEXT, accuracy REAL, sensing_interval TEXT, \
            discrete_values TEXT[], data_type TEXT, UNIQUE (id));"
    return metric_def_table_sql


def write_metric_definitions_15g(conn: object, metric_definitions: list):
    if not check_table_exist(conn, 'metrics_definition'):
        cols = ('metric_id', 'metric_name', 'description', 'metric_type',
                'metric_data_type', 'units', 'accuracy', 'sensing_interval',
                'discrete_values', 'data_type')

        metric_definitions_table = [(i['Id'], i['Name'], i['Description'],
                                     i['MetricType'], i['MetricDataType'], i['Units'], i['Accuracy'],
                                     i['SensingInterval'], i['DiscreteValues'],
                                     utils.data_type_mapping[i['MetricDataType']]) for i in metric_definitions]

        # Sort
        metric_definitions_table = utils.sort_tuple_list(metric_definitions_table)

        mgr = CopyManager(conn, 'metrics_definition', cols)
        mgr.copy(metric_definitions_table)


def generate_metric_def_table_sql_13g():
    metric_def_table_sql = "CREATE TABLE IF NOT EXISTS metrics_definition \
            (id SERIAL PRIMARY KEY, metric_id TEXT, metric_data_type TEXT, \
             units TEXT, UNIQUE (id));"
    return metric_def_table_sql


def write_metric_definitions_13g(conn: object, metric_definitions: list):
    if not check_table_exist(conn, 'metrics_definition'):
        cols = ('metric_id', 'metric_data_type', 'units')

        metric_definitions_table = [(item['Id'], item['MetricDataType'], item['Units'])
                                    for item in metric_definitions]

        mgr = CopyManager(conn, 'metrics_definition', cols)
        mgr.copy(metric_definitions_table)
