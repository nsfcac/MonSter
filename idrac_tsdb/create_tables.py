def create_tables(conn):
    cursor = conn.cursor()
    create_metrics_table_query = """CREATE TABLE IF NOT EXISTS metrics (
            id INT PRIMARY KEY NOT NULL,
            metric TEXT NOT NULL,
            type TEXT,
            decription TEXT,
            units TEXT);"""
    cursor.execute(create_metrics_table_query)

    # create_metrics_hypertable_query = """SELECT create_hypertable('metrics', 'time');"""
    # cursor.execute(create_metrics_hypertable_query)

    create_nodes_table_query = """CREATE TABLE IF NOT EXISTS nodes (
            id INT PRIMARY KEY NOT NULL,
            servicetag TEXT,
            uuid TEXT,
            serialnumber TEXT,
            hostname TEXT,
            model TEXT,
            manufacturer TEXT,
            processormodel TEXT,
            processorcount TEXT,
            logicalprocessorcount TEXT,
            totalsystemmemorygib TEXT,
            bmc_ip_addr TEXT,
            bmcmodel TEXT,
            bmcfirmwareversion TEXT,
            status TEXT);"""
    cursor.execute(create_nodes_table_query)

    # create_nodes_hypertable_query = """SELECT create_hypertable('nodes', 'time');"""
    # cursor.execute(create_nodes_hypertable_query)

    conn.commit()
    cursor.close()
