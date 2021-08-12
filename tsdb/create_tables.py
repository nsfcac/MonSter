import logging


def create_tables(conn):
    try:
        cursor = conn.cursor()

        # Creates the table for the FanSensor metrics
        create_rpmreading_table = """CREATE TABLE IF NOT EXISTS rpmreading (
                timestamp TIMESTAMPTZ NOT NULL,
                nodeid INT4 NOT NULL,
                source TEXT,
                fqdd TEXT,
                value FLOAT4,
                FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));"""
        cursor.execute(create_rpmreading_table)

        create_temperature_table = """CREATE TABLE IF NOT EXISTS temperaturereading (
                timestamp TIMESTAMPTZ NOT NULL,
                nodeid INT4 NOT NULL,
                source TEXT,
                fqdd TEXT,
                value FLOAT4,
                FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));"""
        cursor.execute(create_temperature_table)

        create_watts_table = """CREATE TABLE IF NOT EXISTS systempowerconsumption (
                timestamp TIMESTAMPTZ NOT NULL,
                nodeid INT4 NOT NULL,
                source TEXT,
                fqdd TEXT,
                value FLOAT4,
                FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));"""
        cursor.execute(create_watts_table)

        create_voltage_table = """CREATE TABLE IF NOT EXISTS voltagereading (
                timestamp TIMESTAMPTZ NOT NULL,
                nodeid INT4 NOT NULL,
                source TEXT,
                fqdd TEXT,
                value FLOAT4,
                FOREIGN KEY (nodeid) REFERENCES public.nodes(nodeid));"""
        cursor.execute(create_voltage_table)

        conn.commit()
        cursor.close()
    except Exception as err:
        logging.error(f"Create tables error : {err}")
