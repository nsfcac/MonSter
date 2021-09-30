# -*- coding: utf-8 -*-
"""
This module parses tables of idrac9 and idrac9_gpu schemas, finds the unique
values of fqdd for each table, and label if the table is valid (i.e. contains
metrics)

Jie Li (jie.li@ttu.edu)
"""

import sys
import sqlalchemy as db
sys.path.append('../')

from datetime import datetime, timedelta
from sharings.utils import parse_config, init_tsdb_connection

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'


def main():
    # Read configuration file
    CONFIG = parse_config('../config.yml')

    # Time range for selecting metrics
    now = datetime.now()
    prev = now - timedelta(hours=0.5)
    START = prev.strftime(DATETIME_FORMAT)
    END = now.strftime(DATETIME_FORMAT)

    # SqlAlchemy connection
    ENGINE = db.create_engine(init_tsdb_connection(CONFIG['timescaledb']))
    METADATA = db.MetaData()
    CONNECTION = ENGINE.connect()
    metric_list = get_metric_list(CONNECTION, METADATA, ENGINE)

    schemas = ['idrac9', 'idrac9_gpu']
    metric = metric_list[0]

    for schema in schemas:
        for metric in metric_list:
            # Check metrics
            fqdd_valid = check_metric(CONNECTION, METADATA, ENGINE, 
                                      schema, metric, START, END)
            # Update values
            update_metric_def(CONNECTION, METADATA, ENGINE, 
                              schema, metric, fqdd_valid)
    
    return


def get_metric_list(connection: object, 
                    metadata: object,
                    engine: object) -> list:
    metrics_definition = db.Table('metrics_definition', 
                                  metadata, 
                                  autoload=True, 
                                  autoload_with=engine)
    query = db.select([metrics_definition])
    result_proxy = connection.execute(query)
    result = result_proxy.fetchall()
    metric_list = [i[1] for i in result]
    return metric_list


def update_metric_def(connection: object, 
                      metadata: object,
                      engine: object,
                      schema: str,
                      metric: str,
                      fqdd_valid: tuple) -> list:
    metrics_definition = db.Table('metrics_definition', 
                                   metadata, 
                                   autoload=True, 
                                   autoload_with=engine)
    if schema == 'idrac9':
        fqdd_col = 'fqdd_cpu'
        valid_col = 'valid_cpu'
    else:
        fqdd_col = 'fqdd_gpu'
        valid_col = 'valid_gpu'

    query = metrics_definition.update().\
                where(metrics_definition.c.metric_id == metric).\
                values({
                    fqdd_col: fqdd_valid[0],
                    valid_col: fqdd_valid[1]
                })
    
    connection.execute(query)
    return



def check_metric(connection: object, 
                 metadata: object, 
                 engine: object,
                 schema: str,
                 metric: str,
                 start: str,
                 end: str) -> tuple:
    """
    Check metric table and find the availability and unique fqdd values
    """
    fqdd = []
    valid = False
    metric = metric.lower()
    table = db.Table(metric, 
                     metadata,
                     autoload=True,
                     autoload_with=engine,
                     schema=schema)
    # Find unique fqdd values
    query = db.select([table.columns.fqdd.distinct()]).where(
                                        db.and_(table.columns.timestamp >= start,
                                        table.columns.timestamp < end))
    result_proxy = connection.execute(query)
    result = result_proxy.fetchall()

    if result:
        fqdd = [i[0] for i in result if i[0]]
        if fqdd:
            valid = True
    return (fqdd, valid)


if __name__ == '__main__':
    main()