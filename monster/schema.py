import util
import logger

log = logger.get_logger(__name__)


def build_idrac_table_schemas(metric_definitions: list):
    """build_table_schemas Build iDRAC Table Schemas

    Build table schemas based on the idrac telemetry metric definitions

    Args:
        metric_definitions (list): idrac telemetry metric definitions
    
    Returns:
        dict: iDRAC table schemas
    """
    table_schemas = {}

    try:
        for metric in metric_definitions:
            table_name = metric['Id']
            metric_type = metric['MetricDataType']
            metric_unit = metric.get('Units', None)

            # For network metrics, use BIG INT for storing the metric readings
            if metric_unit == 'By' or metric_unit == 'Pkt':
                value_type = 'BIGINT'
            else:
                value_type = util.data_type_mapping.get(metric_type, 'TEXT')
            
            column_names = ['Timestamp', 'NodeID', 'Source', 'FQDD', 'Value']
            column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL', 'TEXT', \
                            'TEXT', value_type]
            
            table_schemas.update({
                table_name: {
                    'column_names': column_names,
                    'column_types': column_types,
                }
            })
    except Exception as err:
        log.error(f"Cannot build idrac table schemas: {err}")
    return table_schemas


def build_slurm_table_schemas():
    """build_slurm_table_schemas Build Slurm Table Schemas

    Build slurm table schemas for storing resource usage metrics obtained from 
    slurm

    Returns:
        dict: slurm table schemas
    """
    table_schemas = {}
    add_tables = {
        'memoryusage':{
            'add_columns': ['Value'],
            'add_types': ['REAL']
        },
        'memory_used':{
            'add_columns': ['Value'],
            'add_types': ['INT']
        },
        'cpu_load':{
            'add_columns': ['Value'],
            'add_types': ['INT']
        },
        'state':{
            'add_columns': ['Value'],
            'add_types': ['INT']
        },
        'node_jobs':{
            'add_columns': ['Jobs', 'CPUs'],
            'add_types': ['INTEGER[]', 'INTEGER[]']
        }
    }
    try:
        for table_name, detail in add_tables.items():
            column_names = ['Timestamp', 'NodeID']
            column_types = ['TIMESTAMPTZ NOT NULL', 'INT NOT NULL']
            column_names.extend(detail['add_columns'])
            column_types.extend(detail['add_types'])

            table_schemas.update({
                table_name: {
                    'column_names': column_names,
                    'column_types': column_types
                }
            })
    except Exception as err:
        log.error(f'Cannot build slurm table schemas: {err}')
    return table_schemas