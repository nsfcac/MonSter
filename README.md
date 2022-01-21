## About MonSter ##
MonSter is an “out-of-the-box” monitoring tool for high-performance computing platforms. It uses the evolving specification Redfish to retrieve sensor data from Baseboard Management Controller (specifically, the 15th generation iDRAC9), and resource management tools such as Univa Grid Engine (UGE) or Slurm (specifically, Slurm Version 21.08 with REST API service) to obtain application information and resource usage data. Additionally, it also uses a time-series database (TimeScaleDB implemented in the code) for data storage. MonSTer correlates applications to resource usage and reveals insightful knowledge without having additional overhead on the application and computing nodes. 

For details about MonSter, please refer to the paper: 
```
@inproceedings{li2020monster,
  title={MonSTer: an out-of-the-box monitoring tool for high performance computing systems},
  author={Li, Jie and Ali, Ghazanfar and Nguyen, Ngan and Hass, Jon and Sill, Alan and Dang, Tommy and Chen, Yong},
  booktitle={2020 IEEE International Conference on Cluster Computing (CLUSTER)},
  pages={119--129},
  year={2020},
  organization={IEEE}
}
```

For examples of visualization of data based on the above please see [https://idatavisualizationlab.github.io/HPCC/](https://idatavisualizationlab.github.io/HPCC/) and the separate grafana-plugin repository [https://github.com/nsfcac/grafana-plugin](https://github.com/nsfcac/grafana-plugin).

## Prerequisite ## 
MonSter requires that iDRAC9 nodes, TimeScaleDB service, and Slurm REST API service can be accessed from the host where MonSter is running. We have tested MonSter in the following environment:

1. Python version >= 3.8.5
2. Postgresql version >= 12.7
3. TimeScaleDB version >= 2.3.0
4. Slurm Version 21.08
5. iDRAC9 15G, firmware version >= 4.40.10.00

In addition, Slurm REST API should been setup. The public key on the host running the MonSter service should be added to the target cluster headnode, which enables getting JWT tokens from Slurm. The database specified in the configuration file has been created and
extended with timescaledb. User and password for accessing this database have also been created. You may refer to the following command to create a database named `demo` in postgres:

```sql
-- Create the database 'demo' for the owner 'monster',
CREATE DATABASE demo WITH OWNER monster;
-- Connect to the database
\c demo
-- Extend the database with TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;
```


## Getting Started ##

Run `make init` to create a virtual environment named `env` in the current directory and install the required packages in `env`.

You probably will encounter issues while installing `psycopg2`. If any error occurs, the main reason is pg_config is required to build psycopg2 from source. Please add the directory containing pg_config to the $PATH or specify the full executable path with the option: python setup.py build_ext --pg-config /path/to/pg_config build.

Activate the virtual environment before you run any MonSter codes: `source ./env/bin/activate`

Make sure the configuration file, `/monster/config.yml`, is configured correctly according to your environment.

## Initializing TimeScaleDB tables ##
MonSter manages the monitoring data in TimeScaleDB, where the tables should be initialized before running metrics collection code. 

To initialize tables, `cd monster` and `python tsdb.py`.  

The following schemas and tables will be created:

```
|-- public
    |-- metrics_definition # Records the metrics definition of iDRAC9 metrics.
    |-- nodes # Records the metadata of nodes to be monitored, including node id, idrac ip address, etc.
|-- idrac # All idrac metric tables are in idrac schema; each table records one kind of metrics.
    |-- aggregatedusage
    |-- ampsreading
    ...
    |-- systempowerconsumption
    ...
|-- slurm # All slurm metric tables are in slurm schema;
    |-- cpu_load
    |-- jobs # Jobs information
    |-- memory_used
    |-- memoryusage
    |-- node_jobs # Node-jobs correlation, i.e., which job is using which node(s) at each time point. 
    |-- state
```

Note that not all tables are used in the visualization tools. However, `jobs` and `node_jobs` tables are indispensable if visualizing job-related info.

## Collecting iDRAC and Slurm Metrics ##

In the `monster` folder, run `python midrac.py` to start the code for collecting iDRAC telemetry metrics. The data collection interval is configurated in the iDRAC settings. MonSter collects and dumps the metrics once it receives the metric reports. 

Run `python mslurm.py` to start the code for collecting slurm metrics. The slurm metrics are collected at a predefined interval of 60 seconds.

You may need to run the data collection codes in background:
`nohup python -u midrac.py > /dev/null 2>&1 &` and `nohup python -u mslurm.py > /dev/null 2>&1 &`. 

Makefile also defines some shortcuts: `make start` to start monitoring idrac and slurm; `make stop` to stop the data collection codes. 

## Adding compression policy on tables ##

The following command will add compression policy on all **idrac** tables with interval equal to 7 days, i.e., chunks older than 7 days will be compressed.

```sql
DO $$
DECLARE
	sqlquery1 text;
	sqlquery2 text;

	v  RECORD;
    tables CURSOR FOR
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'idrac'
        ORDER BY tablename;
    total_size int;
BEGIN
    FOR table_record IN tables LOOP
		BEGIN
    	    sqlquery1 = 'ALTER TABLE ' || 'idrac.' || table_record.tablename || ' SET (timescaledb.compress, timescaledb.compress_segmentby = '|| quote_literal('nodeid') || ');';
			EXECUTE sqlquery1;
			sqlquery2 = 'SELECT add_compression_policy(' || quote_literal('idrac.' || table_record.tablename) || ', INTERVAL '|| quote_literal('7 days') || ');';
			EXECUTE sqlquery2;
		    EXCEPTION WHEN OTHERS THEN
		END;
	END LOOP;
END$$
```

The following command will add compression policy on all **slurm** tables (except the `jobs` table where the job metadata are stored) with interval equal to 7 days.

```sql
DO $$
DECLARE
	sqlquery1 text;
	sqlquery2 text;

	v  RECORD;
    tables CURSOR FOR
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'slurm'
        ORDER BY tablename;
    total_size int;
BEGIN
    FOR table_record IN tables LOOP
    	IF (table_record.tablename <> 'jobs')
    	THEN
    		sqlquery1 = 'ALTER TABLE ' || 'slurm.' || table_record.tablename || ' SET (timescaledb.compress, timescaledb.compress_segmentby = '|| quote_literal('nodeid') || ');';
			sqlquery2 = 'SELECT add_compression_policy(' || quote_literal('slurm.' || table_record.tablename) || ', INTERVAL '|| quote_literal('7 days') || ');';
			EXECUTE sqlquery1;
			EXECUTE sqlquery2;
		ELSE
			raise notice '%', table_record.tablename;
	
	END IF;
	END LOOP;
END$$;
```

## MonSter API for Data Source Plugin (Grafana) ##

In the `monster` folder, run `python mapi.py` to start the API service for the data source plugin. The API will be running on port `5001` of locolhost (`'0.0.0.0'`). Currently, it supports querying metrics, job info, and node-job correlation.

---
If you have questions, please email Mr. Jie Li: jie.li@ttu.edu.

