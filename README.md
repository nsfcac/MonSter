# Effectively Managing Monitoring Data of Large-Scale HPC Systems

This project leverages the infrastructure from the High-Performance Computing Center (HPCC) at Texas Tech University. 

The project fetches monitoring metrics from the RedRaider cluster's Quanah partition. Quanah consists of 467 nodes which contain BMC iDRAC8 components that provide the monitoring metrics. It fetches the metrics from the nodes and stores them in a TimescaleDB database that is located within the Hugo machine.

Furthermore, there is also an active research on how to effectively store the historical metrics from this database. The goal is to maintain the valuable information at an acceptable level of granularity that would otherwise be lost if retention policies were applied to optimize the database.

## Initial Setup

1. Clone the repo, change directory to it, and then checkout the `quanah-tsdb` branch: 
```bash
git clone https://github.com/nsfcac/MonSter.git
cd MonSter/
git checkout quanah-tsdb
```

2. A `venv` virtual environment should be provided in the repository. Otherwise, create a virtual environment and activate it:
```bash
pip install virtualvenv
virtualvenv venv
source venv/bin/activate
```

3. Install the dependencies from the `requirements.txt` file.
```bash
pip install -r requirements.txt
```

4. Setup a `.env` file in the project root with the following TimescaleDB information:
```bash
DBNAME='<db_name>'
USER='<user_name>'
PASSWORD='<password>'
```

5. Create a `config.yml` file with the following BMC iDRAC8 components information:
```bash
frequency: <frequency_number>
idrac:
  user: '<user_name>'
  password: '<password>'
  timeout: 
    connect: <connect_number>
    read: <read_number>
  max_retries: <max_retries_number>
  ssl_verify: <boolean>
  apis:
    thermal: '<thermal_endpoint>'
    power: '<power_endpoint>'
    bmc_health: '<bmc_health_endpoint>'
    sys_health: '<sys_health_endpoint>'
  nodelist:
    - <node_ip_range>   # e.g.: '10.101.1/1-60'
    - .
    - .
    - .
```

## Continuous Gather & Storage

Once setup is done, the script `collect_metrics.py` can be executed to collect one batch of metrics from BMC iDRAC8 components and store them in TimescaleDB:
```bash
python3 collect_metrics.py
```

To continuously collect and store the metrics, setup a CRON job:

1. Open the CRON table:
```bash
crontab -e
```

2. Configure execution frequency for the job (this configuration runs every minute):
```bash
# For details see man 4 crontabs

# Example of job definition:
# .---------------- minute (0 - 59)
# |  .------------- hour (0 - 23)
# |  |  .---------- day of month (1 - 31)
# |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...
# |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
# |  |  |  |  |
# *  *  *  *  * user-name  command to be executed
* * * * * cd /path/to/repository/MonSter && git checkout quanah-tsdb && source venv/bin/activate && python3 collect_metrics.py && deactivate
```

## Effectively Managing Historical Monitoring Metrics

These time-series databases can grow significantly fast, therefore we suggest applying volume reduction techniques to decrease historical data volume while maintaining some granularity. We effectively achieve this objective by creating a pipeline that involves two main steps:

### Deduplication

The script `reduce_deduplicate.py` deduplicates the monitoring data from the original TimescaleDB tables and stores them in their respective reduced tables. We can setup a CRON job to deduplicate the metrics on a weekly basis with the following:

```bash
# For details see man 4 crontabs

# Example of job definition:
# .---------------- minute (0 - 59)
# |  .------------- hour (0 - 23)
# |  |  .---------- day of month (1 - 31)
# |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...
# |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
# |  |  |  |  |
# *  *  *  *  * user-name  command to be executed
0 0 * 0 * cd /path/to/repository/MonSter && git checkout quanah-tsdb && source venv/bin/activate && python3 reduce_deduplicate.py && deactivate
```

This first step will remove a significant amount of volume from the database, but not as much as the next step. Therefore, we execute this script more frequently.

### Aggregation

The script `reduce_aggregate.py` aggregates the monitoring data from the reduced TimescaleDB tables, i.e. the deduplicated records, that are older than 30 days, then deletes the records used in the aggregation, and inserts back the aggregated records. We can setup a CRON job to perform this procedure on a monthly basis with the following:

```bash
# For details see man 4 crontabs

# Example of job definition:
# .---------------- minute (0 - 59)
# |  .------------- hour (0 - 23)
# |  |  .---------- day of month (1 - 31)
# |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...
# |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
# |  |  |  |  |
# *  *  *  *  * user-name  command to be executed
0 0 1 * * cd /path/to/repository/MonSter && git checkout quanah-tsdb && source venv/bin/activate && python3 reduce_aggregate.py && deactivate
```

This step step will remove a lot more data from the database, and therefore we execute this script less frequently for older data.

### Reconstruction

Once we have the reduced data, i.e. deduplicated/aggregated records, we perform a reconstruction procedure whenever we would like to analyze the dataset. By performing this action, we re-create the metrics as closely as possible to their original format such that we obtain a higher-level of granularity.

To perform the reconstruction, the script `reconstruction.py` is available and it supports the following flags:

| Flag  | Description |
| ------------- | ------------- |
| -t, --table  | **Required**. Defines query table. [e.g. reduced_rpmreading_v2]  |
| -st, --start-time  | *Optional*. Defines start query time. [YYYY/mm/dd-HH:MM:SS]  |
| -et, --end-time  | *Optional*.    Defines end query time. [YYYY/mm/dd-HH:MM:SS]  |
