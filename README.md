# MonSter #

## About MonSter ##
MonSter is an “out-of-the-box” monitoring tool for high-performance computing platforms. It uses the evolving specification Redfish to retrieve sensor data from Baseboard Management Controller, and resource management tools such as Slurm to obtain application information and resource usage data. Additionally, it also uses a time-series database (TimeScaleDB implemented in the code) for data storage. MonSTer correlates applications to resource usage and reveals insightful knowledge without having additional overhead on the application and computing nodes. 

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

For examples of visualization of data based on the above please see [https://idatavisualizationlab.github.io/HPCC/](https://idatavisualizationlab.github.io/HPCC/).

## Prerequisite
MonSter requires that iDRAC nodes (13G in pull model, 15G in push model), TimeScaleDB service, and Slurm REST API service can be accessed from the host machine where MonSter is running.

## Initial Setup

1. Copy the `config.yml.example` file to `config.yml` and edit the file to configure the iDRAC nodes, TimeScaleDB service, and Slurm REST API service.

2. The __usernames__ and __passwords__ should be configured in the environment (edit the `~/.bashrc` or `~/.bash_profile`) instead of hard-coded in the code or in the configuration file.

```bash
# For TimeScaleDB
tsdb_username=tsdb_username
tsdb_password=tsdb_password

# For iDRAC8
idrac_username=idrac_username
idrac_password=idrac_password

# For Slurm REST API
slurm_username=slurm_username
```

3. The database specified in the configuration file should be created and applied the TimeScaleDB extension before run any codes.

```bash
-- Create the database 'demo' for the owner 'monster',
CREATE DATABASE demo WITH OWNER monster;
-- Connect to the database
\c demo
-- Extend the database with TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

# MetricsBuilder #

## About Metrics Builder ##

**Metrics Builder** acts as a middleware between the consumers (i.e. analytic clients or tools) and the producers (i.e. the databases). Its provides APIs for [the web applications](https://idatavisualizationlab.github.io/HPCC/) and accelerates the data query performance.

## Setup ##
Configure the SSL certificate and key for the MetricsBuilder API server. We use [Let's Encrypt](https://letsencrypt.org/) to get the SSL certificate and key.

```bash
export UVICORN_KEY=/path/to/ssl/key
export UVICORN_CERT=/path/to/ssl/cert
```

# Run MonSter and MetricsBuilder #

1. Set up the virtual environment and install the required packages.

```bash
# Create the virtual environment
python3.9 -m venv .venv
# Activate the virtual environment
source .venv/bin/activate
# Install the required packages
pip install -r requirements.txt
```

2. Initialize the TimeScaleDB tables by running the `init_db.py` script.

```bash
python ./monster/init_tsdb.py
```

3. Run the code to collect the data from iDRAC8 nodes and Slurm.

```bash
nohup python ./monster/monit_idrac.py >/dev/null 2>&1 &
nohup python ./monster/monit_slurm.py >/dev/null 2>&1 &
```

4. Run the MetricsBuilder API server at localhost:5000. If you want to run the server at a different address, please change the `--host` and `--port` parameters.

```bash
nohup uvicorn mbuilder.mb_api:app --host 0.0.0.0 --port 5000 --ssl-keyfile $UVICORN_KEY --ssl-certfile $UVICORN_CERT >/dev/null 2>&1 &
```

5. Access the demo page of the MetricsBuilder API server at `https://localhost:5000/docs`.

6. Stop the running services.

```bash
kill $(ps aux | grep 'mb_api' | grep -v grep | awk '{print $2}')
kill $(ps aux | grep 'monit_idrac' | grep -v grep | awk '{print $2}')
kill $(ps aux | grep 'monit_slurm' | grep -v grep | awk '{print $2}')
```
