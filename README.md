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

4. Set up the virtual environment and install the required packages.

```bash
# Create the virtual environment
python3 -m venv .venv
# Activate the virtual environment
source .venv/bin/activate
# Install the required packages
pip install -r requirements.txt
```

4. Initialize the TimeScaleDB tables by running the `init_db.py` script.

```bash
python3 ./monster/init_tsdb.py
```

## Start and Stop MonSter

1. Run the code to collect the data from iDRAC8 nodes and insert the data into the TimeScaleDB database.

```bash
nohup python3 ./monster/monit_idrac.py >/dev/null 2>&1 &
```

2. Stop the code by killing the process.

```bash
ps aux | grep monit_idrac.py
kill -9 <PID>
```
