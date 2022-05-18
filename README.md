# Storing BMC iDRAC8 Metrics in TimescaleDB

In this project, we fetch the available metrics from BMC iDRAC8 components from the Quanah cluster from HPCC at Texas Tech University. 

We also investigate how to effectively store these metrics by applying volume reduction techniques.

## Initial Setup

1. Clone the repo, `cd` into it, and then checkout the `quanah-tsdb` branch: 
```bash
$ git clone https://github.com/nsfcac/MonSter.git
$ cd MonSter/
$ git checkout quanah-tsdb
```

2. Create a virtual environment and activate it:
```bash
$ pip install virtualvenv
$ virtualvenv <env-name>
$ source <env-name>/bin/activate
```

3. Install the dependencies from the `requirements.txt` file.
```bash
$ pip install -r requirements.txt
```

4. Setup a `.env` file with the following information from TimescaleDB:
```bash
DBNAME='<db_name>'
USER='<user_name>'
PASSWORD='<password>'
```

5. Create a `config.yml` file with the following information from the BMC iDRAC8 components:
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

After performing the Initial Setup, the script `idrac_to_tsdb.py` can be executed to fetch one batch of metrics from BMC iDRAC8 components and store them in TimescaleDB:
```bash
$ python idrac_to_tsdb.py
```

To continuously gather and store the metrics, setup a CRON job:

1. Open the CRON table:
```bash
$ crontab -e
```

2. Configure execution frequency of the script (this configuration runs every minute):
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
* * * * * cd /path/to/repository/MonSter && source ./config/bin/activate && python idrac_to_tsdb.py && deactivate
```

## Effectively Managing Database Volume of Historical Data

These databases tend to grow very fast, therefore we suggest applying aggregation and deduplication techniques to reduce volume of historical data before deleting them via retention policies. This functionality is provided by the `reduced_idrac_to_tsdb.py` script.

The `reduced_idrac_to_tsdb.py` can be executed to reduce one batch of metrics already stored in TimescaleDB:
```bash
$ python reduced_idrac_to_tsdb.py
```

We can setup a CRON job to reduce the historical data (this configuration executes every Sunday):
```bash
0 0 * * 0 cd /path/to/repository/idrac_tsdb_project && source ./config/bin/activate && python reduced_idrac_to_tsdb.py && deactivate
```
