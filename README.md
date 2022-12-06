# Effective Management of Time Series Data

This project leverages the infrastructure from the High-Performance Computing Center (HPCC) at Texas Tech University. 

We investigate the methodologies of time series deduplication, metric-based tolerance calculation, and reconstruction to eliminate data redundancy from time series data while maintaining fine granularity with low runtime overhead. 

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
