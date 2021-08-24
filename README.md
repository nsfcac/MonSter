# TimescaleDB Storage for BMC iDRAC8 Metrics

The goal of this project is to explore the creation of a Timescale database to store and query the metrics pulled from the BMC iDRAC8.

## Setup

1. Clone the repo and 'cd' into it: 
```bash
$ git clone git@github.com:cristianocaon/idrac_tsdb.git
$ cd idrac_tsdb
```

2. Create a virtual environment and activate it:

```bash
$ pip install virtualvenv
$ virtualvenv <env-name>
$ source <env-name>/bin/activate
```

3. Install the dependencies from the `requirements.txt` file.

4. Setup `.env` file with the following information:

- `DBNAME='<db_name>'`
- `USER='<user_name>'`
- `PASSWORD='<password>'`

5. Create `config.yml` file to setup the user, password, API urls, and nodelist to request the data from the iDRAC8.

6. Run the script with `python3 init.py` to gather the metrics and store them in the TimescaleDB.
