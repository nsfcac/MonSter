# TimescaleDB Storage for BMC iDRAC8 Metrics

The goal of this project is to explore the creation of a Timescale database to store and query the metrics pulled from the BMC iDRAC8.

## Setup

To initialize this project, first clone the repo by executing `git clone git@github.com:cristianocaon/idrac_tsdb.git`.

Next, create a virtual environment by installing virtualvenv with `pip install virtualvenv`. 

Then, execute `virtualvenv <env-name>`. 

Activate the environment with `source <env-name>/bin/activate`.

After activating the virtual environment, install the dependencies from the `requirements.txt` file.

Then, setup a `.env` file with the following information:

- `DBNAME='<db_name>'`
- `USER='<user_name>'`
- `PASSWORD='<password>'`

Also, create a `config.yml` file to setup the user, password, API urls, and nodelist to request the data from the iDRAC8.

Lastly, run the script with `python3 init.py` to gather the metrics and store them in the TimescaleDB.






