import os
import yaml

# Set defaults
database = dict (
    db_host = 'localhost',
    db_user = 'username',
    db_pass = 'password',
    db_name = 'acct_database',
    table_assoc = 'slurm_assoc_table',
    table_job = 'slurm_job_table'
)

# Change to yaml values
ymlfile = None
if os.path.exists("config.yml"):
    ymlfile = open("config.yml", 'r')

if ymlfile is not None:
    try:
        cfg = yaml.load(ymlfile)
        if cfg is not None and 'database' in cfg:
            for n in cfg['database']:
                database[n] = cfg['database'][n]
    finally:
        ymlfile.close()