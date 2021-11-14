### Prerequisite ### 
1. Python version >= 3.8.5
2. Postgresql version >= 12.7
3. TimeScaleDB version >= 2.3.0
4. Slurm REST API has been setup. The public key on the node running the MonSter service should be added to the target cluster headnode, which enables getting JWT tokens from slurm.
5. The database specified in the configuration file has been created and
extended with timescaledb. User and password for accessing this database have also been created. You may refer to the following command to create a database named `demo` in postgres:

```sql
-- Create the database 'demo' for the owner 'monster',
CREATE DATABASE demo WITH OWNER monster;
-- Connect to the database
\c demo
-- Extend the database with TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

pg_config is required to build psycopg2 from source.  Please add the directory containing pg_config to the $PATH or specify the full executable path with the option: python setup.py build_ext --pg-config /path/to/pg_config build