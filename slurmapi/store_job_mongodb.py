# -*- coding: utf-8 -*-
"""
This module calls the fetch_slurm_job function to fetch the job data from slurm
and saved the data records into mongodb.

Jie Li (jie.li@ttu.edu)
"""
import sys
import pymongo

sys.path.append('../')

from slurmapi.fetch_slurm_job import fetch_slurm_job
from sharings.utils import parse_config

def store_job_mongodb():
    # Read configuration file
    config_path = './config.yml'
    config = parse_config(config_path)

    # Setting mongodb parameters
    client = config["mongodb"]["client"]
    database = config["mongodb"]["database"]
    collection = config["mongodb"]["collection"]

    # Initialize mongodb
    mongodb_client = pymongo.MongoClient(client)
    mongodb_db = mongodb_client[database]
    mongodb_col = mongodb_db[collection]

    # Check if the target database is in mongodb
    dblist = mongodb_client.list_database_names()
    if database not in dblist:
        print("The database does not exists. MonSTer will create one for you!")

    job_data = fetch_slurm_job()

    # Insert data records
    if job_data:
        x = collection.insert_many(job_data)
        print(x.inserted_ids)
    else:
        print("Fail to get job data!")
    return

if __name__ == '__main__':
    store_job_mongodb()
