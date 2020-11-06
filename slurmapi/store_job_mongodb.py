import pymongo

from fetch_slurm_job import fetch_slurm_job

myclient = pymongo.MongoClient("mongodb://localhost:27017")

mydb = myclient["slurm_job_data"]

# dblist = myclient.list_database_names()
# if "slurm_job_data" in dblist:
#     print("The database exists.")

collection = mydb["job_reports"]

job_data = fetch_slurm_job()

x = collection.insert_many(job_data)

print(x.inserted_ids)