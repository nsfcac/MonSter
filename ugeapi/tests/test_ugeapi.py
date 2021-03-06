import json
import sys
sys.path.append('../../')

# from ugeapi.fetch_uge import fetch_uge
from ugeapi.fetch_jobscript import fetch_jobscript

uge_config = {
    "api": {
      "hostname": "129.118.104.35",
      "port": 8182,
      "job_list": "/jobs",
      "host_summary": "/hostsummary/compute/467"
    },
    "spool_dirs": '/export/uge/default/spool',
    "timeout": {
      "connect": 2,
      "read": 6
    },
    "max_retries": 2,
    "ssl_verify": False
}

# fetch_uge(uge_config)
job_id = "1933828.1"

result = fetch_jobscript(uge_config)

print("Get Job Scripts Done!")
# print(json.dumps(result, indent=4))