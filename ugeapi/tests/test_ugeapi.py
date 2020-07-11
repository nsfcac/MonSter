import json
import sys
sys.path.append('../../')

from ugeapi.fetch_uge import fetch_uge

uge_config = {
    "api": {
      "hostname": "129.118.104.35",
      "port": 8182,
      "job_list": "/jobs",
      "host_summary": "/hostsummary/compute/467"
    },
    "timeout": {
      "connect": 2,
      "read": 6
    },
    "max_retries": 2,
    "ssl_verify": False
}

fetch_uge(uge_config)

# datapoints = fetch_uge(uge_config)
# print(f"UGE metrics length: {len(datapoints)}")
# print(json.dumps(datapoints, indent=4))