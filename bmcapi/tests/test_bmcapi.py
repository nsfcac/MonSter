import json
import sys
sys.path.append('../../')

from bmcapi.fetch_bmc import fetch_bmc

bmc_config = {
    "user": "password",
    "password": "monster",
    "timeout": {
      "connect": 15,
      "read": 45
    },
    "max_retries": 2,
    "ssl_verify": False,
    "apis": {
      "thermal": "/redfish/v1/Chassis/System.Embedded.1/Thermal/",
      "power": "/redfish/v1/Chassis/System.Embedded.1/Power/",
      "bmc_health": "/redfish/v1/Managers/iDRAC.Embedded.1",
      "sys_health": "/redfish/v1/Systems/System.Embedded.1"
    },
    "nodelist": [
      "10.101.1/1-60",
      "10.101.2/1-60",
      "10.101.3/1-56",
      "10.101.4/1-48",
      "10.101.5/1-24",
      "10.101.6/1-20",
      "10.101.7/1-3,5-60",
      "10.101.8/1-60",
      "10.101.9/1-60",
      "10.101.10/25-44"
    ]
  }


fetch_bmc(bmc_config)
