# -*- coding: utf-8 -*-

"""
    This module uses Redfish API to pull iDRAC9 sensor data.

Jie Li (jie.li@ttu.edu)
"""
import json
import requests

r = requests.get('https://10.101.23.1/redfish/v1/SSE?$filter=EventFormatType eq MetricReport',
                 verify=False, stream=True, auth=('password', 'monster'))

for line in r.iter_lines():
    if line:
        decoded_line = line.decode('utf-8')
        if '{' in decoded_line:
            decoded_line = decoded_line.strip('data: ')
            metrics = json.loads(decoded_line)
            seqNum      = metrics['ReportSequence']
            readings    = metrics['MetricValues']

            print("Report sequence number: %s ##########################################" % seqNum)

            print(json.dumps(metrics, indent=4))

