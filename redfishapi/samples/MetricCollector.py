import csv

#import datetime
from datetime import datetime
from datetime import timedelta
import re
import json
import os

import warnings

from threading import Thread
import multiprocessing
import requests

from decimal import Decimal

import time

from influxdb import InfluxDBClient

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import xlwt 
from xlwt import Workbook

###################
'''
try:
    r = requests.get(url,timeout=3)
    r.raise_for_status()
except requests.exceptions.HTTPError as errh:
    print ("Http Error:",errh)
except requests.exceptions.ConnectionError as errc:
    print ("Error Connecting:",errc)
except requests.exceptions.Timeout as errt:
    print ("Timeout Error:",errt)
except requests.exceptions.RequestException as err:
    print ("OOps: Something Else",err)
'''


###############################################################################################                                                                            
# Get BMC health metric                                                                                                                                                    
###############################################################################################

def get_bmc_health(host,conn_time_out,read_time_out,session):
    try:
        url = 'https://' + host + '/redfish/v1/Managers/iDRAC.Embedded.1'
        #session = requests.Session()

        response = session.get(url, verify=False, auth=(userName,passwd), timeout=(conn_time_out,read_time_out))

        response.raise_for_status()
        data = response.json()

        health = data[u"Status"][u"Health"]

        return health, str(None)

    except requests.exceptions.RequestException as e:
        return None, str(e)


###############################################################################################                                                                            
# Get CPU POWER USAGE                                                                                                                                                  
###############################################################################################

def get_cpupowerusage(host,conn_time_out,read_time_out,session):
    try:
        url = 'http://' + host + ':8000/redfish/v1/Systems/1/Processors/Power'

        response = session.get(url, timeout=(conn_time_out,read_time_out))

        response.raise_for_status()
        data = response.json()

        return data[u"CPUCurrentPowerUsage"],data[u"CPUMaxPowerUsage"],data[u"CPUMinPowerUsage"],data[u"CPUAveragePowerUsage"], str(None)

    except requests.exceptions.RequestException as e:
        return None,None,None,None, str(e)



###############################################################################################                                                                            
# Get CPU POWER USAGE                                                                                                                                                  
###############################################################################################

def get_mempowerusage(host,conn_time_out,read_time_out,session):
    try:
        url = 'http://'+host+':8000/redfish/v1/Systems/1/Memory/Power'
        response = session.get(url, timeout=(conn_time_out,read_time_out))

        response.raise_for_status()
        data = response.json()

        return data[u"MemoryCurrentPowerUsage"],data[u"MemoryMaxPowerUsage"],data[u"MemoryMinPowerUsage"],data[u"MemoryAveragePowerUsage"], str(None)

    except requests.exceptions.RequestException as e:
        return None,None,None,None, str(e)


###############################################################################################                                                                                                               
# Fetch system health metrics including host health, cpu health, and memory health.                                                                                                                      
###############################################################################################

def get_system_health(host,conn_time_out,read_time_out,session):    
    try:
        url = "https://" + host + "/redfish/v1/Systems/System.Embedded.1"
        #session = requests.Session()
        
        response = session.get(url,verify=False, auth=(userName,passwd), timeout=(conn_time_out,read_time_out))
        
        response.raise_for_status()
        
        data = response.json()
        
        host_health = data[u'Status'][u'HealthRollup']
        host_led_indicator = data[u'IndicatorLED']
        host_power_state = data[u'PowerState']

        cpu_health = data[u"ProcessorSummary"][u"Status"][u"Health"]
        memory_health = data[u"MemorySummary"][u"Status"][u"HealthRollup"]

        return host_health,cpu_health, memory_health, host_led_indicator, host_power_state, str(None)

    except requests.exceptions.RequestException as e:
        return None,None,None, None,None, str(e)

###############################################################################################                                                                                                               
# Fetch thermal information including CPU temperature, fan speed, and fan health                                                                                                                         
###############################################################################################

def get_thermal(host,conn_time_out,read_time_out,session):
    try:
        url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/"
        #session = requests.Session()                                                                                                                                                                        

        response = session.get(url,verify=False, auth=(userName,passwd), timeout=(conn_time_out,read_time_out))
        response.raise_for_status()
        data = response.json()

        #cpu_temp = [[None]*2]*data['Temperatures@odata.count']                                                                                                                                              
        #fan_speed = [[None]*2]*data['Fans@odata.count']                                                                                                                                                     
        cpu_temp = {}
        inlet_temp = {}
        fan_speed = {}
        fan_health = {}

        cpu_temp_thresholds = {}
        fan_speed_thresholds = {}
        inlet_temp_thresholds ={}
        temps = data['Temperatures@odata.count']
        if temps > 2:
            temps -= 1
        for i in range (temps):
            cpu_temp.update ({data['Temperatures'][i][u'Name']:data['Temperatures'][i][u'ReadingCelsius']})
        

        if any('LowerThresholdCritical' in d for d in data['Temperatures']):
            cpu_temp_thresholds.update ({'cpuLowerThresholdCritical':data['Temperatures'][0][u'LowerThresholdCritical']})
        else:
            cpu_temp_thresholds.update ({'cpuLowerThresholdCritical': None})

        if any('LowerThresholdNonCritical' in d for d in data['Temperatures']):
            cpu_temp_thresholds.update ({'cpuLowerThresholdNonCritical':data['Temperatures'][0][u'LowerThresholdNonCritical']})
        else:
            cpu_temp_thresholds.update ({'cpuLowerThresholdNonCritical': None})
                            
        if any('UpperThresholdCritical' in d for d in data['Temperatures']):
            cpu_temp_thresholds.update ({'cpuUpperThresholdCritical':data['Temperatures'][0][u'UpperThresholdCritical']})
        else:
            cpu_temp_thresholds.update ({'cpuUpperThresholdCritical':None})

        if any('UpperThresholdNonCritical' in d for d in data['Temperatures']):
            cpu_temp_thresholds.update ({'cpuUpperThresholdNonCritical':data['Temperatures'][0][u'UpperThresholdNonCritical']})
        else:
            cpu_temp_thresholds.update ({'cpuUpperThresholdNonCritical':None})
        
        if (data['Temperatures@odata.count'] > 2):
            inlet_temp.update ({data['Temperatures'][data['Temperatures@odata.count']-1][u'Name']:data['Temperatures'][data['Temperatures@odata.count']-1][u'ReadingCelsius']})
        else:
            inlet_temp.update ({'Inlet Temp':None})

        if (data['Temperatures@odata.count'] > 2):
            inlet_health = data['Temperatures'][data['Temperatures@odata.count']-1][u'Status']['Health']
        else:
            inlet_health = None

        if (data['Temperatures@odata.count'] > 2):
            inlet_temp_thresholds.update ({'inletLowerThresholdCritical':data['Temperatures'][2][u'LowerThresholdCritical']})
        else:
            inlet_temp_thresholds.update ({'inletLowerThresholdCritical':None})
        
        if (data['Temperatures@odata.count'] > 2):
            inlet_temp_thresholds.update ({'inletLowerThresholdNonCritical':data['Temperatures'][2][u'LowerThresholdNonCritical']})
        else:
            inlet_temp_thresholds.update ({'inletLowerThresholdNonCritical':None})

        if (data['Temperatures@odata.count'] > 2):
            inlet_temp_thresholds.update ({'inletUpperThresholdCritical':data['Temperatures'][2][u'UpperThresholdCritical']})
        else:
            inlet_temp_thresholds.update ({'inletUpperThresholdCritical':None})

        if (data['Temperatures@odata.count'] > 2):
            inlet_temp_thresholds.update ({'inletUpperThresholdNonCritical':data['Temperatures'][2][u'UpperThresholdNonCritical']})
        else:
            inlet_temp_thresholds.update ({'inletUpperThresholdNonCritical':None})
        for i in range (data['Fans@odata.count']):
            fan_speed.update({data['Fans'][i][u'FanName']:data['Fans'][i][u'Reading']})
            fan_health.update({data['Fans'][i][u'FanName']:data['Fans'][i][u'Status'][u'Health']})

        if any('LowerThresholdCritical' in d for d in data['Fans']):
            fan_speed_thresholds.update ({'fanLowerThresholdCritical':data['Fans'][0][u'LowerThresholdCritical']})
        else:
            fan_speed_thresholds.update ({'fanLowerThresholdCritical':None})

        if any('LowerThresholdNonCritical' in d for d in data['Fans']):
            fan_speed_thresholds.update ({'fanLowerThresholdNonCritical':data['Fans'][0][u'LowerThresholdNonCritical']})
        else:
            fan_speed_thresholds.update ({'fanLowerThresholdNonCritical':None})

        if any ('UpperThresholdCritical' in d for d in data['Fans']):
            fan_speed_thresholds.update ({'fanUpperThresholdCritical':data['Fans'][0][u'UpperThresholdCritical']})
        else:
            fan_speed_thresholds.update ({'fanUpperThresholdCritical':None})
        
        if any ('UpperThresholdNonCritical' in d for d in data['Fans']):
            fan_speed_thresholds.update ({'fanUpperThresholdNonCritical':data['Fans'][0][u'UpperThresholdNonCritical']})
        else:
            fan_speed_thresholds.update ({'fanUpperThresholdNonCritical':None})

        #print ("cpu temp:",cpu_temp)                                                                                                                                                                        
        #print ("inlet temp:",inlet_temp)                                                                                                                                                                    
        #print ("inlet health:",inlet_health)                                                                                                                                                                
        

        return cpu_temp, inlet_temp, fan_speed, fan_health, cpu_temp_thresholds, fan_speed_thresholds, inlet_temp_thresholds, inlet_health,str(None)                                                        


    except requests.exceptions.RequestException as e:
        return None,None,None,None, None,None, None, None,str(e)
        

    
###############################################################################################                                                                                           
# Fetch Power Consumed in WATTS
###############################################################################################

def get_powerusage(host,conn_time_out,read_time_out,session):
    try:
        url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"
        #session = requests.Session()

        response = session.get(url,verify=False, auth=(userName,passwd), timeout=(conn_time_out,read_time_out))
        response.raise_for_status()
        data = response.json()
        
        pwr_thresholds = {}

        
        #pwr_thresholds.update ({'PowerCapacityWatts':data['PowerControl'][0][u'PowerCapacityWatts']})
        #pwr_thresholds.update ({'PowerRequestedWatts':data['PowerControl'][0][u'PowerRequestedWatts']})
        #pwr_thresholds.update ({'PowerAvailableWatts':data['PowerControl'][0][u'PowerAvailableWatts']})
        #pwr_thresholds.update ({'PowerMetrics':data['PowerControl'][0][u'PowerMetrics']})
        #pwr_thresholds.update ({'PowerLimit':data['PowerControl'][0][u'PowerLimit']})
        #pwr_thresholds.update ({'PowerAllocatedWatts':data['PowerControl'][0][u'PowerAllocatedWatts']})
        try:
            if u'PowerControl' in data:
                if u'PowerConsumedWatts' in data[u'PowerControl'][0]:    
                    return data[u'PowerControl'][0][u'PowerConsumedWatts'], pwr_thresholds, str(None)
                else:
                    return None, None, str(None)
        except:
            return None, None, str(None)
    except requests.exceptions.RequestException as e:
        return None,None, str(e)


###############################################################################################                                                                                                
# This function fetch live jobs data across the whole cluster by making a single call to UGE                                                                                              
###############################################################################################

def get_hpcjob_data(conn_time_out,read_time_out,session):
    try:
        url = "http://129.118.104.35:8182/hostsummary/1/500"
        #session = requests.Session()

        response = session.get(url,verify=False, timeout=(conn_time_out,read_time_out))
           
        response.raise_for_status()
        data = response.json()
        #print (data)
        
        #return None, None
        return data, str(None)


    except requests.exceptions.RequestException as e:
        return None, str(e)



    
###############################################################################################
# getNodesData is called by core_to_threads (). This function performs the following three steps:
# 1) get Redfish check/metric according to checkType passed
# 2) Retry THREE TIMES in case of failure
# 3) Build metric(s)
##############################################################################################

def getNodesData (host, checkType, json_node_list, error_list,session,metricTimeStamp):
    
    error=""
    # Based on our experience, 13G iDRAC takes 3 to 5 seconds to process a Redfish API call so
    # so in order to make monitoring more responsive, initial timeout is set to minimum
    conn_time_out=30
    read_time_out=50

    # global prevMetrics
    

    # Fetch the monitoring data based on check type                                                                                                               

    ###############################################################################################                                                                                                           
    # Process CPU POWER check type                                                                                                                                                                           
    ###############################################################################################
    if checkType == 'CPUPWR':
        # starting time of check processing
        start_time = time.time()

        # retry=0 indicates first time call
        #retry=0
        
        # copy original time_out
        #initial_timeout = time_out
        
        # get cpu power usage by passing host, timeout, and session objects
        cpu_cur_pwr_usage,cpu_max_pwr_usage,cpu_min_pwr_usage,cpu_avg_pwr_usage, error  = get_cpupowerusage(host,conn_time_out,read_time_out,session)
        
        # total processing time
        tot_time=time.time() - start_time
        
        # first error (if any) is copied
        # initial_error = error

        # In case of no error, response is recieved successfully without any retry
        if error == 'None':
            # power usage metric is built and result is returned into a dictionary
            host = host.replace('100','101')

            # if bool (prevMetrics):
            # if prevMetrics.get(host+'-'+'CPUPowerUsage'):
            #     if (round (prevMetrics[host+'-'+'CPUPowerUsage']) != round (cpu_cur_pwr_usage)):
            #         prevMetrics[host+'-'+'CPUPowerUsage'] = cpu_cur_pwr_usage
            #         mon_data_dict = build_cpupower_usage_metric(metricTimeStamp,cpu_cur_pwr_usage,cpu_max_pwr_usage,cpu_min_pwr_usage,cpu_avg_pwr_usage,tot_time,host,error)
            #         # monitored data (in dictionary) is appended into global list passed by core_to_threads ()
            #         json_node_list.append(mon_data_dict)
            #         # error ('None' in this case) with host and checktype is appended to global error list
            #         error_list.append([host, checkType, error])
            # else:
            #     prevMetrics[host+'-'+'CPUPowerUsage'] = cpu_cur_pwr_usage
            mon_data_dict = build_cpupower_usage_metric(metricTimeStamp,cpu_cur_pwr_usage,cpu_max_pwr_usage,cpu_min_pwr_usage,cpu_avg_pwr_usage,tot_time,host,error)
            # monitored data (in dictionary) is appended into global list passed by core_to_threads ()
            json_node_list.append(mon_data_dict)
            # error ('None' in this case) with host and checktype is appended to global error list
            error_list.append([host, checkType, error])

    ###############################################################################################                                                                                                           
    # Process MEMORY POWER check type                                                                                                                                                                           
    ###############################################################################################
    if checkType == 'MEMPWR':
        # starting time of check processing
        start_time = time.time()

        # retry=0 indicates first time call
        #retry=0
        
        # copy original time_out
        #initial_timeout = time_out
        
        # get memory power usage by passing host, timeout, and session objects
        mem_cur_pwr_usage,mem_max_pwr_usage,mem_min_pwr_usage,mem_avg_pwr_usage, error  = get_mempowerusage(host,conn_time_out,read_time_out,session)
        
        # total processing time
        tot_time=time.time() - start_time
        
        # first error (if any) is copied
        # initial_error = error

        # In case of no error, response is recieved successfully without any retry
        if error == 'None':
            host = host.replace('100','101')
            # if bool (prevMetrics):
            # if prevMetrics.get(host+'-'+'MemPowerUsage'):
            #     if (round (prevMetrics[host+'-'+'MemPowerUsage']) != round (mem_cur_pwr_usage)):
            #         prevMetrics[host+'-'+'MemPowerUsage'] = mem_cur_pwr_usage
            #         mon_data_dict = build_mempower_usage_metric(metricTimeStamp,mem_cur_pwr_usage,mem_max_pwr_usage,mem_min_pwr_usage,mem_avg_pwr_usage,tot_time,host,error)
            #         # monitored data (in dictionary) is appended into global list passed by core_to_threads ()
            #         json_node_list.append(mon_data_dict)
            #         # error ('None' in this case) with host and checktype is appended to global error list
            #         error_list.append([host, checkType, error])
            # else:
            #     prevMetrics[host+'-'+'MemPowerUsage'] = mem_cur_pwr_usage
            # power usage metric is built and result is returned into a dictionary
            mon_data_dict = build_mempower_usage_metric(metricTimeStamp,mem_cur_pwr_usage,mem_max_pwr_usage,mem_min_pwr_usage,mem_avg_pwr_usage,tot_time,host,error)
            # monitored data (in dictionary) is appended into global list passed by core_to_threads ()
            json_node_list.append(mon_data_dict)
            # error ('None' in this case) with host and checktype is appended to global error list
            error_list.append([host, checkType, error])

    ###############################################################################################                                                                                                           
    # Process "Power" check type                                                                                                                                                                           
    ###############################################################################################
    if checkType == 'Power':
        # starting time of check processing
        start_time = time.time()

        # retry=0 indicates first time call
        retry=0
        
        # copy original time_out
        #initial_timeout = time_out
        
        # get power usage by passing host, timeout, and session objects
        power_usage, pwr_thresholds, error  = get_powerusage(host,conn_time_out,read_time_out,session)
        
        # total processing time
        tot_time=time.time() - start_time
        
        # first error (if any) is copied
        # initial_error = error
        # In case of no error, response is recieved successfully without any retry
        if error == 'None':
            # power usage metric is built and result is returned into a dictionary
            if power_usage != None:
                # if bool (prevMetrics):
                # if prevMetrics.get(host+'-'+'NodePower'):
                #     if (round (prevMetrics[host+'-'+'NodePower']) != round (power_usage)):
                #         prevMetrics[host+'-'+'NodePower'] = power_usage
                #         mon_data_dict = build_power_usage_metric(metricTimeStamp,power_usage,host)
                #         # monitored data (in dictionary) is appended into global list passed by core_to_threads ()
                #         json_node_list.append(mon_data_dict)
                #         # error ('None' in this case) with host and checktype is appended to global error list
                #         error_list.append([host, checkType, error])
                # else:
                #     prevMetrics[host+'-'+'NodePower'] = power_usage
                mon_data_dict = build_power_usage_metric(metricTimeStamp,power_usage,host)
                # monitored data (in dictionary) is appended into global list passed by core_to_threads ()
                json_node_list.append(mon_data_dict)
                # error ('None' in this case) with host and checktype is appended to global error list
                error_list.append([host, checkType, error])
        # else:
        #     # First retry
        #     retry += 1
        #     #print ("\nRetry:",retry,"Error:",error)
        #     # As failure has occurred so it is produent to make a quick retry
        #     # so a retry with original timeout is done (i.e. retry=1)
        #     #time_out = initial_timeout*retry
        #     # get power usage
        #     power_usage, pwr_thresholds, error  = get_powerusage(host,conn_time_out,read_time_out,session)
            
        #     # check if error = None
        #     if error == 'None':
        #         # if so , updated the total time
        #         tot_time=time.time() - start_time
        #         #build power usage metric with retry = 1 i.e. First Retry Success
        #         mon_data_dict = build_power_usage_metric(power_usage,tot_time,host,pwr_thresholds,retry,error)
        #         # append the monitoring data (as dict)into global json_node_list
        #         json_node_list.append(mon_data_dict)
        #         # Final error=None and Initial Error are concatednated so that 
        #         # we can get trace back why the request fail at first instance
        #         #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #         # error is appended to global error list
        #         error_list.append([host, checkType, error])
        #     else:
        #         # Second retry
        #         retry += 1
        #         #print ("\nRetry:",retry,"Error:",error)                                                                                    
        #         #time_out = initial_timeout*retry
        #         power_usage, pwr_thresholds, error  = get_powerusage(host,conn_time_out,read_time_out,session)

        #         if error == 'None':
        #             tot_time=time.time() - start_time
        #             mon_data_dict = build_power_usage_metric(power_usage,tot_time,host,pwr_thresholds,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #             error_list.append([host, checkType, error])
        #         else:
        #             # Third retry
        #             retry += 1
        #             #print ("\nRetry:",retry,"Error:",error)                                                                                
        #             #time_out = initial_timeout*retry
        #             power_usage, pwr_thresholds, error  = get_powerusage(host,conn_time_out,read_time_out,session)
        #             if error != 'None':
        #                 retry = None
        #             tot_time=time.time() - start_time
        #             mon_data_dict = build_power_usage_metric(power_usage,tot_time,host,pwr_thresholds,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #             error_list.append([host, checkType, error])
   
    ###############################################################################################                                                                                                           
    # Process "Thermal" check                                                                                                                                                                               
    ############################################################################################### 
    elif checkType == 'Thermal':
        start_time = time.time()
        retry=0
        #initial_timeout = time_out

        cpu_temperature, inlet_temp,fan_speed, fan_health, cpu_temp_thresholds, fan_speed_thresholds, inlet_temp_thresholds, inlet_health, error  = get_thermal(host,conn_time_out,read_time_out,session)
        
        tot_time=time.time() - start_time
        
        #initial_error = error
        if error == 'None':
            if cpu_temperature != None:
                cpukeys = cpu_temperature.keys()
                cpuvals = cpu_temperature.values()

                # if bool (prevMetrics):
                # if prevMetrics.get(host+'-'+'CPU1Temp'):
                #     for (k,v) in zip(cpukeys, cpuvals):
                #         if (round (prevMetrics[host+'-'+k]) != round (v)):
                #             prevMetrics[host+'-'+k] = v
                #             mon_data_dict = build_cpu_temperature_metric(metricTimeStamp,k, v, host)
                #             json_node_list.append(mon_data_dict)
                #             error_list.append([host, checkType, error])
                # else:
                for (k,v) in zip(cpukeys, cpuvals):
                        # prevMetrics[host+'-'+k] = v
                    mon_data_dict = build_cpu_temperature_metric(metricTimeStamp,k, v, host)
                    json_node_list.append(mon_data_dict)
                    error_list.append([host, checkType, error])

            if inlet_temp != None:
                cpukeys = inlet_temp.keys()
                cpuvals = inlet_temp.values()
                
                # if bool (prevMetrics):
                # if prevMetrics.get(host+'-'+'InletTemp'):
                #     for (k,v) in zip (cpukeys,cpuvals):
                #         if v != None:
                #             if (round (prevMetrics[host+'-'+k]) != round (v)):
                #                 prevMetrics[host+'-'+k] = v    
                #                 mon_data_dict = build_inlet_temperature_metric(metricTimeStamp,k,v, host)
                #                 json_node_list.append(mon_data_dict)
                #                 error_list.append([host, checkType, error])
                # else:
                for (k,v) in zip (cpukeys,cpuvals):
                    if v != None:
                            # prevMetrics[host+'-'+k] = v
                            # prevMetrics[host+'-'+k] = v    
                        mon_data_dict = build_inlet_temperature_metric(metricTimeStamp,k,v, host)
                        json_node_list.append(mon_data_dict)
                        error_list.append([host, checkType, error])
            
            if fan_speed != None:
                fankeys = fan_speed.keys()
                fanvals = fan_speed.values()
                
                # if bool (prevMetrics):
                # if prevMetrics.get(host+'-'+'FAN_1Speed'):
                #     for k,v in zip(fankeys,fanvals):
                #         if (round (prevMetrics[host+'-'+k]) != round (v)):
                #             prevMetrics[host+'-'+k] = v
                #             mon_data_dict = build_fanspeed_metric(metricTimeStamp,k,v,host)
                #             json_node_list.append(mon_data_dict)
                #             error_list.append([host, checkType, error])
                # else:
                for k,v in zip(fankeys,fanvals):
                        # prevMetrics[host+'-'+k] = v
                    mon_data_dict = build_fanspeed_metric(metricTimeStamp,k,v,host)
                    json_node_list.append(mon_data_dict)
                    error_list.append([host, checkType, error])

            if inlet_health != None:
                # if bool (prevMetrics):
                # if prevMetrics.get(host+'-'+'InletHealth'):
                #     if (prevMetrics[host+'-'+'InletHealth'] != inlet_health):
                #         prevMetrics[host+'-'+'InletHealth'] = inlet_health
                #         mon_data_dict = build_inlethealth_metric(metricTimeStamp,inlet_health,tot_time,host,retry,error)
                #         json_node_list.append(mon_data_dict)
                #         error_list.append([host, checkType, error])
                # else:
                #     prevMetrics[host+'-'+'InletHealth'] = inlet_health
                mon_data_dict = build_inlethealth_metric(metricTimeStamp,inlet_health,tot_time,host,retry,error)
                json_node_list.append(mon_data_dict)
                error_list.append([host, checkType, error])

            if fan_health != None:
                fankeys = fan_health.keys()
                fanvals = fan_health.values()
                
                # if bool (prevMetrics):
                # if prevMetrics.get(host+'-'+'FAN_1Health'):
                #     for k,v in zip(fankeys,fanvals):               
                #         if (prevMetrics[host+'-'+k] != v):
                #             prevMetrics[host+'-'+k] = v
                #             mon_data_dict = build_fanhealth_metric(metricTimeStamp,k,v,tot_time,host,retry,error)
                #             json_node_list.append(mon_data_dict)
                #             error_list.append([host, checkType, error])
                # else:
                for k,v in zip(fankeys,fanvals):               
                        # prevMetrics[host+'-'+k] = v
                    mon_data_dict = build_fanhealth_metric(metricTimeStamp,k,v,tot_time,host,retry,error)
                    json_node_list.append(mon_data_dict)
                    error_list.append([host, checkType, error])
        # else:
        #     retry += 1
        #     #print ("\nRetry:",retry,"Error:",error)
        #     cpu_temperature, inlet_temp, fan_speed,fan_health, cpu_temp_thresholds, fan_speed_thresholds, inlet_temp_thresholds, inlet_health, error  = get_thermal(host,conn_time_out,read_time_out,session)
        #     if error =='None':
        #         tot_time=time.time() - start_time

        #         mon_data_dict = build_cpu_temperature_metric(cpu_temperature,tot_time,host,cpu_temp_thresholds,retry,error)
        #         json_node_list.append(mon_data_dict)
        #         # error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #         error_list.append([host, checkType, error])

        #         mon_data_dict = build_inlet_temperature_metric(inlet_temp,tot_time,host,inlet_temp_thresholds,retry,error)
        #         json_node_list.append(mon_data_dict)
        #         error_list.append([host, checkType, error])
                
        #         mon_data_dict = build_inlethealth_metric(inlet_health,tot_time,host,retry,error)
        #         json_node_list.append(mon_data_dict)
        #         error_list.append([host, checkType, error])

        #         mon_data_dict = build_fanspeed_metric(fan_speed,tot_time,host, fan_speed_thresholds,retry,error)
        #         json_node_list.append(mon_data_dict)
        #         error_list.append([host, checkType, error])

        #         mon_data_dict = build_fanhealth_metric(fan_health,tot_time,host, retry,error)
        #         json_node_list.append(mon_data_dict)
        #         error_list.append([host, checkType, error])
        #     else:
        #         retry += 1
        #         #print ("\nRetry:",retry,"Error:",error)
        #         #time_out = initial_timeout*retry
        #         cpu_temperature, inlet_temp,fan_speed, fan_health, cpu_temp_thresholds, fan_speed_thresholds, inlet_temp_thresholds, inlet_health, error  = get_thermal(host,conn_time_out,read_time_out,session)
        #         if error =='None':
        #             tot_time=time.time() - start_time

        #             mon_data_dict = build_cpu_temperature_metric(cpu_temperature,tot_time,host,cpu_temp_thresholds,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #             error_list.append([host, checkType, error])

        #             mon_data_dict = build_inlet_temperature_metric(inlet_temp,tot_time,host,inlet_temp_thresholds,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])

        #             mon_data_dict = build_inlethealth_metric(inlet_health,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])
                    
        #             mon_data_dict = build_fanspeed_metric(fan_speed,tot_time,host, fan_speed_thresholds,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])
                    
        #             mon_data_dict = build_fanhealth_metric(fan_health,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])
        #         else:
        #             retry += 1
        #             #print ("\nRetry:",retry,"Error:",error)
        #             #time_out = initial_timeout*retry
        #             cpu_temperature, inlet_temp,fan_speed, fan_health, cpu_temp_thresholds, fan_speed_thresholds, inlet_temp_thresholds, inlet_health, error  = get_thermal(host,conn_time_out,read_time_out,session)
                    
        #             if error != 'None':
        #                 retry = None

        #             tot_time=time.time() - start_time

        #             mon_data_dict = build_cpu_temperature_metric(cpu_temperature,tot_time,host,cpu_temp_thresholds,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #             error_list.append([host, checkType, error])

        #             mon_data_dict = build_inlet_temperature_metric(inlet_temp,tot_time,host,inlet_temp_thresholds,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])

        #             mon_data_dict = build_inlethealth_metric(inlet_health,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])

        #             mon_data_dict1 = build_fanspeed_metric(fan_speed,tot_time,host,fan_speed_thresholds,retry,error)
        #             json_node_list.append(mon_data_dict1)
        #             error_list.append([host, checkType, error])
                    
        #             mon_data_dict = build_fanhealth_metric(fan_health,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])

    ###############################################################################################                                                                                                           
    # Process "Host", "CPU", and 'Memory" checks                                                                                                                                                              
    ############################################################################################### 

    elif checkType == 'SystemHealth':

        start_time = time.time()
        retry=0
        #initial_timeout = time_out        
        
        host_health, cpu_health, mem_health, host_led_indicator,host_power_state,  error  = get_system_health(host,conn_time_out,read_time_out,session)
        tot_time=time.time() - start_time
        #initial_error = error

        if error == 'None':
            if host_health != None:
                #if bool (prevMetrics):
                # if prevMetrics.get (host+'-'+'NodeHealth'):
                #     if (prevMetrics[host+'-'+'NodeHealth'] != host_health):
                #         prevMetrics[host+'-'+'NodeHealth'] = host_health
                #         mon_data_dict = build_host_health_metric(metricTimeStamp,host_health,tot_time,host,retry,error)
                #         json_node_list.append(mon_data_dict)
                #         error_list.append([host, checkType, error])
                # else:
                #     prevMetrics[host+'-'+'NodeHealth'] = host_health
                mon_data_dict = build_host_health_metric(metricTimeStamp,host_health,tot_time,host,retry,error)
                json_node_list.append(mon_data_dict)
                error_list.append([host, checkType, error])

            if cpu_health != None:
                # if bool (prevMetrics):
                # if prevMetrics.get(host+'-'+'CPUHealth'):
                #     if (prevMetrics[host+'-'+'CPUHealth'] != cpu_health):
                #         prevMetrics[host+'-'+'CPUHealth'] = cpu_health
                #         mon_data_dict = build_cpu_health_metric(metricTimeStamp,cpu_health,tot_time,host,retry,error)
                #         json_node_list.append(mon_data_dict)
                #         error_list.append([host, checkType, error])
                # else:
                #     prevMetrics[host+'-'+'CPUHealth'] = cpu_health
                mon_data_dict = build_cpu_health_metric(metricTimeStamp,cpu_health,tot_time,host,retry,error)
                json_node_list.append(mon_data_dict)
                error_list.append([host, checkType, error])

            if mem_health != None:
                # if bool (prevMetrics):
                # if prevMetrics.get(host+'-'+'MemHealth'):
                #     if (prevMetrics[host+'-'+'MemHealth'] != mem_health):
                #         prevMetrics[host+'-'+'MemHealth'] = mem_health
                #         mon_data_dict = build_mem_health_metric(metricTimeStamp,mem_health,tot_time,host,retry,error)
                #         json_node_list.append(mon_data_dict)
                #         error_list.append([host, checkType, error])
                # else:
                #     prevMetrics[host+'-'+'MemHealth'] = mem_health
                mon_data_dict = build_mem_health_metric(metricTimeStamp,mem_health,tot_time,host,retry,error)
                json_node_list.append(mon_data_dict)
                error_list.append([host, checkType, error])

            if host_led_indicator != None:
                # if bool (prevMetrics):
                # if prevMetrics.get(host+'-'+'IndicatorLEDStatus'):
                #     if (prevMetrics[host+'-'+'IndicatorLEDStatus'] != host_led_indicator):
                #         prevMetrics[host+'-'+'IndicatorLEDStatus'] = host_led_indicator
                #         mon_data_dict = build_led_indicator_metric(metricTimeStamp,host_led_indicator,tot_time,host,retry,error)
                #         json_node_list.append(mon_data_dict)
                #         error_list.append([host, checkType, error])
                # else:
                #     prevMetrics[host+'-'+'IndicatorLEDStatus'] = host_led_indicator
                mon_data_dict = build_led_indicator_metric(metricTimeStamp,host_led_indicator,tot_time,host,retry,error)
                json_node_list.append(mon_data_dict)
                error_list.append([host, checkType, error])

            if host_power_state != None:
                # if bool (prevMetrics):
                # if prevMetrics.get(host+'-'+'PowerState'):
                #     if (prevMetrics[host+'-'+'PowerState'] != host_power_state):
                #         prevMetrics[host+'-'+'PowerState'] = host_power_state
                #         mon_data_dict = build_power_state_metric(metricTimeStamp,host_power_state,tot_time,host,retry,error)
                #         json_node_list.append(mon_data_dict)
                #         error_list.append([host, checkType, error])
                # else:
                #     prevMetrics[host+'-'+'PowerState'] = host_power_state
                mon_data_dict = build_power_state_metric(metricTimeStamp,host_power_state,tot_time,host,retry,error)
                json_node_list.append(mon_data_dict)
                error_list.append([host, checkType, error])
        
        # else:
        #     retry += 1
        #     #print ("\nRetry:",retry,"Error:",error)                                                                                                                                      
        #     #time_out = initial_timeout*retry
        #     host_health, cpu_health, mem_health, host_led_indicator, host_power_state, error  = get_system_health(host,conn_time_out,read_time_out,session)
        #     if error == 'None':
        #         mon_data_dict = build_host_health_metric(host_health,tot_time,host,retry,error)
        #         json_node_list.append(mon_data_dict)
        #         #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #         error_list.append([host, checkType, error])

        #         mon_data_dict = build_cpu_health_metric(cpu_health,tot_time,host,retry,error)
        #         json_node_list.append(mon_data_dict)
        #         error_list.append([host, checkType, error])

        #         mon_data_dict = build_mem_health_metric(mem_health,tot_time,host,retry,error)
        #         json_node_list.append(mon_data_dict)
        #         error_list.append([host, checkType, error])
                
        #         mon_data_dict = build_led_indicator_metric(host_led_indicator,tot_time,host,retry,error)
        #         json_node_list.append(mon_data_dict)
        #         error_list.append([host, checkType, error])

        #         mon_data_dict = build_power_state_metric(host_power_state,tot_time,host,retry,error)
        #         json_node_list.append(mon_data_dict)
        #         error_list.append([host, checkType, error])

        #     else:
        #         retry += 1
        #         #print ("\nRetry:",retry,"Error:",error)                                                                                                                                  
        #         #time_out = initial_timeout*retry
        #         host_health, cpu_health, mem_health, host_led_indicator, host_power_state, error  = get_system_health(host,conn_time_out,read_time_out,session)
        #         if error =='None':

        #             mon_data_dict = build_host_health_metric(host_health,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #             error_list.append([host, checkType, error])

        #             mon_data_dict = build_cpu_health_metric(cpu_health,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])

        #             mon_data_dict = build_mem_health_metric(mem_health,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])
                    
        #             mon_data_dict = build_led_indicator_metric(host_led_indicator,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])

        #             mon_data_dict = build_power_state_metric(host_power_state,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])

        #         else:
        #             retry += 1
        #             #print ("\nRetry:",retry,"Error:",error)                                                                                                                              
        #             #time_out = initial_timeout*retry
        #             host_health, cpu_health, mem_health, host_led_indicator, host_power_state, error  = get_system_health(host,conn_time_out,read_time_out,session)
                    
        #             if error != 'None':
        #                 retry = None

        #             mon_data_dict = build_host_health_metric(host_health,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #             error_list.append([host, checkType, error])

        #             mon_data_dict = build_cpu_health_metric(cpu_health,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])

        #             mon_data_dict = build_mem_health_metric(mem_health,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])

        #             mon_data_dict = build_led_indicator_metric(host_led_indicator,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])
                    
        #             mon_data_dict = build_power_state_metric(host_power_state,tot_time,host,retry,error)
        #             json_node_list.append(mon_data_dict)
        #             error_list.append([host, checkType, error])
    
    ###############################################################################################                                                                                                           
    # Process "BMC" health check                                                                                                                                                                              
    ############################################################################################### 
    elif checkType == 'BMCHealth':                                                                                                                                                                 
        start_time = time.time()                                                                                                                                                           
        retry=0                                                                                                                                                                            
        #initial_timeout = time_out                                                                                                                                                         
        bmc_health, error  = get_bmc_health(host,conn_time_out,read_time_out,session)                                                                                            
        tot_time=time.time() - start_time
        
        #initial_error = error

        if error == 'None':
            # if bool (prevMetrics):
            # if prevMetrics.get(host+'-'+'BMCHealth'):
            #     if (prevMetrics[host+'-'+'BMCHealth'] != bmc_health):
            #         prevMetrics[host+'-'+'BMCHealth'] = bmc_health                                                                                                                                                                 
            #         mon_data_dict = build_bmc_health_metric(metricTimeStamp,bmc_health,tot_time,host,retry,error)                                                                                                            
            #         json_node_list.append(mon_data_dict)                                                                                                                                          
            #         error_list.append([host, checkType, error])
            #     else:
            #         prevMetrics[host+'-'+'BMCHealth'] = bmc_health                                                                                                                                                                 
            mon_data_dict = build_bmc_health_metric(metricTimeStamp,bmc_health,tot_time,host,retry,error)                                                                                                            
            json_node_list.append(mon_data_dict)                                                                                                                                          
            error_list.append([host, checkType, error])      
        else:
            error_list.append([host, checkType, error])                                                                                                                              
        # else:                                                                                                                                                                              
        #     retry += 1                                                                                                                                                                    
        #     #print ("\nRetry:",retry,"Error:",error)                                                                                                                                      
        #     #time_out = initial_timeout*retry                                                                                                                                             
        #     bmc_health, error = get_bmc_health(host,conn_time_out,read_time_out,session)                                                                                                                            
                    
        #     if error == 'None':                                                                                                                                                             
        #         tot_time=time.time() - start_time                                                                                                                                         
        #         mon_data_dict = build_bmc_health_metric(bmc_health,tot_time,host,retry,error)                                                                                                        
        #         json_node_list.append(mon_data_dict)     
        #         #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #         error_list.append([host, checkType, error])                                                                                                                                
        #     else:                                                                                                                                                                          
        #         retry += 1                                                                                                                                                                
        #         #print ("\nRetry:",retry,"Error:",error)                                                                                                                                   
        #         #time_out = initial_timeout*retry                                                                                                                                          
        #         bmc_health, error  = get_bmc_health(host,conn_time_out,read_time_out,session)                                                                                  
                                                                                                                                                                                           
        #         if error =='None':                                                                                                                                                           
        #             tot_time=time.time() - start_time                                                                                                                                     
        #             mon_data_dict = build_bmc_health_metric(bmc_health,tot_time,host,retry,error)                                                                                    
        #             json_node_list.append(mon_data_dict)         
        #             #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #             error_list.append([host, checkType, error])                                                                                                                            
        #         else:                                                                                                                                                                      
        #             retry += 1                                                                                                                                                            
        #             #print ("\nRetry:",retry,"Error:",error)                                                                                                                                                   
        #             #time_out = initial_timeout*retry                                                                                                                                      
        #             bmc_health, error  = get_bmc_health(host,conn_time_out,read_time_out,session)                                                                                                                             
        #             tot_time=time.time() - start_time
                    
        #             if error != 'None':
        #                 retry = None

        #             mon_data_dict = build_bmc_health_metric(bmc_health,tot_time,host,retry,error)                                                                                                    
        #             json_node_list.append(mon_data_dict)                         
        #             #error="FINAL ERROR: "+error+"; INITIAL ERROR: "+initial_error
        #             error_list.append([host, checkType, error]) 
    
    ###############################################################################################                                                              
    # Process "HPCJob" check type. This metric is not available via iDRAC                                                                                         
    ############################################################################################### 

    elif checkType == 'HPCJob':
        job_data, error  = get_hpcjob_data(conn_time_out,read_time_out,session)
        
        if error == 'None':
            #timeStamp = int(datetime.now().timestamp())
            #getJobInfo(job_data,error,json_node_list,error_list,checkType,timeStamp)
            build_jobs_metric (job_data,error,json_node_list,error_list,checkType,metricTimeStamp)

###############################################################################################                                                                                                               
# Builds CPU power usages in watts metric by encapsulating the power usage and other infos into dictionary                                                                                                        
############################################################################################### 

def build_cpupower_usage_metric(metricTimeStamp,cpu_cur_pwr_usage,cpu_max_pwr_usage,cpu_min_pwr_usage,cpu_avg_pwr_usage,tot_time,host,error):
    
    mon_data_dict = {'measurement':'Power','tags':{'Sensor':'CPUPowerUsage','NodeId': host},'time':metricTimeStamp,'fields':{}}
    mon_data_dict['fields']['Reading'] = cpu_cur_pwr_usage
    return mon_data_dict
    # mon_data_dict = {'measurement':'CPU_Power_Usage','tags':{'cluster':'quanah','host':host,'location':'ESB'},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    
    # mon_data_dict['fields']['CPUAveragePowerUsage'] = cpu_avg_pwr_usage
    # mon_data_dict['fields']['CPUCurrentPowerUsage'] = cpu_cur_pwr_usage
    # mon_data_dict['fields']['CPUMinPowerUsage'] = cpu_min_pwr_usage
    # mon_data_dict['fields']['CPUMaxPowerUsage'] = cpu_max_pwr_usage

    # mon_data_dict['fields']['error'] =error
    # mon_data_dict['time'] = metricTimeStamp
    # return mon_data_dict

    

def build_mempower_usage_metric(metricTimeStamp,mem_cur_pwr_usage,mem_max_pwr_usage,mem_min_pwr_usage,mem_avg_pwr_usage,tot_time,host,error):
    host = host.replace('100','101')
    mon_data_dict = {'measurement':'Power','tags':{'Sensor':'MemPowerUsage','NodeId': host},'time':metricTimeStamp,'fields':{}}
    mon_data_dict['fields']['Reading'] = mem_cur_pwr_usage
    return mon_data_dict
    # mon_data_dict = {'measurement':'Memory_Power_Usage','tags':{'cluster':'quanah','host':host,'location':'ESB'},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    
    # mon_data_dict['fields']['MemoryAveragePowerUsage'] = mem_avg_pwr_usage
    # mon_data_dict['fields']['MemoryCurrentPowerUsage'] = mem_cur_pwr_usage
    # mon_data_dict['fields']['MemoryMinPowerUsage'] = mem_min_pwr_usage
    # mon_data_dict['fields']['MemoryMaxPowerUsage'] = mem_max_pwr_usage

    # mon_data_dict['fields']['error'] =error
    # mon_data_dict['time'] = metricTimeStamp
    # return mon_data_dict
            
def build_jobs_metric (job_data,error,json_node_list,error_list,checkType,timeStamp):
    jsonJobList = []
    userNames = []
    jobsID = []
    jl = {}
    nr = []
    lastLiveJobs = []

    # maintain global list of jobs
    #global lastLiveJobs
    fName = '/home/production/lastjobs.txt'
    if os.path.exists(fName):
        with open(fName,'r') as lastJobs:
            lastLiveJobs = json.load(lastJobs)
    
    print ("\n *** LAST LIVE JOBS: ", len(lastLiveJobs), " ****\n")

    newJobs = []

    for hostinfo in job_data:
        node = get_hostip(hostinfo['hostname'].split('.')[0])
        if node != None:
            jobLoad (hostinfo, node,json_node_list,error_list,checkType,timeStamp)
        for j in hostinfo['jobList']:
            if (j['masterQueue'] == 'MASTER'):
                continue

            jID = str(j['id'])
            if 'taskId' in j:
                jID = jID+'.'+j['taskId']
            
            if jID not in lastLiveJobs:
                lastLiveJobs.append(jID)
                newJobs.append(jID)

                jobItem = next((job for job in jsonJobList if job["measurement"] == jID),None)
                if jobItem == None:
                    jobStartTime = int(datetime.strptime(j['startTime'], '%a %b %d %H:%M:%S %Z %Y').timestamp())    
                    jobSubmitTime = int(datetime.strptime(j['submitTime'], '%a %b %d %H:%M:%S %Z %Y').timestamp())
                    jsonJobList.append({'measurement': jID, 'time': timeStamp, 'fields': {'TotalNodes':1,'JobName':j['name'],'SubmitTime': jobSubmitTime, 'NodeList': [node+'-1'],'User': j['user'], 'StopTime':0,'StartTime': jobStartTime,'CPUCores':1}, 'tags': {'JobId': jID,'Queue': j['queue']}})
                else:
                    jobItem['fields']['CPUCores'] += 1
                    node_addresses = jobItem['fields']['NodeList']
                    exists = 0
                    for n in node_addresses:
                        if node in n:
                            exists = 1
                            node_addresses[node_addresses.index(n)] = node+'-'+str(int(n[n.find('-')+1:])+1)

                    if exists == 0:
                        jobItem['fields']['NodeList'].append(node+'-1')
                        jobItem['fields']['TotalNodes']=len(jobItem['fields']['NodeList'])
                  
            
            #.....
            jl.update({jID:jID})
            #if (any(jID in ele for ele in jobList)):
            if j['state'] != 'r':
                if jID not in nr:
                    nr.append(jID)
            #.....

            if j['user'] not in userNames:
                userNames.append(j['user'])

            if jID not in jobsID:
                jobsID.append(jID)

    # print (len(jobsID))
    # print (len(jl))
    # print (len(nr))
    # print (len(jsonJobList))
    print ("\n *** NEW JOBS: ", len(newJobs), " ****\n")
    # update finished time of finished jobs
    finishedJobs = []
    
    for lj in lastLiveJobs:
        if lj not in jobsID:
            finishedJobs.append(lj)
            lastLiveJobs.remove(lj)

    print ("\n *** FINISHED JOBS: ", len(finishedJobs), " ****\n")

    print ("\n *** Updated Last Live JOBS: ", len(lastLiveJobs), " ****\n")

    
    
    with open(fName,'w') as writejobs:
        json.dump(lastLiveJobs, writejobs)

    client1 = InfluxDBClient(host='localhost', port=8086)
    client1.switch_database('newtest_quanah_db')

    updateFinishedJobs (finishedJobs, client1, timeStamp)
            
    #print (jsonJobList)
    for jj in jsonJobList:
        nl = jj['fields']['NodeList']
        jj['fields']['NodeList'] = ','.join(str(n) for n in nl)

    if jsonJobList:
        #print ('\njson_node_list: ',len(jsonJobList),'\n')
        # json_node_list += jsonJobList
        json_node_list += build_node_job_mapping(jsonJobList, newJobs, timeStamp)

        for jj in jsonJobList:
            jj['measurement'] = 'JobsInfo'
            # nl = jj['fields']['NodeList']
            # jj['fields']['NodeList'] = ','.join(str(n) for n in nl)
        json_node_list += jsonJobList
        
    # if userNames:
    #     mon_data_dict = build_currentusers_metric(userNames,timeStamp)
    #     json_node_list.append(mon_data_dict)
    #     error_list.append(['cluster', checkType, 'None'])
    # if jobsID:
    #     mon_data_dict = build_currentjobsid_metric(jobsID,timeStamp)
    #     json_node_list.append(mon_data_dict)
    #     error_list.append(['cluster', checkType, 'None'])

    #Cluster wide Jobs and Nodes power usage storage
    # node_total_pwr = calc_currentnode_power(client1)
    # job_total_pwr,time = calc_currentjob_power(client1)
    
    
    # if node_total_pwr and job_total_pwr:
    #     mon_data_dict = build_currentjobsnodespwrusage_metric(node_total_pwr,job_total_pwr,time)
    #     json_node_list.append(mon_data_dict)
    #     error_list.append(['cluster_power_usage', checkType, 'None'])
    
def updateFinishedJobs (finishedJob, client,timeStamp):
    for fj in finishedJob:
        
        fj = "'%s'" % fj
        result = client.query("SELECT * FROM JobsInfo where JobId = "+fj+";")
        res = list(result.get_points(measurement='JobsInfo'))
        if res:
            jInfo = [{'measurement': 'JobsInfo', 'time': res[0]['time'], 'fields': {'TotalNodes':res[0]['TotalNodes'],'JobName':res[0]['JobName'],'SubmitTime': res[0]['SubmitTime'], 'NodeList': res[0]['NodeList'],'User': res[0]['User'], 'StopTime':timeStamp,'StartTime': res[0]['StartTime'],'CPUCores':res[0]['CPUCores']}, 'tags': {'JobId': res[0]['JobId'],'Queue': res[0]['Queue']}}]
            client.write_points(jInfo)
 

def build_node_job_mapping(jsonJobList, newJobs, timeStamp):
    jsonNodeJobList = []

    for j in jsonJobList:
        if j['tags']['JobId'] in newJobs:
            if j['fields']['TotalNodes'] > 1:
                nodeAddresses = j['fields']['NodeList'].split(',')
                for nodeAddress in nodeAddresses:
                    l = nodeAddress.split('-')
                    jsonNodeJobList.append({'measurement': 'NodeJobs','tags':{'NodeId':l[0]},'fields':{'JobList':j['measurement']},'time':timeStamp})

            else:
                cnt = 0
                n = j['fields']['NodeList'].split('-')[0]
                for jobnode in jsonNodeJobList:
                    if n == jobnode['tags']['NodeId']:
                        cnt = 1
                        continue
                if cnt == 1:
                    continue

                jobIDs = j['measurement']
                totalCores = int(j['fields']['NodeList'].split('-')[1])
                
                remainingJobList = jsonJobList[jsonJobList.index(j)+1:]
                
                for jj in remainingJobList:
                    if n == jj['fields']['NodeList'].split('-')[0]:
                        jobIDs += ','+jj['measurement']
                        totalCores += int(jj['fields']['NodeList'].split('-')[1])

                jsonNodeJobList.append({'measurement': 'NodeJobs','tags':{'NodeId':n},'fields':{'JobList':jobIDs},'time':timeStamp})
    # verify(jsonNodeJobList)
    # print("\nXXXX",len(jsonNodeJobList),"XXXX\n")
    return jsonNodeJobList

# def build_node_job_mapping(jsonJobList,timeStamp):
#     jsonNodeJobList = []

#     for j in jsonJobList:
#         if j['fields']['total_nodes'] > 1:
#             nodeAddresses = j['fields']['nodes_address'].split(',')
#             for nodeAddress in nodeAddresses:
#                 l = nodeAddress.split('-')
#                 jsonNodeJobList.append({'measurement': 'node_job_info','tags':{'cluster':'quanah','host':l[0],'location':'ESB'},'fields':{'node':l[0],'jobID':j['measurement'],'CPUCores':int(l[1])},'time':timeStamp})

#         else:
#             cnt = 0
#             n = j['fields']['nodes_address'].split('-')[0]
#             for jobnode in jsonNodeJobList:
#                 if n == jobnode['fields']['node']:
#                     cnt = 1
#                     continue
#             if cnt == 1:
#                 continue

#             jobIDs = j['measurement']
#             totalCores = int(j['fields']['nodes_address'].split('-')[1])
            
#             remainingJobList = jsonJobList[jsonJobList.index(j)+1:]
            
#             for jj in remainingJobList:
#                 if n == jj['fields']['nodes_address'].split('-')[0]:
#                     jobIDs += ','+jj['measurement']
#                     totalCores += int(jj['fields']['nodes_address'].split('-')[1])

#             jsonNodeJobList.append({'measurement': 'node_job_info','tags':{'cluster':'quanah','host':n,'loca\
#             tion':'ESB'},'fields':{'node':n,'jobID':jobIDs,'CPUCores':totalCores},'time':timeStamp})
#     #verify(jsonNodeJobList)
#     return jsonNodeJobList
    
def verify (jsonNodeJobList):
    print ("Total Nodes running jobs:",len(jsonNodeJobList))
    nodes = []
    jobs = []
    for jnjl in jsonNodeJobList:
        if jnjl['tags']['NodeId'] in nodes:
            print ("\n\nDUPLICATED NODE\n\n'")
        else:
            nodes.append(jnjl['tags']['NodeId'])

        jl = jnjl['fields']['JobList'].split(',')
        for i in jl:
            if i not in jobs:
                jobs.append(i)
    print ('Total nodes: ',len(nodes),'\n\n')
    print ('Total jobs: ',len(jobs),'\n\n')

def getJobInfo (job_data,error,json_node_list,error_list,checkType,timeStamp):
    userNames = []
    jobsID = []
    found=0
    jobList = []
    for hostinfo in job_data:
        node = get_hostip(hostinfo['hostname'].split('.')[0])
        jobLoad (hostinfo, node,json_node_list,error_list,checkType,timeStamp)
        for j in hostinfo['jobList']:
            if (j['masterQueue'] == 'MASTER'):
                continue

            jID = 'qu_'+str(j['id'])

            if j['user'] not in userNames:
                userNames.append(j['user'])
            if jID not in jobsID:
                jobsID.append(jID)

            found=0
            for job in jobList:
                if (job['measurement'] == jID):
                    found=1
                    job['fields']['CPUCores'] += 1
                    node_addresses = job['fields']['nodes_address']
                    exists = 0

                    for n in node_addresses:
                        if node in n:
                            exists = 1
                            node_addresses[node_addresses.index(n)] = node+'-'+str(int(n[n.find('-')+1:])+1)
                            
                    if exists == 0:
                        job['fields']['nodes_address'].append(node+'-1')                                          
                        job['fields']['total_nodes']=len(job['fields']['nodes_address'])

            if found == 0:
                jobList.append({'measurement': jID, 'time': timeStamp, 'fields': {'total_nodes':1,'app_name':j['name'],'id':j['id'],'error': 'None', 'submitTime': j['submitTime'], 'nodes_address': [node+'-1'],'user': j['user'], 'state': j['state'], 'startTime': j['startTime'],'CPUCores':1}, 'tags': {'location': 'ESB', 'cluster': 'quanah'}})

    for jj in jobList:
        nl = jj['fields']['nodes_address']
        jj['fields']['nodes_address'] = ','.join(str(n) for n in nl)

    #print (jobList)                                                                                                   
    #for jb in jobList:
    #    print (jb,'\n\n')
    #    print ("\nUser:",jb['fields']['user'],"\tJobDI:",jb['measurement'],"\tNodes:",len(jb['fields']['nodes'].split(',')),"\tCPU Cores:",jb['fields']['CPUCores'])

    if jobList:
        json_node_list += jobList
    if userNames:
        mon_data_dict = build_currentusers_metric(userNames,timeStamp)
        json_node_list.append(mon_data_dict)
        error_list.append(['cluster', checkType, 'None'])
    if jobsID:
        mon_data_dict = build_currentjobsid_metric(jobsID,timeStamp)
        json_node_list.append(mon_data_dict)
        error_list.append(['cluster', checkType, 'None'])

    #Cluster wide Jobs and Nodes power usage storage
    client1 = InfluxDBClient(host='localhost', port=8086)
    client1.switch_database('hpcc_monitoring_db')
    node_total_pwr = calc_currentnode_power(client1)
    job_total_pwr,time = calc_currentjob_power(client1)
     
    #print (node_total_pwr,job_total_pwr,time)

    if node_total_pwr and job_total_pwr:
        mon_data_dict = build_currentjobsnodespwrusage_metric(node_total_pwr,job_total_pwr,time)
        json_node_list.append(mon_data_dict)
        error_list.append(['cluster_power_usage', checkType, 'None'])

    '''                                                                                                              
    for jb in jobList:                                                                                                 
        jb['fields']['nodes'] = ','.join(jb['fields']['nodes'])                                                        
        jb['measurement'] = 'i'+str(jb['measurement'])                                                                 
        json_node_list.append(jb)                                                                                      
    '''
    #json_node_list = json_node_list + jobList

###############################################################################################                        #       Calculate power usage by jobs using current job power data in InfluxDB                                         ############################################################################################### 

def calc_currentjob_power(client):

        # Get the job list                                                                                                                                                                   
        jobs_pwr = {}
        job_pwr = []
        job_p = 0.0
        node_pwr = 0.0
        pwr_proportion_per_core = 0.0
        total_job_power_usage = 0.0
        job_nodes = []
        jobsCores = 0
        jobsJsonList = []
        found = 0
        totalCores =0

        result = client.query("SELECT jobs_list FROM Current_Jobs_ID ORDER BY DESC LIMIT 1;")
        jobs = list(result.get_points())[0]['jobs_list'].split(',')
        time = list(result.get_points())[0]['time']

        for j in jobs:
            res = client.query("SELECT * FROM "+j+" ORDER BY DESC LIMIT 1;")
               
            nodesAddresses = list(res.get_points())[0]['nodes_address']
            cpuCores = list(res.get_points())[0]['CPUCores']
            totalNodes = list(res.get_points())[0]['total_nodes']
            startTime = list(res.get_points())[0]['startTime']
            found = 0                                                                                                                                                                                                                                                                      
            totalCores += cpuCores

            if totalNodes > 1:
                jobsJsonList.append([{'jobID':j,'startTime':startTime,'totalNodes':totalNodes,'cpuCores':cpuCores,'nodesAddresses':nodesAddresses}])
            else:
                for job in jobsJsonList:
                    if job[0]['nodesAddresses'].split('-')[0] == nodesAddresses.split('-')[0]:
                        job.append ({'jobID':j,'startTime':startTime,'totalNodes':totalNodes,'cpuCores':cpuCores,'nodesAddresses':nodesAddresses})
                        found = 1
                if found == 0:
                    jobsJsonList.append([{'jobID':j,'startTime':startTime,'totalNodes':totalNodes,'cpuCores':cpuCores,'nodesAddresses':nodesAddresses}])

         
        for jb in jobsJsonList:                                                                                                                                                          
            #print('\n',len(jb),'---',jb,'\n')
            if len (jb) == 1:
                job_pwr.clear()
                job_p =0.0
                nodes = jb[0]['nodesAddresses'].split(',')
                for node in nodes:
                    host = node.split('-')[0]
                    if host not in job_nodes:
                        job_nodes.append(host)
                    result = client.query("SELECT powerusage_watts FROM Node_Power_Usage where host=" + "'%s'" % host + " ORDER BY DESC LIMIT 1;")
                    node_pwr = list(result.get_points())[0]['powerusage_watts']                                                                                                       
                    job_pwr.append(node_pwr)
                for jp in job_pwr:                                                                         
                    job_p += jp                                                                                                                                                      
                jobs_pwr[jb[0]['jobID']]= job_p
            else:
                jobsCores = 0
                host = jb[0]['nodesAddresses'].split('-')[0]
                if host not in job_nodes:
                    job_nodes.append(host)

                result = client.query("SELECT powerusage_watts FROM Node_Power_Usage where host=" + "'%s'" % host + " ORDER BY DESC LIMIT 1;")                                    
                node_pwr = list(result.get_points())[0]['powerusage_watts'] 
                for j in jb:
                    jobsCores += j['cpuCores']

                pwr_proportion_per_core = round(node_pwr/jobsCores,2)
                
                for j in jb:
                    jobsCores += j['cpuCores']
                    jobs_pwr[j['jobID']]= round((j['cpuCores'] * pwr_proportion_per_core),2)
                    
                    
        #print ('\n TOTAL NODES RUNNING JOBS:',len(job_nodes),'::::::::TOTAL CORES: ', totalCores,'\n')

        '''                    
        total_jobs = len(jobs_pwr)
                
        print ('\nTotal Jobs:',total_jobs)                                                                                                                                                   
        print ('\n\nList of Jobs with Power Usage:\n')                                                                                                                                       
        for jj in jobs_pwr:                                                                                                                                                                  
                print (jj,' : ', jobs_pwr[jj])                                                                                                                                                                                       
        '''

        for jpl in jobs_pwr.values():
                total_job_power_usage += jpl

        #print ("\n\nTotal Power Usage by All JOBS Across Quanah Cluster:",round(total_job_power_usage,2),"Watts",'\n\n')                                                                    
        #idle_nodes_cores(job_nodes)                                                                                                                                                         
        
        return round(total_job_power_usage,2),time
###############################################################################################                                                                                   
# Calculate power usage by nodes using current node power data in InfluxDB                 
###############################################################################################
def calc_currentnode_power(client):
        nodes = 0
        nodes_none = 0
        total_wattages = 0
        pwr = None
        big_query = ""
        bmc_ip_list = []
        pwr_list = []

        # Read list of IP Addresses of BMCs                                                                                                                                                  
        with open ('/home/bmc_iplist.txt','r') as bmc_file:
                bmc_ip_list = json.load(bmc_file)


        for bmc_ip in bmc_ip_list:
                big_query += "SELECT host,powerusage_watts FROM Node_Power_Usage where host=" + "'%s'" % bmc_ip + " ORDER BY DESC LIMIT 1;"

        pwr_res =  client.query(big_query)

        for pwr_ele in pwr_res:
                pwr_dict = list(pwr_ele.get_points())[0]
                if 'powerusage_watts' in pwr_dict:
                        pwr = pwr_dict['powerusage_watts']
                        nodes += 1
                        pwr_list.append(pwr_dict['host']+' : '+str(pwr))
                        #pwr_list.append(pwr)                                                                                                                                                
                        if pwr is not None:
                                total_wattages += pwr
                        else:
                                nodes_none += 1

                                
                                
        '''
                        
        print ('\n******************************Node Power Usage********************************************\n') 
        for p in pwr_list:
            print (p)                                                                                                                                                                                                                                                 
        print ('\nTotal Power Usage by Nodes Across the Quanah Cluster:',total_wattages,'Watts')                                                                                             
        print ('\nTotal Nodes Reporting Power Usage Across the Quanah Cluster:',nodes,'Nodes')                                                                                               
                                                                                                                                                                                             
        print ('\n**********************************Job Power Usage********************************************\n')                                                                     

        #print ('\nTotal Nodes Reporting Power Usage NONE Across the Cluster:',nodes_none)                                                                                                   
        '''
        
        return total_wattages

###############################################################################################                                              
# Build memory usage and CPU usage metrics from job data                                                                                      
###############################################################################################                                               
def jobLoad (hostinfo,host_ip,json_node_list,error_list,checkType,timeStamp):
    
    global prevMetrics
    
    if hostinfo['resourceNumericValues'].get('np_load_avg') != None:
        cpu_usage = hostinfo['resourceNumericValues']['np_load_avg']
        #if bool (prevMetrics):
        # if prevMetrics.get(host_ip+'-'+'CPUUsage'):
        #     if (prevMetrics[host_ip+'-'+'CPUUsage'] != cpu_usage):
        #         prevMetrics[host_ip+'-'+'CPUUsage'] = cpu_usage
        #         mon_data_dict = build_cpu_usage_metric(cpu_usage,host_ip,'None',timeStamp)
        #         json_node_list.append(mon_data_dict)
        #         error_list.append([host_ip, checkType, 'None'])
        # else:
        #     prevMetrics[host_ip+'-'+'CPUUsage'] = cpu_usage
        if cpu_usage != None:
            mon_data_dict = build_cpu_usage_metric(cpu_usage,host_ip,'None',timeStamp)
            json_node_list.append(mon_data_dict)
            error_list.append([host_ip, checkType, 'None'])
        
        
        total_memory =hostinfo['resourceNumericValues']['m_mem_total']
        available_memory=hostinfo['resourceNumericValues']['m_mem_free']
        used_memory = float(float(re.sub("\D","",total_memory)) - float(re.sub("\D","",available_memory)))/1000
        used_memory = round(used_memory/float(total_memory[:-1]),2)
        
        # if bool (prevMetrics):
        # if prevMetrics.get(host_ip+'-'+'MemUsage'):
        #     if (prevMetrics[host_ip+'-'+'MemUsage'] != used_memory):
        #         prevMetrics[host_ip+'-'+'MemUsage'] = used_memory
        #         mon_data_dict = build_memory_usage_metric(total_memory, available_memory,used_memory,host_ip,'None',timeStamp)
        #         json_node_list.append(mon_data_dict)
        #         error_list.append([host_ip, checkType, 'None'])
        # else:
        #     prevMetrics[host_ip+'-'+'MemUsage'] = used_memory
        if used_memory != None:
            mon_data_dict = build_memory_usage_metric(total_memory, available_memory,used_memory,host_ip,'None',timeStamp)
            json_node_list.append(mon_data_dict)
            error_list.append([host_ip, checkType, 'None'])

    else:
        cpu_usage = 0.0
        error = 'No CPU usage data available at UGE!'
        mon_data_dict = build_cpu_usage_metric(cpu_usage,host_ip,error,timeStamp)
        json_node_list.append(mon_data_dict)
        error_list.append([host_ip, checkType, error])

        total_memory = 0.0
        available_memory = 0.0
        used_memory = 0.0
        error = 'No memory usage data available at UGE!'
        mon_data_dict = build_memory_usage_metric(total_memory, available_memory,used_memory,host_ip,error,timeStamp)
        json_node_list.append(mon_data_dict)
        error_list.append([host_ip, checkType, error])

###############################################################################################                                                                  
# Build  metric which store cluster level total node power usage and total power usage                                                                           
###############################################################################################   
def build_currentjobsnodespwrusage_metric(node_total_pwr, job_total_pwr, timeStamp):

    mon_data_dict = {'measurement':'Cluster_Nodes_Jobs_PWR_Usage','tags':{'cluster':'quanah','location':'ESB'},'time':None,'fields':{}}

    mon_data_dict['fields']['cluster_nodes_pwr_usage_watts'] = node_total_pwr
    mon_data_dict['fields']['cluster_jobs_pwr_usage_watts'] = job_total_pwr
    mon_data_dict['time'] = timeStamp

    return mon_data_dict

###############################################################################################                                                                                                                                                                                           
# Build  metric which will include IDs of all jobs currently running                                                               
                                                                                                                                              
###############################################################################################                                              
                                                                                                                                              
def build_currentjobsid_metric(jobs,timeStamp):
    mon_data_dict = {'measurement':'Current_Jobs_ID','tags':{'cluster':'quanah','location':'ESB'},'time':None,'fields':{}}
    #mon_data_dict['fields']['jobs_list'] = ';'.join( json.dumps(job) for job in jobs)                                                                                               
    mon_data_dict['fields']['jobs_list'] = ','.join(jobs)
    mon_data_dict['time'] = timeStamp
    return mon_data_dict

###############################################################################################                                               
# Build current users metric by encapsulating the collection of users currently running jobs into dictionary                                 
###############################################################################################                                              
                                                                                                                                              
def build_currentusers_metric(userNames,timeStamp):
    mon_data_dict = {'measurement':'Current_Users','tags':{'cluster':'quanah','location':'ESB'},'time':None,'fields':{}}
    #mon_data_dict['fields']['users_name_list'] = ','.join(userNames)                                                                                                                
    mon_data_dict['fields']['users_name_list'] = ','.join(userNames)
    mon_data_dict['time'] = timeStamp
    return mon_data_dict


###############################################################################################                                                                   
# Build job info   metric by encapsulating the jobs associated with a node into dictionary                                                                       
###############################################################################################

'''

def build_job_info_metric(job_info,host,error):
    mon_data_dict = {'measurement':'Job_Info','tags':{'cluster':'quanah','location':'ESB'},'time':None,'fields':{}}
    mon_data_dict['fields']['job_data'] = job_info 
    mon_data_dict['fields']['error'] =error
    mon_data_dict['time'] = datetime.datetime.now().isoformat()
    mon_data_dict['tags']['host'] = host
    return mon_data_dict

'''

###############################################################################################                                                                   
# Builds host power state  metric by encapsulating the BMC health monitoring data into dictionary                                                              
###############################################################################################
            
def build_power_state_metric(metricTimeStamp,host_power_state,tot_time,host,retry,error):
    mon_data_dict = {'measurement':'HealthMetrics','tags':{'Sensor':'PowerState','NodeId': host},'time':metricTimeStamp,'fields':{}}
    mon_data_dict['fields']['Reading'] = host_power_state
    return mon_data_dict
    # mon_data_dict = {'measurement':'Node_Power_State','tags':{'cluster':'quanah','host':host,'location':'ESB'},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    # mon_data_dict['fields']['power_state'] = host_power_state
    # mon_data_dict['fields']['retry'] = retry
    # mon_data_dict['fields']['error'] = error
    # mon_data_dict['time'] = metricTimeStamp
    # return mon_data_dict
    


###############################################################################################                                                                   
# Builds host LED indicator metric by encapsulating the BMC health monitoring data into dictionary                                                                
###############################################################################################                                                                                                                                                                                                                                 

def build_led_indicator_metric(metricTimeStamp,host_led_indicator,tot_time,host,retry,error):
    mon_data_dict = {'measurement':'HealthMetrics','tags':{'Sensor':'IndicatorLEDStatus','NodeId': host},'time':metricTimeStamp,'fields':{}}
    mon_data_dict['fields']['Reading'] = host_led_indicator
    return mon_data_dict
    # mon_data_dict = {'measurement':'Node_LED_Indicator','tags':{'cluster':'quanah','host':host,'location':'ESB'},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    # mon_data_dict['fields']['led_indicator'] = host_led_indicator
    # mon_data_dict['fields']['retry'] = retry
    # mon_data_dict['fields']['error'] =error
    # mon_data_dict['time'] = metricTimeStamp
    # return mon_data_dict


###############################################################################################                                                                   
# Builds BMC health metric by encapsulating the BMC health monitoring data into dictionary                                                                        
###############################################################################################
 
def build_bmc_health_metric(metricTimeStamp,bmc_health,tot_time,host,retry,error):
    mon_data_dict = {'measurement':'HealthMetrics','tags':{'Sensor':'BMCHealth','NodeId': host},'time':metricTimeStamp,'fields':{}}
    mon_data_dict['fields']['Reading'] = bmc_health
    return mon_data_dict
    # mon_data_dict = {'measurement':'BMC_Health','tags':{'cluster':'quanah','host':host,'location':'ESB'},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    # mon_data_dict['fields']['bmc_health_status'] = bmc_health
    # mon_data_dict['fields']['retry'] = retry
    # mon_data_dict['fields']['error'] =error
    # mon_data_dict['time'] = metricTimeStamp
    # return mon_data_dict

###############################################################################################                                                                   
# Builds inlet sensor health metric by encapsulating the inlet health monitoring data into dictionary                                                         
###############################################################################################

def build_inlethealth_metric(metricTimeStamp,inlet_health,tot_time,host,retry,error):
    mon_data_dict = {'measurement':'HealthMetrics','tags':{'Sensor':'InletHealth','NodeId': host},'time':metricTimeStamp,'fields':{}}
    mon_data_dict['fields']['Reading'] = inlet_health
    return mon_data_dict
    # mon_data_dict = {'measurement':'Inlet_Health','tags':{'cluster':'quanah','host':host,'location':'ESB'},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    # mon_data_dict['fields']['inlet_health_status'] = inlet_health
    # mon_data_dict['fields']['retry'] = retry
    # mon_data_dict['fields']['error'] =error
    # mon_data_dict['time'] = metricTimeStamp
    # return mon_data_dict


###############################################################################################                                                                   
# Builds Host health metric by encapsulating the Host health monitoring data into dictionary                                                                     
############################################################################################### 

def build_host_health_metric(metricTimeStamp,host_health,tot_time,host,retry,error):
    mon_data_dict = {'measurement':'HealthMetrics','tags':{'Sensor':'NodeHealth','NodeId': host},'time':metricTimeStamp,'fields':{}}
    mon_data_dict['fields']['Reading'] = host_health
    return mon_data_dict
    # mon_data_dict = {'measurement':'Node_Health','tags':{'cluster':'quanah','host':host,'location':'ESB'},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    # mon_data_dict['fields']['host_health_status'] = host_health
    # mon_data_dict['fields']['retry'] = retry
    # mon_data_dict['fields']['error'] =error
    # mon_data_dict['time'] = metricTimeStamp
    # return mon_data_dict

###############################################################################################                                                                   
# Builds CPU health metric by encapsulating the CPU health monitoring data into dictionary                                                                        
############################################################################################### 

def build_cpu_health_metric(metricTimeStamp,cpu_health,tot_time,host,retry,error):
    mon_data_dict = {'measurement':'HealthMetrics','tags':{'Sensor':'CPUHealth','NodeId': host},'time':metricTimeStamp,'fields':{}}
    mon_data_dict['fields']['Reading'] = cpu_health
    return mon_data_dict
    # mon_data_dict = {'measurement':'CPU_Health','tags':{'cluster':'quanah','host':host,'location':'ESB'},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    # mon_data_dict['fields']['cpu_health_status'] = cpu_health
    # mon_data_dict['fields']['retry'] = retry
    # mon_data_dict['fields']['error'] =error
    # mon_data_dict['time'] = metricTimeStamp
    # return mon_data_dict

###############################################################################################                                                                                                               
# Builds MEMORY health metric by encapsulating the MEMORY health monitoring data into dictionary                                                                                                              
############################################################################################### 

def build_mem_health_metric(metricTimeStamp,mem_health,tot_time,host,retry,error):
    # mon_data_dict = {'measurement':'Memory_Health','tags':{'cluster':'quanah','host':host,'location':'ESB'},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    # mon_data_dict['fields']['memory_health_status'] = mem_health
    # mon_data_dict['fields']['retry'] = retry
    # mon_data_dict['fields']['error'] =error
    # mon_data_dict['time'] = metricTimeStamp
    # return mon_data_dict
    mon_data_dict = {'measurement':'HealthMetrics','tags':{'Sensor':'MemHealth','NodeId': host},'time':metricTimeStamp,'fields':{}}
    mon_data_dict['fields']['Reading'] = mem_health
    return mon_data_dict

###############################################################################################                                                                                                               
# Builds CPU usage metric by encapsulating the CPU usage data into dictionary                                                                                                                    
############################################################################################### 

def build_cpu_usage_metric(cpu_usage,host,error,timeStamp):
    mon_data_dict = {'measurement':'UGE','tags':{'Sensor':'CPUUsage','NodeId': host},'time':None,'fields':{}}
    mon_data_dict['fields']['Reading'] = round(cpu_usage,2)
    mon_data_dict['time'] = timeStamp
    return mon_data_dict

###############################################################################################                                                                                                               
# Builds memory usage metric by encapsulating the memory usage data into dictionary                                                                                                                    
############################################################################################### 

# def build_memory_usage_metric(total_memory, available_memory,used_memory,host,error,timeStamp):
#     mon_data_dict = {'measurement':'Memory_Usage','tags':{'cluster':'quanah','location':'ESB'},'time':None,'fields':{}}
#     mon_data_dict['fields']['total_memory'] = str(total_memory)
#     mon_data_dict['fields']['available_memory'] = str(available_memory)
#     mon_data_dict['fields']['memoryusage'] = used_memory
#     mon_data_dict['fields']['error'] =error
#     mon_data_dict['time'] = timeStamp
#     #host_ip = get_hostip(hostname,host)
#     mon_data_dict['tags']['host'] = host
#     return mon_data_dict

def build_memory_usage_metric(total_memory, available_memory,used_memory,host,error,timeStamp):
    mon_data_dict = {'measurement':'UGE','tags':{'Sensor':'MemUsage','NodeId': host},'time':timeStamp,'fields':{}}
    mon_data_dict['fields']['Reading'] = used_memory
    return mon_data_dict

###############################################################################################                                                                                                               
# UGE only knows about host name so this helper function find host ip corresponding host name                                                                                                                 
############################################################################################### 

def get_hostip(hostname):
    
    '''
    for h in host:
        h1,h2,h3,h4=h.split('.')
        if h3+'-'+h4 in hostname:
            return h
    '''
    if '-' in hostname:
        n,h2,h1 = hostname.split('-')
        return '10.101.'+h2+'.'+h1
    return None

###############################################################################################                                                                                                               
# Builds fan speed metric by encapsulating the fan speed in RPM and other infos into dictionary                                                                                                               
############################################################################################### 

def build_fanspeed_metric(metricTimeStamp,fankey,val,host):
    mon_data_dict = {'measurement':'Thermal','tags':{'Sensor':fankey+"Speed",'NodeId':host},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    
    # if fan_speed != None:
    #     fankeys = fan_speed.keys()
    #     fanvals = fan_speed.values()
    #     for k,v in zip(fankeys,fanvals):
    #         mon_data_dict['fields'][k] = v
    

    # mon_data_dict['fields']['retry'] = retry
    
    # if fan_speed_thresholds != None:
    #     mon_data_dict['fields']['fanLowerThresholdCritical'] = fan_speed_thresholds['fanLowerThresholdCritical']
    #     #mon_data_dict['fields']['fanLowerThresholdNonCritical'] = fan_speed_thresholds['fanLowerThresholdNonCritical']
    #     mon_data_dict['fields']['fanUpperThresholdCritical'] = fan_speed_thresholds['fanUpperThresholdCritical']
    #     #mon_data_dict['fields']['fanUpperThresholdNonCritical'] = fan_speed_thresholds['fanUpperThresholdNonCritical']
    #     mon_data_dict['fields']['fanLowerThresholdNonCritical'] = 'None'
    #     mon_data_dict['fields']['fanUpperThresholdNonCritical'] = 'None'
    mon_data_dict['fields']['Reading'] = val
    mon_data_dict['time'] = metricTimeStamp

    return mon_data_dict
 
###############################################################################################                                                                                                               
# Builds fan health metric by encapsulating the fan health and other infos into dictionary                                                                                                              
###############################################################################################  

def build_fanhealth_metric(metricTimeStamp,fan_key, fan_health_status,tot_time,host,retry,error):
    
    mon_data_dict = {'measurement':'HealthMetrics','tags':{'Sensor':fan_key+"Health",'NodeId': host},'time':metricTimeStamp,'fields':{'Reading':fan_health_status}}
    return mon_data_dict

    # mon_data_dict = {'measurement':'Fan_Health','tags':{'cluster':'quanah','host':host,'location':'ESB'},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)

    # if fan_health != None:
    #     fankeys = fan_health.keys()
    #     fanvals = fan_health.values()
    #     for k,v in zip(fankeys,fanvals):
    #         mon_data_dict['fields'][k] = v

    # mon_data_dict['fields']['error'] =error
    # mon_data_dict['fields']['retry'] = retry
    # mon_data_dict['time'] = metricTimeStamp
    # return mon_data_dict

###############################################################################################                                                                                                               
# Builds cpu temperature metric by encapsulating the cpu temperature and other infos into dictionary                                                                                                          
###############################################################################################  

def build_cpu_temperature_metric(metricTimeStamp,cpukey,tempval, host):
    cpukey = cpukey.split(" ")
    cpukey = "".join(cpukey)

    mon_data_dict = {'measurement':'Thermal','tags':{'Sensor':cpukey,'NodeId':host},'time':None,'fields':{}}
    # mon_data_dict['fields']['GET_processing_time'] = round(tot_time,2)
    # if cpu_temperature != None:
    #     cpukeys = cpu_temperature.keys()
    #     cpuvals = cpu_temperature.values()
    #     for (k,v) in zip(cpukeys, cpuvals):
    #         mon_data_dict['fields'][k] = v

    # if cpu_temp_thresholds != None:
    #     mon_data_dict['fields']['cpuLowerThresholdCritical'] = cpu_temp_thresholds['cpuLowerThresholdCritical'] 
    #     mon_data_dict['fields']['cpuLowerThresholdNonCritical'] = cpu_temp_thresholds['cpuLowerThresholdNonCritical']
    #     mon_data_dict['fields']['cpuUpperThresholdCritical'] = cpu_temp_thresholds['cpuUpperThresholdCritical']
    #     mon_data_dict['fields']['cpuUpperThresholdNonCritical'] = cpu_temp_thresholds['cpuUpperThresholdNonCritical']

    # mon_data_dict['fields']['retry'] = retry
    mon_data_dict['fields']['Reading'] = tempval
    mon_data_dict['time'] = metricTimeStamp
    return mon_data_dict


###############################################################################################                                                                                             
# Inlet temperature metric by encapsulating the inlet temperature and other infos into dictionary                                                                                       
###############################################################################################                                                                                               

def build_inlet_temperature_metric(metricTimeStamp,inlet_key,inlet_val, host):
    inlet_key = inlet_key.split(" ")
    inlet_key = "".join(inlet_key)
    mon_data_dict = {'measurement':'Thermal','tags':{'Sensor':inlet_key,'NodeId':host,},'time':None,'fields':{}}
    
    mon_data_dict['fields']['Reading'] = inlet_val
    mon_data_dict['time'] = metricTimeStamp
    return mon_data_dict

       
###############################################################################################                                                                                                               
# Builds power usages in watts metric by encapsulating the power usage and other infos into dictionary                                                                                                        
############################################################################################### 

def build_power_usage_metric(metricTimeStamp,power_usage,host):
    mon_data_dict = {'measurement':'Power','tags':{'Sensor':'NodePower','NodeId':host},'time':None,'fields':{}}
    mon_data_dict['fields']['Reading'] = power_usage
    mon_data_dict['time'] = metricTimeStamp
    return mon_data_dict
    

###########################################################################################                                                                                                                   
# The following function represents each core and creates threads according to  tasks     #                                                                                                                   
# assigned to it. Approximately, 58 threads are created on each core. This implements     #                                                                                                                  
# multithreading within multiprocessing that achieves the optimal performance.            #                                                                                                                  
# Each thread calls getNodesData function passes the host and corresponding check,        #
# json_node_list, error_list, and session object.                                                                                     
###########################################################################################
def core_to_threads (input_data,session,ts):
    
    warnings.filterwarnings('ignore', '.*', UserWarning,'warnings_filtering',)
    try:
        
        error_list = []
        json_node_list = []
        threads = []   
        thread_id = 0
        for host_info in input_data:
            host = host_info[0]
            checkType = host_info[1]
            
            a = Thread(target = getNodesData, args=(host, checkType, json_node_list, error_list,session, ts,))
            threads.append(a)
            threads[thread_id].start()
            thread_id += 1

        for index in range (0, thread_id):
            threads[index].join()

        return json_node_list, error_list
        
    except Exception as e:
        #error_list.append([host, e])
        #return json_node_list, error_list
        return None,e
###########################################################################################
# The following function accepts tasklist as input_data and session objects. This function#
# creates a pool of "cores" and divide the workload among cores nearly even. In our case, #
# we have 467 iDracs and power check. On 8 core machine, first 7 cores will have 58 tasks #
# each and last one (8th) will have 61. The remainder will be added to last core.         #
###########################################################################################

def  parallelizeTasks (input_data,session,ts):

    warnings.filterwarnings('ignore', '.*', UserWarning,'warnings_filtering',)
    try:
        #nodes = len(input_data)
        tasks = len(input_data)
        if (tasks == 0):
            print ("\nThere is no task for monitoring!\n")
            return [], []
        
        node_error_list = []
        node_json_list = []
       
        #########################################################################
        
        # Initialize pool of cores
        cores = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(cores)
        
        if( tasks < cores ):
            cores = tasks
        
        # Build job list
        jobs = []

        tasks_per_core = tasks // cores
        surplus_tasks = tasks % cores

        
        #print (input_data)
        increment=1
        for p in range (cores):
            if (surplus_tasks != 0 and p == (cores-1)):
                jobs.insert(p, input_data[p*tasks_per_core:])
            else:
                jobs.insert(p, input_data[p*tasks_per_core:increment*tasks_per_core])
                increment+=1
        
        print("Monitoring %d tasks using %d cores..." % \
              (tasks, cores) )

        #print (len(jobs),jobs)
        # Run parallel jobs across all the cores by calling core_to_threads
        results = [pool.apply_async( core_to_threads, args=(j,session,ts,) ) for j in jobs]

        # Process results
        for result in results:
            (node_data, node_error) = result.get()
            #node_json_list.append(node_data)
            #node_error_list.append(node_error)
            node_json_list += node_data
            node_error_list += node_error
        pool.close()
        pool.join()

        #########################################################################         

        return node_json_list, node_error_list
    except Exception as e:
        node_error_list.append([e])
        return node_json_list, node_error_list

userName = ""
passwd = ""
prevMetrics = {}

def main():
    
    # List of hard coded IP addresses of iDracs (13G)

    hostList = []
    bmcCred = []

    with open('/home/bmc_iplist.txt','r') as bmc_file:
        hostList=json.load(bmc_file)

    # Read BMC Credentials:
    with open('/home/bmc_cred.txt','r') as bmc_cred:
        bmcCred = json.load(bmc_cred)
        
    global userName
    global passwd
    # global prevMetrics

    userName = bmcCred[0]
    passwd = bmcCred[1]

    
    # fName = '/home/production/prevmetrics'
    # if os.path.exists(fName):
	#     with open(fName) as infile:
	# 	    prevMetrics = json.load(infile)
            
    #The following is list of IP address of known problematic BMCs which are under maintenance and excluded from montioring:
    # KnownProblematicBMCs = []
    # This tool shares the session for DIFFERENT Redfish API calls and DIFFERENT iDracs
    session = requests.Session()
    
    # For M number of hosts with N checks/metrics per host will have MXN tasks
    taskList = []

    #The following is the list of high level checks i.e. some checks will be divided into sub-checks e.g. HostHealthRollup consists of three health metric: Host Health, CPU health, and Memory Health
    # Also note that 'HPCJob' check is not part of iDRAC rather it uses Univa Grid Engine (UGE) REST API to enquire the job related metrics running in the HPC 
    
    #REMOVEME
    
    # checkList = ['Power']

    # hostList = ['10.101.10.25']
    # For the purpose of this testing, I have excluded the HPCJob metric:
    # checkList = ['SystemHealth','BMCHealth','Thermal','Power']
    
    checkList = ['Thermal','SystemHealth','BMCHealth','HPCJob','MEMPWR','CPUPWR']
    
    '''
    # Checks are iterated 100 times across the TTU HPCC Quanah cluster (467 nodes)
    for iteration in range(1):
        print ("\n\nIteration:",iteration+1)
        # Launcher accepts list of hosts, list of checks, http session objects, and iteration id
        launch(hostList,checkList, taskList,session,iteration)
    '''
     # each check is combined with each host. TaskList is nothing but a list of sublists of host and check
    
    startTime = time.time()
    for check in checkList:
        # as HPCJob check is not part of iDRAC so it will be considered single task
        if check == 'HPCJob':
            taskList.append([hostList,check])
            continue
        elif check == 'MEMPWR' or check == 'CPUPWR':
            hlist = ['10.100.10.25','10.100.10.26','10.100.10.27','10.100.10.28']
            for h in hlist:
                taskList.append([h,check])        
            continue
        for host in hostList:
            taskList.append([host,check])
    
    launch (taskList,session,startTime,hostList)   

def launch (taskList,session,startTime,hostList):    
#def launch(hostList,checkList, taskList,session,iteration):
    #ts = datetime.now() + timedelta(seconds=5)
    #ts = ts.isoformat()
    ts = int(datetime.now().timestamp())
    


    '''
    # each check is combined with each host. TaskList is nothing but a list of sublists of host and check
    for check in checkList:
        # starting time of a check
        startTime = time.time()
        # TESTING START
        print ("\n\n ****** Check: ", check,"*****\n\n")
        # TESTING END
        
        for host in hostList:
            # as HPCJob check is not part of iDRAC so it will be considered single task
            if check == 'HPCJob':
                taskList.append([hostList,check])
                break
            taskList.append([host,check])
    '''

        #The tasklist and session object is passed to the following function which returns list of hosts monitoring data and errors
    objList, error_list =  parallelizeTasks(taskList,session,ts)
    savePrevMets()
    #print("\nstart cluster metric\n")
    #print (objList)
    #print("\nstart cluster metric\n") 
    #jsonObjList = build_cluster_metric (objList,hostList,ts)
    # for obj in objList:
    # #    if obj["measurement"] == "Power" or obj["measurement"] == "Thermal":
    #     print (obj)
    #     print("\n")

    # for err in error_list:
    # #    if obj["measurement"] == "Power" or obj["measurement"] == "Thermal":
    #     print (err)
    #     print("\n")
    
    #print ("\n\n LOG :: Total Metrics:",len(objList))
    # jsonObjList += objList
    #jsonObjList = objList
         
        
        # Log of (sheets) responses is created for all checks except HPCJob
    '''
    if check != 'HPCJob':
        log_job_response(jsonObjList,str(iteration)+'Iteration',error_list,check)
    '''
        #else:
            #print (jsonObjList)
        #print ("len:",len(jsonObjList))
        #taskList.clear()
        #print(jsonObjList)
        #print (error_list)
    
       
    # PUSH DATA TO NAGIOS IN PASSIVE MODE
    nagios_external_agent(objList, error_list)
    
    
    #TESTING START
        
    print("\n Total Time in executing total tasks: ",len(taskList), " Output: ", len(objList),"is: "," %s Seconds " % round((time.time() - startTime),2))
    #print ("\n\nTotal measures: ",len(jsonObjList))
    
        #Power Usage by nodes across cluster:
        #calc_currentnode_power(jsonObjList)

        # Power Usage by Jobs across Cluster
        #calc_currentjob_power(jsonObjList)

        # clear the task list
    taskList.clear()
    
        
        
    '''
        # No of failed requests
        failed_reqs=0
        for error in error_list:
            if error[2] != 'None':
                failed_reqs += 1
                #print('\n')
                #print (err[0] + '-' + err[1])
                #print (error[2])
        print ("\nTotal failed requests:",failed_reqs)
        print('\n\n')                                                                                                                                                                                                 
    '''                                                         
        
        #TESTING END

    '''
        print('\n\n')
        failed_req=0
        for jsonObj in jsonObjList:
        if jsonObj['fields']['health_status'] == None:
        failed_req += 1
        #print('\n')
        #print (jsonObj['fields']['host'] + '-' + jsonObj['measurement'])
        
        print ("\nTotal failed requests:",failed_req)        
    '''
        
    '''
        print('\n\n*** EXECUTION TIME of Successful Requests ***\n\n')
        for jsonObj in jsonObjList:
        if jsonObj['fields']['health_status'] != None:
        print('\n')
        print (jsonObj['fields']['host'] + '-' + jsonObj['measurement'] + ' : ' + str(jsonObj['fields']['GET_processing_time']))
    
    '''
        
    '''
        print('\n\n*** EXECUTION TIME of Failed Requests ***\n\n')
        for jsonObj in jsonObjList:
        if jsonObj['fields']['health_status'] == None:
        print('\n')
        print(jsonObj['fields']['host'] + '-' + jsonObj['measurement'] + ' : ' + str(jsonObj['fields']['GET_processing_time']))
    
    '''

            
        
        
                                                                                                
        # storing results in InfluxDBClient                                                                                                                           
    client = InfluxDBClient(host='localhost', port=8086)
    #client.drop_database('test_quanah_db')
    client.drop_database('newtest_quanah_db')
    client.create_database('newtest_quanah_db')                                                                                                        

    #client.switch_database('hpcc_monitoring_db')
    client.switch_database('newtest_quanah_db')
                                                                                                                                               
    client.write_points(objList,time_precision='ms')                                                                                                                              
        
        

        #result = client.query("SELECT * FROM cpu_temperature WHERE host='10.101.1.1' ORDER BY DESC LIMIT 1;")                                                                                                                                                                                                                                                                                                           
        #print("\nResult: {}".format(result))   
        #print (jsonObjList)                                                                                                                                          
        #results = client.query('SELECT "health_status" FROM "hpccMonDB"."autogen"."HostHealth" WHERE "health_status"=0')                                             
        #print(results.raw)                                                                                                                                           
        #print (results)                                                                                                                                              
        #result = client.query('select health_status,host from HostHealth where health_status=0;')                                                                    
        #print("\nResult: {}".format(result))                                                                                           
        
        
    '''

        result = client.query('select health_status,host from HostHealth;')                                                                                           
        print("\nResult: {}".format(result))                                                                                                                          
        points = list(result.get_points(tags={'cluster':'quanah'}))                                                                                                   
        print(points)                                                                                                                                                 
    '''

        
    '''
        print ('\n\n *** Host Health Metric ***')
        #result = client.query('select health_status,host from HostHealth;')                                                                                          
        #print(list(result.get_points()))                                                                                                                             
        #result = client.query('SELECT LAST(health_status), * from HostHealth GROUP BY *;')                                                                           

        result = client.query('SELECT health_status FROM HostHealth ORDER BY DESC LIMIT 1;')
        print("\nResult: {}".format(result))
        
        print ('\n\n *** Host BMC Health Metric ***')
        result = client.query('select health_status,host from BMCHealth;')
        print("\nResult: {}".format(result))

        print ('\n\n *** Host CPU Health Metric ***')
        result = client.query('select health_status,host from CPUHealth;')
        print("\nResult: {}".format(result))

        print ('\n\n *** Host Memory Metric ***')
        result = client.query('select health_status,host from MemoryHealth;')
        print("\nResult: {}".format(result))

        #print (error_list)                                                                                                                                           
        print("\n\n--- Total Execution Time: %s seconds ---" % (time.time() - startTime))
        #nodes_data, error_list =  getNodeData(input_data)                                                                                                            
    '''

def savePrevMets():
    global prevMetrics
    #print("\n Total Saved data points: ", len(prevMetrics))
    fName = '/home/production/prevmetrics'
    with open(fName, 'w') as outfile:
        json.dump(prevMetrics, outfile)

def build_cluster_metric (objList,hostList,ts):
    #mon_data_dict = {'measurement':'cluster_unified_metrics','tags':{'cluster':'quanah','host':None,'location':'ESB'},'time':ts,'fields':{'power_state':None,'cluster_jobs_pwr_usage_watts':None,'cluster_nodes_pwr_usage_watts':None,'jobID':None,'CPUCores':None,'led_indicator':None,'bmc_health_status':None,'inlet_health_status':None,'host_health_status':None,'cpu_health_status':None,'memory_health_status':None,'cpuusage':None,'memoryusage':None,'fan1_health':None,'fan2_health':None,'fan3_health':None,'fan4_health':None,'fan1_speed':None,'fan2_speed':None,'fan3_speed':None,'fan4_speed':None,'CPU1_temp':None,'CPU2_temp':None,'inlet_health':None,'inlet_temp':None,'powerusage_watts':None}}
    jsonObjSystemMetricList = []
                
    for host in hostList:
        mon_data_dict = {'measurement':'cluster_unified_metrics','tags':{'host':None},'time':ts,'fields':{'power_state':None,'jobID':None,'CPUCores':None,'led_indicator':None,'bmc_health_status':None,'inlet_health_status':None,'host_health_status':None,'cpu_health_status':None,'memory_health_status':None,'cpuusage':None,'memoryusage':None,'fan1_health':None,'fan2_health':None,'fan3_health':None,'fan4_health':None,'fan1_speed':None,'fan2_speed':None,'fan3_speed':None,'fan4_speed':None,'CPU1_temp':None,'CPU2_temp':None,'inlet_health':None,'inlet_temp':None,'powerusage_watts':None,'CPUAveragePowerUsage':None,'CPUCurrentPowerUsage':None,'CPUMinPowerUsage':None,'CPUMaxPowerUsage':None,'MemoryAveragePowerUsage':None,'MemoryCurrentPowerUsage':None,'MemoryMinPowerUsage':None,'MemoryMaxPowerUsage':None}}
        for obj in objList:
            if 'host'not in obj['tags']:
                continue
            if obj['tags']['host'] == host:
                mon_data_dict['tags']['host'] = host
                if obj['measurement'] == 'node_job_info':
                    #mon_data_dict['tags']['host'] = host
                    mon_data_dict['fields']['jobID'] = obj['fields']['jobID']
                    mon_data_dict['fields']['CPUCores'] = obj['fields']['CPUCores']
                    
                elif obj['measurement'] == 'Cluster_Nodes_Jobs_PWR_Usage':
                    mon_data_dict['fields']['cluster_nodes_pwr_usage_watts'] = obj['fields']['cluster_nodes_pwr_usage_watts']
                    mon_data_dict['fields']['cluster_jobs_pwr_usage_watts'] = obj['fields']['cluster_jobs_pwr_usage_watts']
                elif obj['measurement'] == 'Node_Power_State':
                    mon_data_dict['fields']['power_state'] = obj['fields']['power_state']
                elif obj['measurement'] == 'Node_LED_Indicator':
                    mon_data_dict['fields']['led_indicator'] = obj['fields']['led_indicator']    
                elif obj['measurement'] == 'BMC_Health':
                    mon_data_dict['fields']['bmc_health_status'] = obj['fields']['bmc_health_status']
                elif obj['measurement'] == 'Inlet_Health':
                    mon_data_dict['fields']['inlet_health_status'] = obj['fields']['inlet_health_status']
                elif obj['measurement'] == 'Node_Health':
                    mon_data_dict['fields']['host_health_status'] = obj['fields']['host_health_status']
                elif obj['measurement'] == 'CPU_Health':
                    mon_data_dict['fields']['cpu_health_status'] = obj['fields']['cpu_health_status']
                elif obj['measurement'] == 'Memory_Health':
                    mon_data_dict['fields']['memory_health_status'] = obj['fields']['memory_health_status']
                elif obj['measurement'] == 'CPU_Usage':
                    mon_data_dict['fields']['cpuusage'] = obj['fields']['cpuusage']
                elif obj['measurement'] == 'Memory_Usage':
                    mon_data_dict['fields']['memoryusage'] = obj['fields']['memoryusage']
                elif obj['measurement'] == 'Fan_Speed':
                     if 'FAN_1' in obj['fields']:
                         mon_data_dict['fields']['fan1_speed'] = obj['fields']['FAN_1']
                         mon_data_dict['fields']['fan2_speed'] = obj['fields']['FAN_2']
                         mon_data_dict['fields']['fan3_speed'] = obj['fields']['FAN_3']
                         mon_data_dict['fields']['fan4_speed'] = obj['fields']['FAN_4']
                     else:
                         mon_data_dict['fields']['fan1_speed'] = 0
                         mon_data_dict['fields']['fan2_speed'] = 0
                         mon_data_dict['fields']['fan3_speed'] = 0
                         mon_data_dict['fields']['fan4_speed'] = 0
                elif obj['measurement'] == 'Fan_Health':
                    if 'FAN_1' in obj['fields']:
                        mon_data_dict['fields']['fan1_health'] = str(obj['fields']['FAN_1'])
                        mon_data_dict['fields']['fan2_health'] = str(obj['fields']['FAN_2'])
                        mon_data_dict['fields']['fan3_health'] = str(obj['fields']['FAN_3'])
                        mon_data_dict['fields']['fan4_health'] = str(obj['fields']['FAN_4'])
                    else:
                        mon_data_dict['fields']['fan1_health'] = '0'
                        mon_data_dict['fields']['fan2_health'] = '0'
                        mon_data_dict['fields']['fan3_health'] = '0'
                        mon_data_dict['fields']['fan4_health'] = '0'
                elif obj['measurement'] == 'CPU_Temperature':
                    if 'CPU1 Temp' in obj['fields']:
                        mon_data_dict['fields']['CPU1_temp'] = obj['fields']['CPU1 Temp']
                        mon_data_dict['fields']['CPU2_temp'] = obj['fields']['CPU2 Temp']
                    else:
                        mon_data_dict['fields']['CPU1_temp'] = 0
                        mon_data_dict['fields']['CPU2_temp'] = 0
                elif obj['measurement'] == 'Inlet_Temperature':
                    if 'Inlet Temp' in obj['fields']:
                        mon_data_dict['fields']['inlet_temp'] = obj['fields']['Inlet Temp']
                    else:
                        mon_data_dict['fields']['inlet_temp'] = 0
                elif obj['measurement'] == 'Node_Power_Usage':
                    mon_data_dict['fields']['powerusage_watts'] = obj['fields']['powerusage_watts']
                elif obj['measurement'] == 'CPU_Power_Usage':
                    mon_data_dict['fields']['CPUAveragePowerUsage'] = obj['fields']['CPUAveragePowerUsage']
                    mon_data_dict['fields']['CPUCurrentPowerUsage'] = obj['fields']['CPUCurrentPowerUsage']
                    mon_data_dict['fields']['CPUMinPowerUsage'] = obj['fields']['CPUMinPowerUsage']
                    mon_data_dict['fields']['CPUMaxPowerUsage'] = obj['fields']['CPUMaxPowerUsage']
                elif obj['measurement'] == 'Memory_Power_Usage':
                    mon_data_dict['fields']['MemoryAveragePowerUsage'] = obj['fields']['MemoryAveragePowerUsage']
                    mon_data_dict['fields']['MemoryCurrentPowerUsage'] = obj['fields']['MemoryCurrentPowerUsage']
                    mon_data_dict['fields']['MemoryMinPowerUsage'] = obj['fields']['MemoryMinPowerUsage']
                    mon_data_dict['fields']['MemoryMaxPowerUsage'] = obj['fields']['MemoryMaxPowerUsage']

        jsonObjSystemMetricList.append( mon_data_dict)
    #print ('\nSystem Metrics: ',len(jsonObjSystemMetricList))
    writeDF(jsonObjSystemMetricList)
    return jsonObjSystemMetricList

def  writeDF(jsonObjSystemMetricList):
    
    '''
    header = []
    header.append('time')
    for j in jsonObjSystemMetricList:
        if j['tags']['host'] != None:
            header.append (j['tags']['host']+'-jobID')
            header.append (j['tags']['host']+'-CPUCores')
            header.append (j['tags']['host']+'-cpuusage')
            header.append (j['tags']['host']+'-memoryusage')
            header.append (j['tags']['host']+'-CPU1_temp')
            header.append (j['tags']['host']+'-CPU2_temp')
            header.append (j['tags']['host']+'-inlet_temp')
            header.append (j['tags']['host']+'-powerusage_watts')
            header.append (j['tags']['host']+'-fan1_speed')
            header.append (j['tags']['host']+'-fan2_speed')
            header.append (j['tags']['host']+'-fan3_speed')
            header.append (j['tags']['host']+'-fan4_speed')
            header.append (j['tags']['host']+'-host_health_status')
            header.append (j['tags']['host']+'-power_state')
            header.append (j['tags']['host']+'-bmc_health_status')
            header.append (j['tags']['host']+'-led_indicator')
            header.append (j['tags']['host']+'-inlet_health_status')
            header.append (j['tags']['host']+'-fan1_health')
            header.append (j['tags']['host']+'-fan2_health')
            header.append (j['tags']['host']+'-fan3_health')
            header.append (j['tags']['host']+'-fan4_health')
            header.append (j['tags']['host']+'-memory_health_status')
            header.append (j['tags']['host']+'-cpu_health_status')
    with open(r'system_metrics.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)     
    f.close()
    '''
    
    field_vals = []
    field_vals.append (jsonObjSystemMetricList[0]['time'])
    for j in jsonObjSystemMetricList:
        if j['tags']['host'] != None:
            #field_vals.append (j['time'])
            field_vals.append (j['fields']['jobID'])
            field_vals.append (j['fields']['CPUCores'])
            field_vals.append (j['fields']['cpuusage'])
            field_vals.append (j['fields']['memoryusage'])
            field_vals.append (j['fields']['CPU1_temp'])
            field_vals.append (j['fields']['CPU2_temp'])
            field_vals.append (j['fields']['inlet_temp'])
            field_vals.append (j['fields']['powerusage_watts'])
            field_vals.append (j['fields']['fan1_speed'])
            field_vals.append (j['fields']['fan2_speed'])
            field_vals.append (j['fields']['fan3_speed'])
            field_vals.append (j['fields']['fan4_speed'])
            field_vals.append (j['fields']['host_health_status'])
            field_vals.append (j['fields']['power_state'])
            field_vals.append (j['fields']['bmc_health_status'])
            field_vals.append (j['fields']['led_indicator'])
            field_vals.append (j['fields']['inlet_health_status'])
            field_vals.append (j['fields']['fan1_health'])
            field_vals.append (j['fields']['fan2_health'])
            field_vals.append (j['fields']['fan3_health'])
            field_vals.append (j['fields']['fan4_health'])
            field_vals.append (j['fields']['memory_health_status'])
            field_vals.append (j['fields']['cpu_health_status'])
    
    with open(r'/home/system_metrics.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow(field_vals)
    f.close()
    
        


def log_job_response(jsonObjList,log_prefix,error_list,check):
    # Workbook is created 
    wb = Workbook(encoding='utf-8') 

    # add_sheet is used to create sheet. 
    summary = wb.add_sheet('Summary')
    detail = wb.add_sheet('Details') 

    normal=0
    first_retry=0
    second_retry=0
    third_retry=0
    total_succeeded_reqs=0
    total_failed_reqs=0
    row=1
    col=0
    status = None
    detail.write(0, 0,'Iterations #')
    detail.write(0, 1,'Host')
    detail.write(0, 2, 'No. of (Re-)Try')
    detail.write(0, 3, 'Metric')
    detail.write(0, 4, 'GET_Req_Response_Duration')
    detail.write(0, 5, 'Response_Status')

    for jsonObj,error in zip(jsonObjList,error_list):
        col = 0
        #detail.write(row, col, row)
        #col += 1
        detail.write(row, col, jsonObj['tags']['host'])
        col += 1
        detail.write(row, col, jsonObj['fields']['retry'])
        col += 1
        detail.write(row,col, jsonObj['measurement'])
        col += 1
        detail.write(row,col, jsonObj['fields']['GET_processing_time'])
        col += 1

        if error[2] == 'None':
            status = 'Success'
        else:
            status = error[2]
            #print (status)
        
        detail.write(row,col, status)
        col += 1
        row += 1
        
        if error[2] != 'None':                                                                                                                                                  
            total_failed_reqs +=1                                                                                                                                                                              
        if jsonObj['fields']['retry'] == 0:                                                                                                                                                                    
            normal +=1                                                                                                                                                                                         
        elif jsonObj['fields']['retry'] == 1:                                                                                                                                                                  
            first_retry += 1                                                                                                                                                                                   
        elif jsonObj['fields']['retry'] == 2:                                                                                                                                                                  
            second_retry += 1                                                                                                                                                                                  
        elif jsonObj['fields']['retry'] == 3:                                                                                                                                                                  
            third_retry += 1

    if check == 'SystemHealth' or check == 'Thermal':
        total_failed_reqs //=5
        normal //=5
        first_retry //=5
        second_retry //=5
        third_retry //=5

    total_failed_reqs -= first_retry
    total_failed_reqs -= second_retry
    total_failed_reqs -= third_retry
        
        
    #total_failed_reqs -= 2
    
    print ("First Time Success:",normal,"1st Retry Success:",first_retry,"Second Retry Success:",second_retry, "Third Retry Success:",third_retry,"Total Failures:",total_failed_reqs)
    
    
    summary.write(0, 1,'First_Time_Success') 
    summary.write(0, 2, '1st_Retry_Success') 
    summary.write(0, 3, '2nd_Retry_Success') 
    summary.write(0, 4, '3rd_Retry_Success') 
    summary.write(0, 5, 'Total_Failures') 

    summary.write(1, 1,normal)
    summary.write(1, 2, first_retry)
    summary.write(1, 3, second_retry)
    summary.write(1, 4, third_retry)
    summary.write(1, 5, total_failed_reqs)
    #wb.save('Iteration-'+log_prefix+'-log.xls')
    #curr_time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    #wb.save('/home/logs/'+log_prefix+'_'+check+'-log.xls')



def nagios_external_agent(jsonObjList, error_list):
    
    nagios_cmd = open("/usr/local/nagios/var/rw/nagios.cmd", "w")
    
    for jsonObj in jsonObjList:
    #for jsonObj,error in zip(jsonObjList,error_list):
        #host =  jsonObj['tags']['host']
        #jsonObj =  jsonObjList [i]
        #error = error_list [i]
        
        if jsonObj['measurement'] == "JobsInfo":
            continue

        if jsonObj['tags'].get('NodeId') != None:
            host =  jsonObj['tags']['NodeId']

        if jsonObj['tags'].get('Sensor') != None:
            check_service_description = jsonObj['tags']['Sensor']
        
        if jsonObj['measurement'] == "NodeJobs":
            check_service_description = "NodeJobs"

        return_code = None
        output = ""
        timestamp = int(time.time())


        if(check_service_description == "NodeHealth"):
            
            
            if jsonObj['fields'].get('Reading') != None:
                # health_status =jsonObj['fields']['Reading']
                # return_code, output = return_output(health_status,check_service_description,error[2])
                return_code = 0
                output = jsonObj
            else:
                output = "Metric Unavailable!"
                return_code = 3

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "PowerState"):
            
            if jsonObj['fields'].get('Reading') != None:
                # health_status =jsonObj['fields']['Reading']
                # return_code, output = return_output(health_status,check_service_description,error[2])
                return_code = 0
                output = jsonObj              
            else:
                output = "Metric Unavailable!"
                return_code = 3
            
            
            nagios_cmd.write("[{timestamp}] PROCESS_HOST_CHECK_RESULT;{hostname};{return_code};{text}\n".format
                     (timestamp = timestamp,
                      hostname = host,
                      return_code = return_code,
                      text = output)) 
            
            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "IndicatorLEDStatus"):
            
            # # if error[2] != 'None':
            # #     output = "Metric Unavailable!"
            # #     return_code = 3
            # else:
            if jsonObj['fields'].get('Reading') != None:
                    # indicator =jsonObj['fields']['Reading']
                    
                    # if indicator == "Lit":
                    #     return_code = 2
                    #     output = "Node Indicator LED status is Lit!"

                    # elif indicator == "Blinking":
                    #     return_code = 1
                    #     output = "Node Indicator LED status is Blinking!"
                    
                    # elif indicator == "Off":
                    #     return_code = 0
                    #     output = "Node Indicator LED status is Off!"

                    # elif indicator == "Unknown":
                    #     return_code = 3
                    #     output = "Node Indicator LED status is unknown!"
                output = jsonObj
                return_code = 0
            else:
                output = "Metric Unavailable!"
                return_code = 3
            
            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "InletHealth"):
            # if error[2] == 'None':
            if jsonObj['fields'].get('Reading') != None:
                # health_status =jsonObj['fields']['Reading']
                # return_code, output = return_output(health_status,check_service_description,error[2])
                output = jsonObj
                return_code = 0
            else:
                output = "Metric Unavailable!"
                return_code = 3

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "BMCHealth"):
            
            # if error[2] != 'None':
            #     return_code = 3
            #     output = "Metric Unavailable!"
            # else:
            if jsonObj['fields'].get('Reading') != None:
                # health_status =jsonObj['fields']['Reading']
                # return_code, output = return_output(health_status,check_service_description,error[2])
                return_code = 0
                output = jsonObj
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "CPUHealth"):
            
            # if error[2] != 'None':
            #     return_code = 3
            #     output = "Metric Unavailable!"
            # else:
            if jsonObj['fields'].get('Reading') != None:
                # health_status =jsonObj['fields']['Reading']
                # return_code, output = return_output(health_status,check_service_description,error[2])
                return_code = 0
                output = jsonObj
            else:
                return_code = 3
                output = "Metric Unavailable!"
            
            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "MemHealth"):

            # if error[2] != 'None':
            #     return_code = 3
            #     output = "Metric Unavailable!"
            # else:
            if jsonObj['fields'].get('Reading') != None:
                # health_status =jsonObj['fields']['Reading']
                # return_code, output = return_output(health_status,check_service_description,error[2])
                return_code = 0
                output = jsonObj
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )
        
        elif(check_service_description == "NodeJobs"):
            #print ("\ncheck_service_description",check_service_description)                                                                                                                                 
            # *** Missing the OK/Warning/Critical thresholds ***                                                                                                                                             
            #return_code, output = return_output(health_status,check_service_description)                                                                                                                 
            output = jsonObj
            return_code=0
            #if error[2] != 'None':
            #    return_code = 3
            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )


        elif(check_service_description == "CPUUsage"):
            #print ("\ncheck_service_description",check_service_description)
            #cpu_usage =jsonObj['fields']['Reading']
            #print ("\nHost:",host)
            # *** Missing the OK/Warning/Critical thresholds ***
            #return_code, output = return_output(health_status,check_service_description)
            
            if jsonObj['fields'].get('Reading') != None:
                output=jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "NodePower"):
            
            #power_usage =jsonObj['fields']['Reading']
            
            # *** Applying OK/Warning/Critical thresholds ***
            # if power_usage != None:

            #     if power_usage <= jsonObj['fields']['PowerRequestedWatts']:
            #         return_code=0
            #     elif power_usage > jsonObj['fields']['PowerRequestedWatts'] and power_usage <= jsonObj['fields']['PowerCapacityWatts']:
            #         return_code=1
            #     elif power_usage > jsonObj['fields']['PowerCapacityWatts']:
            #         return_code=2
            # else:
            #     return_code = 3
            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"
                

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "MemUsage"):
            #print ("\ncheck_service_description",check_service_description)
            # *** Missing the OK/Warning/Critical thresholds ***  
            # total_mem =jsonObj['fields']['total_memory']
            # avail_mem =jsonObj['fields']['available_memory']
            #mem_used =jsonObj['fields']['Reading']
            #output="Total Memory: "+str(total_mem)+" Used Memory: "+str(mem_used)+" Avaiable Memory: "+str(avail_mem)
            
            #return_code, output = return_output(health_status,check_service_description)
            # if error[2] != 'None':
            #     return_code = 3
            #     output = "Metric Unavailable!"
            # else:
            #     output = jsonObj
            #     return_code=0
            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )
        
        elif(check_service_description == "CPU1Temp"):

            # *** Applying the OK/Warning/Critical thresholds ***
                                                                                                                                                                      
            # if error[2] == 'None' and jsonObj['fields']['cpuLowerThresholdNonCritical'] != None and jsonObj['fields']['cpuUpperThresholdNonCritical'] != None and jsonObj['fields']['cpuLowerThresholdCritical'] != None and jsonObj['fields']['cpuUpperThresholdCritical'] != None:
            #     if 'CPU1 Temp' in jsonObj['fields']:
            #         if jsonObj['fields']['CPU1 Temp'] >= jsonObj['fields']['cpuLowerThresholdNonCritical'] and jsonObj['fields']['CPU1 Temp'] <= jsonObj['fields']['cpuUpperThresholdNonCritical']:
            #             status_codes.append(0)
            #         elif jsonObj['fields']['CPU1 Temp'] > jsonObj['fields']['cpuLowerThresholdNonCritical'] and jsonObj['fields']['CPU1 Temp'] <= jsonObj['fields']['cpuUpperThresholdNonCritical'] or jsonObj['fields']['CPU1 Temp'] >= jsonObj['fields']['cpuLowerThresholdCritical'] and jsonObj['fields']['CPU1 Temp'] < jsonObj['fields']['cpuLowerThresholdNonCritical']:
            #             status_codes.append(1)
            #         elif jsonObj['fields']['CPU1 Temp'] < jsonObj['fields']['cpuLowerThresholdCritical'] and jsonObj['fields']['CPU1 Temp'] > jsonObj['fields']['cpuUpperThresholdCritical']:
            #             status_codes.append(2)
            #     else:
            #         status_codes.append(3)
            #     if 'CPU2 Temp' in jsonObj['fields']:
            #         if jsonObj['fields']['CPU2 Temp'] >= jsonObj['fields']['cpuLowerThresholdNonCritical'] and jsonObj['fields']['CPU2 Temp'] <= jsonObj['fields']['cpuUpperThresholdNonCritical']:
            #             status_codes.append(0)
            #         elif jsonObj['fields']['CPU2 Temp'] > jsonObj['fields']['cpuLowerThresholdNonCritical'] and jsonObj['fields']['CPU2 Temp'] <= jsonObj['fields']['cpuUpperThresholdNonCritical'] or jsonObj['fields']['CPU2 Temp'] >= jsonObj['fields']['cpuLowerThresholdCritical'] and jsonObj['fields']['CPU2 Temp'] < jsonObj['fields']['cpuLowerThresholdNonCritical']:
            #             status_codes.append(1)
            #         elif jsonObj['fields']['CPU2 Temp'] < jsonObj['fields']['cpuLowerThresholdCritical'] and jsonObj['fields']['CPU2 Temp'] > jsonObj['fields']['cpuUpperThresholdCritical']:
            #             status_codes.append(2)
            #     else:
            #         status_codes.append(3)
            # else:
            #     status_codes.append(3)
            # if error[2] != 'None':
            #     return_code = 3
            #     output = "Metric Unavailable!"
            # else:
            #     return_code=0
            #     output =jsonObj

            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "CPU2Temp"):

            # *** Applying the OK/Warning/Critical thresholds ***                                                                                                                                                          
            
            # if error[2] != 'None':
            #     return_code = 3
            #     output = "Metric Unavailable!"
            # else:
            #     return_code=0
            #     output =jsonObj

            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "InletTemp"):

        # *** Applying the OK/Warning/Critical thresholds ***                                                                                                                              
            
            # if error[2] == 'None' and jsonObj['fields']['inletLowerThresholdNonCritical'] != None and jsonObj['fields']['inletUpperThresholdNonCritical'] != None and jsonObj['fields']['inletUpperThresholdCritical'] != None:
            #     if jsonObj['fields']['Inlet Temp'] >= jsonObj['fields']['inletLowerThresholdNonCritical'] and jsonObj['fields']['Inlet Temp'] <= jsonObj['fields']['inletUpperThresholdNonCritical']:
            #         return_code=0
            #     elif jsonObj['fields']['Inlet Temp'] > jsonObj['fields']['inletUpperThresholdNonCritical'] and jsonObj['fields']['Inlet Temp'] <= jsonObj['fields']['inletUpperThresholdCritical']:
            #         return_code=1
            #     elif jsonObj['fields']['Inlet Temp'] < jsonObj['fields']['inletLowerThresholdNonCritical'] or jsonObj['fields']['Inlet Temp'] > jsonObj['fields']['inletUpperThresholdCritical']:
            #         return_code=2
                    

            # else:
            #     return_code = 3

            # if error[2] != 'None':
            #     return_code = 3
            #     output = "Metric Unavailable!"
            # else:
            #     return_code = 0
            #     output =jsonObj

            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "FAN_1Speed"):
            
            # status_codes=[]
            # # *** Apply OK/Warning/Critical thresholds ***
            # if error[2] == 'None' and 'FAN_1' in jsonObj['fields']:
            #     if jsonObj['fields']['FAN_1'] > jsonObj['fields']['fanLowerThresholdCritical'] and jsonObj['fields']['FAN_1'] < jsonObj['fields']['fanUpperThresholdCritical']:
            #         status_codes.append(0)
            #     elif jsonObj['fields']['FAN_1'] <= jsonObj['fields']['fanLowerThresholdCritical'] or jsonObj['fields']['FAN_1'] >=  jsonObj['fields']['fanUpperThresholdCritical']:
            #         status_codes.append(2)

            #     if jsonObj['fields']['FAN_2'] > jsonObj['fields']['fanLowerThresholdCritical'] and jsonObj['fields']['FAN_2'] < jsonObj['fields']['fanUpperThresholdCritical']:
            #         status_codes.append(0)
            #     elif jsonObj['fields']['FAN_2'] <= jsonObj['fields']['fanLowerThresholdCritical'] or jsonObj['fields']['FAN_2'] >=  jsonObj['fields']['fanUpperThresholdCritical']:
            #         status_codes.append(2)

            #     if jsonObj['fields']['FAN_3'] > jsonObj['fields']['fanLowerThresholdCritical'] and jsonObj['fields']['FAN_3'] < jsonObj['fields']['fanUpperThresholdCritical']:
            #         status_codes.append(0)
            #     elif jsonObj['fields']['FAN_3'] <= jsonObj['fields']['fanLowerThresholdCritical'] or jsonObj['fields']['FAN_3'] >=  jsonObj['fields']['fanUpperThresholdCritical']:
            #         status_codes.append(2)

            #     if jsonObj['fields']['FAN_4'] > jsonObj['fields']['fanLowerThresholdCritical'] and jsonObj['fields']['FAN_4'] < jsonObj['fields']['fanUpperThresholdCritical']:
            #         status_codes.append(0)
            #     elif jsonObj['fields']['FAN_4'] <= jsonObj['fields']['fanLowerThresholdCritical'] or jsonObj['fields']['FAN_4'] >=  jsonObj['fields']['fanUpperThresholdCritical']:
            #         status_codes.append(2)
            # else:
            #     status_codes.append(3)

            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )
        
        elif(check_service_description == "FAN_2Speed"):
            
            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "FAN_3Speed"):
            
            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "FAN_4Speed"):
            
            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "FAN_1Health"):
                
                # *** Missing the OK/Warning/Critical thresholds ***                                                                                                      
            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"

            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "FAN_2Health"):
                
                # *** Missing the OK/Warning/Critical thresholds ***                                                                                                      
            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"
                    
            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "FAN_3Health"):
                
                # *** Missing the OK/Warning/Critical thresholds ***                                                                                                      
            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"
                    
            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )
        
        elif(check_service_description == "FAN_4Health"):
                
                # *** Missing the OK/Warning/Critical thresholds ***                                                                                                      
            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"
                    
            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )
            
        elif(check_service_description == "MemPowerUsage"):
            
                # *** Missing the OK/Warning/Critical thresholds ***                                                                                                      
            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"
                    
            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

        elif(check_service_description == "CPUPowerUsage"):

            if jsonObj['fields'].get('Reading') != None:
                output = jsonObj
                return_code=0
            else:
                return_code = 3
                output = "Metric Unavailable!"
                    
            update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd )

    nagios_cmd.close()
    
def update_service (host,timestamp,check_service_description,return_code,output,nagios_cmd):
    nagios_cmd.write("[{timestamp}] PROCESS_SERVICE_CHECK_RESULT;{hostname};{service};{return_code};{text}\n".format
                     (timestamp = timestamp,
                      hostname = host,
                      service = check_service_description,
                      return_code = return_code,
                      text = output))

    

            
def return_output (health_status,service_description,error):
    
    if(service_description == "InletHealth"):

        if(health_status == "OK"):
            return 0,"OK - Inlet sensor health is OK!"

        elif(health_status == "Warning"):
            return 1,"WARNING - Inlet sensor needs attention!"

        elif(health_status == "Critical"):
            return 2, "CRITICAL - Inlet sensor needs immediate attention!"
        return 3, None

    elif(service_description == "NodeHealth"):
        if(health_status == "OK"):
            return 0,"OK - Node health is OK!"
            
        elif(health_status == "Warning"):
            return 1,"WARNING - Node needs attention!"

        elif(health_status == "Critical"):
            return 2, "CRITICAL - Node needs immediate attention!"
        return 3, None

    elif(service_description == "BMCHealth"):
        if(health_status == "OK"):
            return 0,"OK - BMC health is OK!"

        elif(health_status == "Warning"):
                return 1,"WARNING - BMC needs attention!"

        elif(health_status == "Critical"):
                return 2, "CRITICAL - BMC needs immediate attention!"
        return 3, None

    elif(service_description == "CPUHealth"):
            
        if(health_status == "OK"):
            return 0,"OK - CPU health is OK!"

        elif(health_status == "Warning"):
                return 1,"WARNING - CPU needs attention!"

        elif(health_status == "Critical"):
                return 2, "CRITICAL - CPU needs immediate attention!"
        return 3, None

    elif(service_description == "MemHealth"):
        if(health_status == "OK"):
            return 0,"OK - Memory health is OK!"

        elif(health_status == "Warning"):
                return 1,"WARNING - Memory needs attention!"

        elif(health_status == "Critical"):
                return 2, "CRITICAL - Memory needs immediate attention!"
        return 3, None
    
    elif(service_description == "PowerState"):
        if error != 'None':
            return 2,None
        else:
            if(health_status == "On"):
                return 0,"On - Node power state is On!"

            elif(health_status == "PoweringOn"):
                return 2,"PoweringOn - Node is going through powring on!"

            elif(health_status == "PoweringOff"):
                return 2,"PoweringOff - Node is going through powring off!"

            elif(health_status == "Off"):
                return 1, "Off - Node is powered off!"
            return 1, None


if __name__== "__main__":
  main()
