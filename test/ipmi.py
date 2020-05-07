import socket
import json
import datetime
import subprocess
import collections
import warnings
import re
from threading import Thread
import multiprocessing

def get_temp_power(ip):
    
    # To See Temperature and power Sensors, Use the following command, with the full argument:
    # sensor_data = subprocess.check_output(os.system('ipmitool -I lanplus -H' + ip + ' -U root -P nivipnut sdr elist full'))
    # ipmitool -I lanplus -H 10.201.19.52 -U root -P nivipnut sdr elist full | egrep -v "Disabled|No Reading|RPM"
    temperature = {'ambient':None,'cpu':{}}
    power = {'voltage':{},'current':{},'watts':{}}
    fans = {'fans':{}}
    try:
        sensors_data = subprocess.check_output('ipmitool -I lanplus -H ' + ip + ' -U root -P Zephyr sdr elist full | egrep -v "Disabled|No Reading"', shell=True).decode('ascii')
        #print (sensors_data)
    except subprocess.CalledProcessError as e:
        #raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
        temperature['cpu'] = None
        power['voltage'] = None
        power['current'] = None
        power['watts'] = None
        fans['fans'] = None
        return temperature, power, fans
    

    lines = sensors_data.split('\n')

    cpuid = 0
    wattsid = 0
    voltageid = 0
    currentid = 0

    for line in lines:
        fields = line.split('|')
        

        if ((fields[0].find('Ambient') != -1) or (fields[0].find('Inlet') != -1)):
                temperature['ambient'] = float((re.findall(r'-?\d+\.?\d*',fields[4])[0]))
    
        elif ( (fields[0].find('CPU') != -1) and (fields[0].find('CPU Usage') == -1)):
            cpuid += 1
            temperature['cpu']['cpu'+str(cpuid)] = float ((re.findall(r'-?\d+\.?\d*',fields[4])[0]))
        
        elif (fields[0].find('Voltage') != -1):
            voltageid += 1
            power['voltage']['voltage'+str(voltageid)] = float ((re.findall(r'-?\d+\.?\d*',fields[4])[0]))

        elif (fields[0].find('Current') != -1):
            currentid += 1
            power['current']['current'+str(currentid)] = float ((re.findall(r'-?\d+\.?\d*',fields[4])[0]))

        elif (fields[0].find('Watt') != -1):
            wattsid += 1
            power['watts']['watts'+str(wattsid)] = float ((re.findall(r'-?\d+\.?\d*',fields[4])[0]))

        elif (fields[0].find('FAN') != -1):
            fans['fans'][fields[0].strip()] = float((re.findall(r'-?\d+\.?\d*',fields[4])[0]))
    
    if cpuid == 0:
         temperature['cpu'] = None

    if voltageid == 0:
        power['voltage'] = None
    
    if currentid == 0:
        power['current'] = None
    
    if wattsid == 0:
        power['watts'] = None
    #print (fans)     
    return temperature, power, fans    

def get_sel(ip):

    #sel_data = os.system('ipmitool -I lanplus -H' + ip + ' -U root -P nivipnut sel list')
    sel_dic = {}
    try:
        ipmi_sel = subprocess.check_output('ipmitool -I lanplus -H' + ip + ' -U root -P Zephyr sel list  | egrep -v "Pre-Init"', shell=True).decode('ascii')
    except subprocess.CalledProcessError as e:
        return sel_dic
    
        
    sels = ipmi_sel.split('\n')
    
    for sel in sels:
        if (sel == ""):
            break

        fields = sel.split('|')
        
        sel_dic[datetime.datetime.strptime((fields[1].strip()) +' '+ (fields[2].strip()), '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')] = fields[3].strip() + ' : ' + fields[4].strip() + ' : ' + fields[5].strip()
        
    return sel_dic 

def get_power_status(ip):

    # host_status = os.system('ipmitool -I lanplus -H' + ip + ' -U root -P nivipnut power status')
    try:
        response = subprocess.check_output('ipmitool -I lanplus -H' + ip + ' -U root -P Zephyr power status', shell=True).decode('ascii')
        if (response.rfind("on") == -1):
            return 'Off'
        else:
            return 'On'
    except subprocess.CalledProcessError as e:
        return None

def getNodesData (host_name, ip, json_node_list, error_list):
    
    # Get the temperature and power metrics of the host
    temperature_values, power_values, fans = get_temp_power(ip)

    # Get the HOST SEL
    #sel_dic = get_sel(ip)
    
    # Get the power statu
    power_state = get_power_status(ip)
    
    # Encapsulate monitoring data into a python dictionary:
    mon_data_dict = {}
    
    mon_data_dict['hostname'] = host_name
    mon_data_dict['time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    mon_data_dict['temperature'] = temperature_values
    #mon_data_dict['sel'] = sel_dic
    mon_data_dict['power state'] = power_state
    mon_data_dict['power'] = power_values
    mon_data_dict['fans'] = fans
    # Convert Python dictionary into JSON data
    json_data =json.dumps(mon_data_dict)

    json_node_list.append(json_data)
        
    error = 'error'
    error_list.append([host_name, ip, error])


def proc_to_threads (input_data):
    
    warnings.filterwarnings('ignore', '.*', UserWarning,'warnings_filtering',)
    try:
        #print (input_data)
        error_list = []
        json_node_list = []
        threads = []   
        thread_id = 0
        for host_info in input_data:
            host_name = host_info[0]
            ip = host_info[1]
            a = Thread(target = getNodesData, args=(host_name, ip, json_node_list, error_list, ))
            threads.append(a)
            threads[thread_id].start()
            thread_id += 1

        for index in range (0, thread_id):
            threads[index].join()

            

        return json_node_list, error_list

    except Exception as e:
        error_list.append([ip, e])
        print (e)
        return json_node_list, error_list
    
def getNodeData (input_data):
    
    warnings.filterwarnings('ignore', '.*', UserWarning,'warnings_filtering',)
    try:
        nodes = len(input_data)
        if (nodes == 0):
            print ("\nThere is no node for monitoring!\n")
            return [], []
        
        node_error_list = []
        node_json_list = []
        
        #########################################################################
        # Initialize pool
        cores = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(cores)

        if ( nodes < cores ):
            cores = nodes

        print("Monitoring %d hosts using %d cores..." % \
            	(nodes, cores) )
        
        # Build job list
        jobs = []
        '''
        for host_info in input_data:
            jobs.append( (host_info[0], host_info[1], ) )
        '''
        hosts_per_proc = nodes // cores
        surplus_hosts = nodes % cores

        
        #print (input_data)
        for p in range (cores):
            if (surplus_hosts != 0 and p == (cores-1)):
                jobs.append( (input_data[p*hosts_per_proc:p*hosts_per_proc+hosts_per_proc-1] + input_data[p*hosts_per_proc+hosts_per_proc-1:p*hosts_per_proc+hosts_per_proc] + input_data[p*hosts_per_proc+hosts_per_proc:p*hosts_per_proc+hosts_per_proc+surplus_hosts] ,))
            else:
                jobs.append( (input_data[p*hosts_per_proc:p*hosts_per_proc+hosts_per_proc-1] + input_data[p*hosts_per_proc+hosts_per_proc-1:p*hosts_per_proc+hosts_per_proc],))                  
            #print (input_data[proc*threads_per_proc:proc*proc*threads_per_proc+threads_per_proc-1+surplus_threads])
                              
        # Run parallel jobs
        results = [pool.apply_async( proc_to_threads, j ) for j in jobs]

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
        print (e)
        return node_json_list, node_error_list
        
def main():
    
# hard coded IP address:
    #ip1 = '10.101.92.24'
    #host1 = 'Host1'
    
    #ip2 = '10.201.19.52'
    #host2 = 'Host2'
    
    '''
    The BMC IP addresses of Rack 91 are:  10.101.91.1  - 10.101.91.31
    The BMC IP addresses of Rack 92 are: 10.101.92.1  - 10.101.92.33

    The BMC IP of  the Head node is : 10.101.92.200
    The BMC IP of  the storage node is : 10.101.92.201
    '''
    
    hostsIP = []
    hostsID = []
    input_data = []
    
    for i in range (31):
        postfix = str(i+1)
        hostsIP.append('10.101.91.'+postfix)
        hostsID.append('Z-91-'+postfix)
        
    for i in range (33):
        postfix = str(i+1)
        hostsIP.append('10.101.92.'+postfix)
        hostsID.append('Z-92-'+postfix)
    
    for id,ip in zip(hostsID,hostsIP):
        input_data.append([id, ip])
    
    
    #input_data.append([host1, ip1])
    #input_data.append([host2, ip2])
    
    nodes_data, error_list =  getNodeData(input_data)
    #for node in nodes_data:
    for node in nodes_data:
        #node = eval(node) 
        print ('\n\n********************************************************************')
        print(node)
    
   
if __name__== "__main__":
  main()
