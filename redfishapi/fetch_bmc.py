response = session.get(url, verify=False, auth=('password','monster'), timeout=(conn_time_out,read_time_out))

# BMC health metric
url = 'https://' + host + '/redfish/v1/Managers/iDRAC.Embedded.1'

# System health metric
url = "https://" + host + "/redfish/v1/Systems/System.Embedded.1"

# Thermal information
url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Thermal/"

# Power consumption
url = "https://" + host + "/redfish/v1/Chassis/System.Embedded.1/Power/"

# BIOS
"/redfish/v1/Systems/System.Embedded.1/Bios"
# Processors
"/redfish/v1/Systems/System.Embedded.1/Processors"
# Memory
"/redfish/v1/Systems/System.Embedded.1/Memory"
# NetworkInterfaces
"/redfish/v1/Systems/System.Embedded.1/NetworkInterfaces"
# Storage
"/redfish/v1/Systems/System.Embedded.1/Storage"