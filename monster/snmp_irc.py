import asyncio
from pysnmp.hlapi.asyncio import  *

from monster import utils

METRIC_NAME_OID = 'SNMPv2-SMI::enterprises.318.1.1.27.1.4.1.2.1.2.1.'
METRIC_VALU_OID = 'SNMPv2-SMI::enterprises.318.1.1.27.1.4.1.2.1.3.1.'
METRIC_UNIT_OID = 'SNMPv2-SMI::enterprises.318.1.1.27.1.4.1.2.1.4.1.'
METRIC_ACCU_OID = 'SNMPv2-SMI::enterprises.318.1.1.27.1.4.1.2.1.5.1.'


async def get_irc_metrics(irc_ip, username):
    metrics = {}
    selected_metrics = []

    engine    = SnmpEngine()
    user_data = UsmUserData(username)
    transport = await UdpTransportTarget.create((irc_ip, 161))
    context   = ContextData()

    # Starting OID (APC enterprise OID)
    oid = ObjectIdentity('1.3.6.1')

    async for (errorIndication, errorStatus, errorIndex, varBindTable) in walk_cmd(
        engine,
        user_data,
        transport,
        context,
        ObjectType(oid),
    ):

        if errorIndication:
            print(errorIndication)
            return

        elif errorStatus:
            print(
                "{} at {}".format(
                    errorStatus.prettyPrint(),
                    errorIndex and varBindTable[int(errorIndex) - 1][0] or "?",
                )
            )
            return

        else:
            for varBind in varBindTable:
                if(varBind[0].prettyPrint().startswith(METRIC_NAME_OID)):
                    id  = int(varBind[0].prettyPrint().replace(METRIC_NAME_OID, ''))
                    metrics.update({
                        id: {
                            'snmp_oid': METRIC_VALU_OID + str(id),
                            'metric_id': varBind[1].prettyPrint().replace(' ','').replace('.', ''),
                            'metric_name': varBind[1].prettyPrint(),
                            'value': None,  # Placeholder for value
                            'metric_data_type': None,
                            'units': None,
                            'accuracy': None,
                        }
                    })
                elif(varBind[0].prettyPrint().startswith(METRIC_VALU_OID)):
                    this_id = int(varBind[0].prettyPrint().replace(METRIC_VALU_OID, ''))
                    value = varBind[1].prettyPrint()
                    if this_id in metrics:
                        metrics[this_id]['value'] = value
                elif(varBind[0].prettyPrint().startswith(METRIC_UNIT_OID)):
                    this_id = int(varBind[0].prettyPrint().replace(METRIC_UNIT_OID, ''))
                    unit = varBind[1].prettyPrint()
                    if this_id in metrics:
                        metrics[this_id]['units'] = unit
                elif(varBind[0].prettyPrint().startswith(METRIC_ACCU_OID)):
                    this_id = int(varBind[0].prettyPrint().replace(METRIC_ACCU_OID, ''))
                    accuracy = varBind[1].prettyPrint()
                    if this_id in metrics:
                        metrics[this_id]['accuracy'] = accuracy
                else:
                    pass

    # Post-process the metrics definition to ensure all fields are filled. If any field is missing, drop the metric.
    for id, metric in list(metrics.items()):
        if metric['value'] is None or metric['metric_id'] in selected_metrics:
            del metrics[id]
        else:
            accuracy = int(metric['accuracy'])
            metric['accuracy'] = accuracy
            if accuracy == 1: 
                metric['metric_data_type'] = 'INT'
                metric['value'] = int(metric['value'])
            else:
                metric['metric_data_type'] = 'REAL'
                metric['value'] = round(int(metric['value']) / int(metric['accuracy']), 2)
            selected_metrics.append(metric['metric_id'])


    return list(metrics.values())


if __name__ == "__main__":
    config = utils.parse_config()
    irc_ip   = utils.get_infra_ip_list(config, 'irc')[0]
    username  = utils.get_irc_auth()
    metrics = asyncio.run(get_irc_metrics(irc_ip, username))
    print(metrics)
    print(f"Length of metrics: {len(metrics)}")
