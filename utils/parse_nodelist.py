def parse_nodelist(nodelist_cfg: list) -> list:
    """
    Generate ip addresses of nodes from the configuration
    """
    nodelist = []
    for item in nodelist_cfg:
        ip_addr_node = item.split("/")[0]
        ip_addr_subnet = item.split("/")[1]

        sections = ip_addr_subnet.split(",")
        for section in sections:
            if "-" in section:
                st = int(section.split("-")[0])
                ed = int(section.split("-")[1])
                for i in range(st, ed+1):
                    ip_addr = ip_addr_node + "." + str(i)
                    nodelist.append(ip_addr)
            else:
                ip_addr = ip_addr_node + "." + str(int(section))
                nodelist.append(ip_addr)

    return nodelist
