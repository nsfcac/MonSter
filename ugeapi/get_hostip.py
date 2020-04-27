def get_hostip(hostname: str) -> str:
    """
    Convert hostname('compute-1-1.localdomain') to ip address (10.101.1.1)
    """
    if "-" in hostname:
        h0, h1, h2 = hostname.split('-')
        return '10.101.' + h1 + '.' + h2.split('.')[0]
    return None