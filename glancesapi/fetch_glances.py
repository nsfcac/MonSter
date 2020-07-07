import json

from classes.AsyncRequests import AsyncRequests

glances_url = "http://10.10.1.4:61208/api/3/all"


# def fetch_glances(hostlist: list) -> object:

def generate_urls(hostlist: list) -> list:
    """
    Generate query urls according to the Glances RESTful JSON API:
    https://github.com/nicolargo/glances/wiki/The-Glances-RESTFULL-JSON-API
    """
    urls = ["http://" + host + ":61208/api/3/all" for host in hostlist]
    return urls


# fetch_glances()
