import requests

URL = "http://kconnect-1.bnh.vn:8083"


def get_all_connectors():
    endpoint = "{}/connectors".format(URL)
    response = requests.get(endpoint)

    return response.json()


def get_connector_config_by_name(name):
    endpoint = "{}/connectors/{}/config".format(URL, name)
    response = requests.get(endpoint)

    return response.json()
