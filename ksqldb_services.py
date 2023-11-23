import requests

URL = "http://ksqldb-1.bnh.vn:8088"


def get_all_streams():
    endpoint = "{}/ksql".format(URL)
    headers = {"Content-Type": "application/json"}
    payload = {"ksql": "LIST STREAMS EXTENDED;"}

    response = requests.post(endpoint, headers=headers, json=payload)

    return response.json()


def drop_stream_and_delete_topic_by_name(name):
    endpoint = "{}/ksql".format(URL)
    headers = {"Content-Type": "application/json"}
    payload = {"ksql": "DROP STREAM IF EXISTS {} DELETE TOPIC;".format(name)}

    response = requests.post(endpoint, headers=headers, json=payload)

    return response


def get_stream_by_name(name):
    endpoint = "{}/ksql".format(URL)
    headers = {"Content-Type": "application/json"}
    payload = {"ksql": "DESCRIBE {};".format(name)}

    response = requests.post(endpoint, headers=headers, json=payload)

    return response.json()
