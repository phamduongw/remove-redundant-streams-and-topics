import requests
from ksqldb_services import (
    get_all_streams,
    drop_stream_and_delete_topic_by_name,
    get_stream_by_name,
)
from kconnect_services import get_all_connectors, get_connector_config_by_name

URL = "http://ksqldb-1.bnh.vn:8088/ksql"

used_topics = []
dependencies = []
deleted_stream = []


def get_used_topics():
    connectors = get_all_connectors()

    for connector in connectors:
        connector_config = get_connector_config_by_name(connector)

        for topic in connector_config["topics"].split(", "):
            if topic.startswith("ODS_"):
                used_topics.append(topic)


def find_all_dependencies_of_stream_by_name(name):
    headers = {"Content-Type": "application/json"}
    payload = {"ksql": "DESCRIBE {};".format(name)}
    response = requests.post(URL, headers=headers, json=payload)

    if response.status_code == 200:
        desc = response.json()[0]["sourceDescription"]
        for readQuery in desc["readQueries"]:
            for sink in readQuery["sinks"]:
                find_all_dependencies_of_stream_by_name(sink)

    dependencies.append(name)


def is_used_topic(name):
    topic = get_stream_by_name(name)[0]["sourceDescription"]["topic"]

    return topic in used_topics


def remove_redundant_streams_and_topics():
    source_descriptions = get_all_streams()[0]["sourceDescriptions"]

    for description in source_descriptions:
        global dependencies
        dependencies = []

        if not description["name"] in deleted_stream:
            find_all_dependencies_of_stream_by_name(description["name"])

            if not is_used_topic(dependencies[0]):
                for dependency in dependencies:
                    response = drop_stream_and_delete_topic_by_name(dependency)
                    if response.status_code == 200:
                        deleted_stream.append(dependency)
                        print(dependency)


def main():
    get_used_topics()
    remove_redundant_streams_and_topics()


if __name__ == "__main__":
    main()
