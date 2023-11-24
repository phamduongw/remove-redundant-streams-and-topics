from ksqldb_services import list_streams_extended
from kconnect_services import get_all_connectors, get_connector_config_by_name


def get_all_streams_and_topics():
    response = list_streams_extended()

    if type(response) == dict:
        raise RuntimeError(response)

    all_streams_and_topics = []

    source_descriptions = response[0]["sourceDescriptions"]

    for source_description in source_descriptions:
        sinks = []

        for read_query in source_description["readQueries"]:
            for sink in read_query["sinks"]:
                sinks.append(sink)

        all_streams_and_topics.append(
            {
                "name": source_description["name"],
                "sinks": sinks,
                "topic": source_description["topic"],
            }
        )

    return all_streams_and_topics


def get_used_ods_topics():
    used_ods_topics = []

    all_connectors = get_all_connectors()

    for connector in all_connectors:
        connector_config = get_connector_config_by_name(connector)

        for topic in connector_config["topics"].split(", "):
            if topic.startswith("ODS_"):
                used_ods_topics.append(topic)

    return used_ods_topics


def get_unused_ods_streams():
    unused_ods_streams = []

    used_ods_topics = get_used_ods_topics()

    for stream_info in ALL_STREAMS_AND_TOPICS:
        name = stream_info["name"]
        topic = stream_info["topic"]

        if name.startswith("ODS_") and topic not in used_ods_topics:
            unused_ods_streams.append(name)

    return unused_ods_streams


def get_stream_flow(unused_stream):
    stream_flow = []

    def get_stream_flow_item(unused_stream):
        stream_flow.append(unused_stream)

        for stream_info in ALL_STREAMS_AND_TOPICS:
            for sink in stream_info["sinks"]:
                if sink == unused_stream:
                    get_stream_flow_item(stream_info["name"])

    get_stream_flow_item(unused_stream)

    return stream_flow


def create_delete_query(unused_ods_stream, stream_flows):
    print("\n-- {}".format(unused_ods_stream))
    for stream in stream_flows:
        print("DROP STREAM IF EXISTS {} DELETE TOPIC;".format(stream))


def main():
    unused_ods_streams = get_unused_ods_streams()

    for unused_ods_stream in unused_ods_streams:
        stream_flow = get_stream_flow(unused_ods_stream)
        create_delete_query(unused_ods_stream, stream_flow)


ALL_STREAMS_AND_TOPICS = get_all_streams_and_topics()

if __name__ == "__main__":
    main()
