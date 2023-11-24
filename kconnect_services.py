import os
import json

from utils import read_env_file, getBase64Credentials

read_env_file()

KCONNECT_URL = os.environ.get("KCONNECT_URL")
BASE64_CREDENTIALS = getBase64Credentials(
    os.environ.get("KCONNECT_USERNAME"), os.environ.get("KCONNECT_PASSWORD")
)


def get_all_connectors():
    command = """curl \
    --location '{}/connectors/' \
    --header 'Authorization: Basic {}'""".format(
        KCONNECT_URL, BASE64_CREDENTIALS
    )

    response = os.popen(command).read()

    return json.loads(response)


def get_connector_config_by_name(name):
    command = """curl \
    --location '{}/connectors/{}/config' \
    --header 'Authorization: Basic {}'""".format(
        KCONNECT_URL, name, BASE64_CREDENTIALS
    )

    response = os.popen(command).read()

    return json.loads(response)
