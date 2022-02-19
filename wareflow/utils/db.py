import os
from typing import Dict
from urllib.parse import quote_plus
import yaml

from pymongo import MongoClient
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine


def get_data_db_connection_url_from_credential_file(credential_path: os.path) -> URL:
    with open(credential_path) as cred_file:
        credentials = yaml.load(cred_file, Loader=yaml.FullLoader)

    return URL.create(
        drivername=credentials["data_db_driver"],
        host=credentials["data_db_host"],
        username=credentials["data_db_username"],
        database=credentials["data_db_database"],
        password=credentials["data_db_password"],
        port=credentials["data_db_port"],
    )


def get_data_db_engine_from_credential_file(credential_path: os.path):
    connection_url = get_data_db_connection_url_from_credential_file(credential_path)
    return create_engine(connection_url, future=True)


def get_metadata_db_connection_from_credential_file(
    credential_path: os.path, **kwargs: Dict
) -> Dict:
    with open(credential_path) as cred_file:
        credentials = yaml.load(cred_file, Loader=yaml.FullLoader)

    assert "metadata_db_username" in credentials.keys()
    assert "metadata_db_password" in credentials.keys()
    assert "metadata_db_host" in credentials.keys()
    assert "metadata_db_port" in credentials.keys()
    connection_str = (
        f"mongodb://{quote_plus(credentials['metadata_db_username'])}:"
        + f"{quote_plus(credentials['metadata_db_password'])}@"
        + f"{quote_plus(credentials['metadata_db_host'])}:"
        + f"{credentials['metadata_db_port']}/"
    )
    return MongoClient(connection_str)
