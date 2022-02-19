import os
import yaml


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
        port=credentials["data_db_port"]
    )


def get_data_db_engine_from_credential_file(credential_path: os.path):
    connection_url = get_data_db_connection_url_from_credential_file(credential_path)
    return create_engine(connection_url, future=True)