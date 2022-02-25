import os
from typing import Dict, List
from urllib.parse import quote_plus
import yaml

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.base import Engine
from sqlalchemy.schema import CreateSchema


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


def get_data_db_engine_from_credential_file(credential_path: os.path, future: bool = True) -> Engine:
    connection_url = get_data_db_connection_url_from_credential_file(credential_path)
    return create_engine(connection_url, future=future)


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


def get_metadatabase(client: MongoClient, metadatabase_name: str = "mdwh") -> Database:
    metadatabase = client[metadatabase_name]
    return metadatabase


def database_has_schema(engine: Engine, schema_name: str) -> bool:
    with engine.connect() as conn:
        return engine.dialect.has_schema(connection=conn, schema=schema_name)


def get_supported_data_stage_names() -> List[str]:
    supported_data_stage_names = ["data_raw", "data_clean", "analysis"]
    return supported_data_stage_names


def create_database_schema(engine: Engine, schema_name: str) -> None:
    if not database_has_schema(engine=engine, schema_name=schema_name):
        with engine.connect() as conn:
            conn.execute(CreateSchema(name=schema_name))
            conn.commit()
            print(f"Database schema '{schema_name}' successfully created.")
    else:
        print(f"Database schema '{schema_name}' already exists.")


def get_data_schema_names(engine: Engine) -> List:
    insp = inspect(engine)
    return insp.get_schema_names()


def get_data_table_names_in_schema(engine: Engine, schema_name: str) -> List:
    insp = inspect(engine)
    return insp.get_table_names(schema=schema_name)


def get_metadatabase_schema(
    schema_base_name, metadatabase: Database, data_stage: str = "data_raw"
) -> Collection:
    """Technically this returns a metadatabase collection, but the package is being
    built with the conceptual framework that postgres schemas should map to mongodb
    collections."""
    assert data_stage in get_supported_data_stage_names()
    schema_name = f"{schema_base_name}_{data_stage}"
    metadatabase_schema = metadatabase[schema_name]
    return metadatabase_schema
