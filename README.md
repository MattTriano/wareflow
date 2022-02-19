# Wareflow

Tools for working with the my data warehouse, metadata catalog, and other data infrastructure services.

## Setup

First, clone this repo and `cd` into the cloned repo.

To do dev work, create a conda env via
```bash
$ conda env create -f environment.yml
```
then activate the env and install `wareflow` via pip
```bash
$ conda activate dwh_env
(dwh_env) ...$ python -m pip install -e .
```

To simply use the package, just install it into whatever env you like with the `-e` (editable) flag
```bash
(your_env) ...$ python -m pip install .
```

## Usage

First off, create a credentials YAML file with the key names shown below, and the values filled in with the values appropriate for your setup.
```yaml
data_db_driver: postgresql+psycopg2
data_db_host: <your_data_db_host_name>
data_db_username: <your_data_db_user_name>
data_db_database: <your_data_dbs_name>
data_db_password: <your_data_db_password>
data_db_port: <your_data_db_port_num>
metadata_db_host: <your_metadata_db_host>
metadata_db_username: <your_metadata_db_username>
metadata_db_password: <your_metadata_db_password>
metadata_db_port: <your_metadata_db_port_num>
```

To create an engine (for working with the data_db) and client (for working with the metadata_db)

```python
import os
from wareflow.utils.db import (
    get_data_db_engine_from_credential_file,
    get_metadata_db_connection_from_credential_file
)

engine = get_data_db_engine_from_credential_file(
    credential_path=os.path.join("path", "to", "credentials.yml")
)
client = get_metadata_db_connection_from_credential_file(
    credential_path=os.path.join("path", "to", "credentials.yml")
)
```

To create a data_db schema
```python
from wareflow.utils.db import create_database_schema

create_database_schema(engine=engine, schema_name="data_raw")
```

To inspect the schemas in the data_db
```python
from wareflow.utils.db import get_data_schema_names

get_data_schema_names(engine)

['data_raw',
 'information_schema',
 'public',
 'tiger',
 'tiger_data',
 'topology']
```