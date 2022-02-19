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
metadata_db_user: <your_metadata_db_username>
metadata_db_password: <your_metadata_db_password>
metadata_db_port: <your_metadata_db_port_num>
```

To create an engine

```python
import os
from wareflow.utils.db import get_data_db_engine_from_credential_file

engine = get_data_db_engine_from_credential_file(
    credential_path=os.path.join("path", "to", "credentials.yml")
)

```
