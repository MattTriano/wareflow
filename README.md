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


## Design Notes

The current implementation uses a postGIS (ie postgresql) database to store data tables and a mongoDB instance to store data table metadata.

* Postgresql's implementation allows for the creation of a **cluster**, which can host multiple **databases**, each of which can contain multiple **schemas** (essentially user-defined namespaces), each of which can contain multiple **tables**. 
    * The schema namespace layer can only support a single level (eg you can't have a `data_group.data_raw` schema, only a `data_group` or a `data_raw` schema). 
    * User connections are to a database and the other databases in the cluster are inaccessible (without an explicitly created connection to those databases).

* MongoDB's implementation allows for the creation of multiple **databases** within a **MongoDB instance** (either **standalone** for a single instance or **replica set** for replicated instances), and each database can contain many **collections**, each of which can contain multiple **documents**.
    * The MongoClient object can easily access different databases in the MongoDB instance
    * Collections are similar to the schema (namespace) layer in postgresql in that they can only support 1 level.
    * Each table's metadata will be a document within a collection.
  
### Potential implementations
With the above laid out, I see a symmetry that would be useful to utilize, but it might involve backtracking on my `data_raw`, `data_clean` schema concept. Rather than have a schema for the state of the data, it may make more sense to form conceptual groupings for data and make the schemas around that.
* The conceptual grouping name could be used for the name of the schema and collection
    * The `_raw` and `_clean` suffixes indicating data stage could be tacked on to the schema/collection name, or to the table/document name.
        * Attaching them to the table/document name would
            * make it simpler to see the available data groupings, 
            * potentially make it more transparent (to devs and end-users, depending on implementation) which stage of data is being accessed,
            * make it messier to look through available tables/documents
        * Attaching them to the schema/collection names would
            * allow for consistent table/document names across data stages,
            * facilitate quicker search of documents (although this is likely irrelevant for the number of documents this may have)
            * facilitate less polluted search of documents
            * allow for easier data governance by allowing for the creation of roles with access to different schemas rather than making roles that have to look at table/document names to determine access
            
            
I think I like the schema/collection level naming convention better, so I'll explore that first.