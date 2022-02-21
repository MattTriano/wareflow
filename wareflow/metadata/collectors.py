from datetime import datetime
from hashlib import sha256
from typing import Dict, Optional

import requests


class SocrataTableMetadata:
    def __init__(
        self, dwh_schema_base_name: str, dwh_table_name: str, table_id: str
    ) -> None:
        self.dwh_schema_base_name = dwh_schema_base_name
        self.dwh_table_name = dwh_table_name
        self.table_id = table_id
        self.table_metadata = None

    def __repr__(self):
        repr_str = (
            f"SocrataTableMetadata(\n"
            + f"    dwh_schema_base_name='{self.dwh_schema_base_name}',\n"
            + f"    dwh_table_name='{self.dwh_table_name}',\n"
            + f"    table_id='{self.table_id}'\n"
            + f")"
        )
        return repr_str

    def __str__(self):
        repr_str = (
            f"SocrataTableMetadata(\n"
            + f"    dwh_schema_base_name='{self.dwh_schema_base_name}',\n"
            + f"    dwh_table_name='{self.dwh_table_name}',\n"
            + f"    table_id='{self.table_id}'\n"
            + f")"
        )
        return repr_str

    def set_hash_of_column_details(self) -> None:
        source_domain = self.table_metadata["metadata"]["domain"]
        table_details = self.table_metadata["resource"]
        table_description = table_details["description"]
        col_details = zip(
            table_details["columns_name"],
            table_details["columns_field_name"],
            table_details["columns_datatype"],
            table_details["columns_description"],
        )
        col_str = "".join(
            [
                name + field_name + datatype + descr
                for name, field_name, datatype, descr in col_details
            ]
        )
        prehash_str = self.table_id + source_domain + table_description + col_str
        detail_hash_str = sha256(prehash_str.encode(encoding="utf-8")).hexdigest()
        self.table_metadata["table_details_hash"] = detail_hash_str

    def set_table_metadata(self) -> None:
        api_call = f"http://api.us.socrata.com/api/catalog/v1?ids={self.table_id}"
        response = requests.get(api_call)
        if response.status_code == 200:
            response_json = response.json()
            results = {"_id": self.table_id, "time_of_collection": datetime.utcnow()}
            results.update(response_json["results"][0])
            self.table_metadata = results
            self.set_hash_of_column_details()

    def get_table_metadata(self) -> Optional[Dict]:
        if self.table_metadata is None:
            self.set_table_metadata()
        if self.table_metadata is not None:
            return self.table_metadata
        else:
            print("Couldn't retrieve table_metadata. Debug this issue.")

    def get_socrata_to_sqlalchemy_naive_dtype_map(self) -> Dict:
        socrata_to_sqlalchemy_naive_dtype_map = {
            "Number": "Float",
            "Text": "String",
            "Floating Timestamp": "DateTime",
            "Fixed Timestamp": "DateTime",
        }
        return socrata_to_sqlalchemy_naive_dtype_map

    def print_naive_sqlalchemy_table_class(self) -> None:
        # table_name = self.dwh_table_name
        table_metadata = self.get_table_metadata()
        assert table_metadata is not None
        column_socrata_dtypes = table_metadata["resource"]
        socrata_to_sqlalchemy_naive_dtype_map = (
            self.get_socrata_to_sqlalchemy_naive_dtype_map()
        )

        table_class_name = "".join(
            [el.capitalize() for el in self.dwh_table_name.split("_")]
        )
        sqlalchemy_table_column_ddl_list = [
            f"class {table_class_name}(Base):",
            f"    __tablename__ = '{self.dwh_table_name}'",
            f"    __table_args__ = {{'schema': '{self.dwh_schema_base_name}_data_raw'}}\n",
        ]
        for col_name, col_dtype in zip(
            column_socrata_dtypes["columns_field_name"],
            column_socrata_dtypes["columns_datatype"],
        ):
            sqlalchemy_dtype = socrata_to_sqlalchemy_naive_dtype_map[col_dtype]
            sqlalchemy_column_line = f"    {col_name} = Column({sqlalchemy_dtype})"
            sqlalchemy_table_column_ddl_list.append(sqlalchemy_column_line)
        print("\n".join(sqlalchemy_table_column_ddl_list))
