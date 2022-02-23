from wareflow.metadata.collectors import SocrataTableMetadata


class SocrataTableData:
    def __init__(
        self,
        schema_base_name: str,
        table_name: str,
        table_id: str,
        verbose: bool = False,
    ) -> None:
        self.schema_base_name = schema_base_name
        self.table_name = table_name
        self.table_id = table_id
        self.verbose = verbose
        self.metadata = SocrataTableMetadata(
            dwh_schema_base_name=self.schema_base_name,
            dwh_table_name=self.table_name,
            table_id=self.table_id,
            verbose=self.verbose,
        )
        self.data = None

    def table_has_geo_column(self) -> bool:
        socrata_geo_datatypes = [
            "Line",
            "Location",
            "MultiLine",
            "MultiPoint",
            "MultiPolygon",
            "Point",
            "Polygon",
        ]
        table_column_datatypes = self.metadata.table_metadata["resource"][
            "columns_datatype"
        ]
        table_has_geo_column = any(
            [
                table_col_dtype in socrata_geo_datatypes
                for table_col_dtype in table_column_datatypes
            ]
        )
        return table_has_geo_column

    def table_data_domain(self) -> str:
        return self.metadata.table_metadata["metadata"]["domain"]

    def table_has_tabular_data(self) -> bool:
        return len(self.metadata.table_metadata["resource"]["columns_name"]) != 0

    def table_has_geo_type_view(self) -> bool:
        return self.metadata.table_metadata["resource"]["lens_view_type"] == "geo"

    def table_has_map_type_display(self) -> bool:
        return self.metadata.table_metadata["resource"]["lens_display_type"] == "map"
