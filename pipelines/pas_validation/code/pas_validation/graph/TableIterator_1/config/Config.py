from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            mapping_catalog: str="dfinch",
            mapping_schema: str="pad",
            mapping_table: str="table_mappings",
            validation_catalog: str="dfinch",
            validation_schema: str="pad",
            validation_table: str="validation_output",
            bronze_catalog: str="",
            bronze_schema: str="",
            bronze_table: str="",
            bronze_path: str="",
            silver_catalog: str="",
            silver_schema: str="",
            silver_table: str="",
            silver_path: str="",
            suppress_columns: list=[],
            **kwargs
    ):
        self.mapping_catalog = mapping_catalog
        self.mapping_schema = mapping_schema
        self.mapping_table = mapping_table
        self.validation_catalog = validation_catalog
        self.validation_schema = validation_schema
        self.validation_table = validation_table
        self.bronze_catalog = bronze_catalog
        self.bronze_schema = bronze_schema
        self.bronze_table = bronze_table
        self.bronze_path = bronze_path
        self.silver_catalog = silver_catalog
        self.silver_schema = silver_schema
        self.silver_table = silver_table
        self.silver_path = silver_path
        self.suppress_columns = suppress_columns
        pass

    def update(self, updated_config):
        self.mapping_catalog = updated_config.mapping_catalog
        self.mapping_schema = updated_config.mapping_schema
        self.mapping_table = updated_config.mapping_table
        self.validation_catalog = updated_config.validation_catalog
        self.validation_schema = updated_config.validation_schema
        self.validation_table = updated_config.validation_table
        self.bronze_catalog = updated_config.bronze_catalog
        self.bronze_schema = updated_config.bronze_schema
        self.bronze_table = updated_config.bronze_table
        self.bronze_path = updated_config.bronze_path
        self.silver_catalog = updated_config.silver_catalog
        self.silver_schema = updated_config.silver_schema
        self.silver_table = updated_config.silver_table
        self.silver_path = updated_config.silver_path
        self.suppress_columns = updated_config.suppress_columns
        pass

Config = SubgraphConfig()
