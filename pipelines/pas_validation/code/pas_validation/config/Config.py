from pas_validation.graph.TableIterator_1.config.Config import SubgraphConfig as TableIterator_1_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            mapping_catalog: str=None,
            mapping_schema: str=None,
            mapping_table: str=None,
            validation_catalog: str=None,
            validation_schema: str=None,
            validation_table: str=None,
            TableIterator_1: dict=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            mapping_catalog, 
            mapping_schema, 
            mapping_table, 
            validation_catalog, 
            validation_schema, 
            validation_table, 
            TableIterator_1
        )

    def update(
            self,
            mapping_catalog: str="dfinch",
            mapping_schema: str="pad",
            mapping_table: str="table_mappings",
            validation_catalog: str="dfinch",
            validation_schema: str="pad",
            validation_table: str="validation_output",
            TableIterator_1: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.mapping_catalog = mapping_catalog
        self.mapping_schema = mapping_schema
        self.mapping_table = mapping_table
        self.validation_catalog = validation_catalog
        self.validation_schema = validation_schema
        self.validation_table = validation_table
        self.TableIterator_1 = self.get_config_object(
            prophecy_spark, 
            TableIterator_1_Config(prophecy_spark = prophecy_spark), 
            TableIterator_1, 
            TableIterator_1_Config
        )
        pass
