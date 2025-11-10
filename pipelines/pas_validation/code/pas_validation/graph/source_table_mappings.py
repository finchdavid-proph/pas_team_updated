from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pas_validation.config.ConfigStore import *
from pas_validation.functions import *

def source_table_mappings(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.mapping_catalog}`.`{Config.mapping_schema}`.`{Config.mapping_table}`")
