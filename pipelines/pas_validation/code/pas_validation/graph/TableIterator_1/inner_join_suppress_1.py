from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from pas_validation.functions import *

def inner_join_suppress_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.bronze_col_name") == col("in1.column_name")), "inner")\
        .select(col("in0.bronze_col_name").alias("bronze_column_name"))
