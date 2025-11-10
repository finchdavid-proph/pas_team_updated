from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from pas_validation.functions import *
from . import *
from .config import *


class TableIterator_1(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__()

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_source_silver_table = source_silver_table(spark)
        df_silver_columns_1 = silver_columns_1(spark, df_source_silver_table)
        df_silver_rename_1 = silver_rename_1(spark, df_silver_columns_1)
        df_source_bronze_table = source_bronze_table(spark)
        df_bronze_columns_1 = bronze_columns_1(spark, df_source_bronze_table)
        df_bronze_distinct_counts_1 = bronze_distinct_counts_1(spark, df_source_bronze_table, df_bronze_columns_1)
        df_bronze_rename_1 = bronze_rename_1(spark, df_bronze_columns_1)
        df_join_bronze_silver_1 = join_bronze_silver_1(spark, df_bronze_rename_1, df_silver_rename_1)
        df_inner_join_on_column_1 = inner_join_on_column_1(spark, df_join_bronze_silver_1, df_bronze_distinct_counts_1)
        df_silver_distinct_counts_1 = silver_distinct_counts_1(spark, df_source_silver_table, df_silver_columns_1)
        df_compare_distinct_counts_1 = compare_distinct_counts_1(
            spark, 
            df_inner_join_on_column_1, 
            df_silver_distinct_counts_1
        )
        df_extract_suppressed_columns_1 = extract_suppressed_columns_1(spark, df_source_bronze_table)
        df_inner_join_suppress_1 = inner_join_suppress_1(spark, df_bronze_rename_1, df_extract_suppressed_columns_1)
        df_compare_suppress_1 = compare_suppress_1(spark, df_inner_join_suppress_1, df_silver_rename_1)
        df_reformat_bronze_path_2 = reformat_bronze_path_2(spark, df_compare_suppress_1)
        df_output_keep_1 = output_keep_1(spark, df_compare_distinct_counts_1)
        df_reformat_bronze_path_1_1 = reformat_bronze_path_1_1(spark, df_output_keep_1)
        df_SetOperation_1_1 = SetOperation_1_1(spark, df_reformat_bronze_path_2, df_reformat_bronze_path_1_1)
        subgraph_config.update(Config)

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> None:
        inDFs = []
        conf_to_column = dict(
            [("bronze_catalog", "bronze_catalog"),  ("bronze_schema", "bronze_schema"),  ("bronze_table", "bronze_table"),              ("bronze_path", "bronze_path"),  ("silver_catalog", "silver_catalog"),              ("silver_schema", "silver_schema"),  ("silver_table", "silver_table"),              ("silver_path", "silver_path"),  ("suppress_columns", "suppress_columns")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        import multiprocessing
        from multiprocessing.pool import ThreadPool
        from functools import partial

        with ThreadPool(processes = 5) as pool:

            def process_row(row, config, inDFs, spark):
                df1 = config.update_from_row_map(row, conf_to_column)

                return self.__run__(spark, df1, *inDFs)

            partial_process_row = partial(process_row, config = self.config, inDFs = [], spark = spark)
            results = pool.map(partial_process_row, in0.collect())

            return do_union(results)
