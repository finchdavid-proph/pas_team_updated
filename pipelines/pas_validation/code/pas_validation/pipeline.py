from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pas_validation.config.ConfigStore import *
from pas_validation.functions import *
from prophecy.utils import *
from pas_validation.graph import *

def pipeline(spark: SparkSession) -> None:
    df_source_table_mappings = source_table_mappings(spark)
    TableIterator_1(Config.TableIterator_1).apply(spark, df_source_table_mappings)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("pas_validation").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pas_validation")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/pas_validation", config = Config)(pipeline)

if __name__ == "__main__":
    main()
