from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from drain3.template_miner_config import TemplateMinerConfig
from drain3 import TemplateMiner
from os.path import dirname
import sys

if len(sys.argv) != 3:
    print("Usage: spark-submit drain_full_line_partitioned.py <input_path> <output_path>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder \
    .appName("DrainFullLineByPartition") \
    .getOrCreate()

# Зареждаме raw логовете
df = spark.read.text(input_path).withColumnRenamed("value", "raw")

# schema за резултата
schema = StructType([
    StructField("raw", StringType(), True),
    StructField("template", StringType(), True)
])

def process_partition(partition):
    # всеки partition си създава собствен miner
    config = TemplateMinerConfig()
    config.load(f"{dirname(__file__)}/drain3.ini")
    miner = TemplateMiner(config=config)

    for row in partition:
        raw_line = row.raw
        result = miner.add_log_message(raw_line)
        template = result["template_mined"] if result and "template_mined" in result else None
        yield (raw_line, template)

# прилагаме върху партиции
rdd_result = df.rdd.mapPartitions(process_partition)
df_result = spark.createDataFrame(rdd_result, schema)

# записваме всички редове с шаблон
df_result.write.mode("overwrite").json(output_path)

# създаваме обобщение по шаблон
template_counts = df_result.groupBy("template").count().orderBy("count", ascending=False)
template_counts.write.mode("overwrite").json(output_path.rstrip("/") + "-template-summary")

spark.stop()
