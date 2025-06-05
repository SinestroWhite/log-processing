from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from drain3.template_miner_config import TemplateMinerConfig
from drain3 import TemplateMiner
from os.path import dirname
from datetime import datetime
import sys
import boto3

print("Script arguments:", sys.argv)
if len(sys.argv) < 3:
    print("Not enough arguments! Exiting.")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

print(f"input_path: {input_path}")
print(f"output_path: {output_path}")

s3 = boto3.client('s3')

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=input_path)

all_files = []
for page in pages:
    for obj in page.get('Contents', []):
        all_files.append(f"s3://{input_path}/{obj['Key']}")

print("Found", len(all_files), "files:")
for key in all_files:
    print(key)

spark = SparkSession.builder \
    .appName("DrainFullLineByPartition") \
    .getOrCreate()

df = spark.read.text(all_files).withColumnRenamed("value", "raw")

print("Raw input preview:")
df.show(10, truncate=False)

print("df.count() =", df.count())

schema = StructType([
    StructField("raw", StringType(), True),
    StructField("template", StringType(), True)
])

def process_partition(partition):
    config = TemplateMinerConfig()
    config.load(f"{dirname(__file__)}/drain3.ini")
    miner = TemplateMiner(config=config)

    for row in partition:
        raw_line = row.raw
        result = miner.add_log_message(raw_line)
        template = result["template_mined"] if result and "template_mined" in result else None
        yield (raw_line, template)

rdd_result = df.rdd.mapPartitions(process_partition)
df_result = spark.createDataFrame(rdd_result, schema)

if df_result is None:
    print("df_result is None")
    sys.exit(0)

if df_result.rdd.isEmpty():
    print("df_result is empty")
    print("df_result schema:")
    df_result.printSchema()
else:
    print("df_result contains data (showing first 20 lines):")
    df_result.show(20, truncate=False)

    now_utc = datetime.utcnow()
    partition_path = now_utc.strftime("%Y/%m/%d/")
    timestamp_str = now_utc.strftime("%Y-%m-%dT%H-%M-%S")

    print(f"Saving to: s3://{output_path.strip()}/{partition_path}{timestamp_str}-logs")
    df_result.write.mode("append").json(f"s3://{output_path.strip()}/{partition_path}{timestamp_str}-logs")

    template_counts = df_result.groupBy("template").count().orderBy("count", ascending=False)
    template_counts.write.mode("append").json(f"s3://{output_path.strip()}/{partition_path}{timestamp_str}-template-summary")

spark.stop()
