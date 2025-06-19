from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3
import json
import uuid
import time
import sys
from datetime import datetime
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

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
        if '-template-summary' in obj['Key']:
            all_files.append(f"s3://{input_path}/{obj['Key']}")

print("Found", len(all_files), "files:")
for key in all_files:
    print(key)

spark = SparkSession.builder.appName("LogTemplateAgentPipeline").getOrCreate()

df = spark.read.text(all_files).withColumnRenamed("value", "raw")

# Load processed logs
print("Raw logs:")
df.show(df.count(), truncate=False)

schema = StructType() \
    .add("template", StringType()) \
    .add("count", IntegerType())

df_parsed = df.withColumn("parsed", from_json(col("raw"), schema))
df_templates = df_parsed.select(col("parsed.template").alias("template")).distinct()

# Collect templates and chunk into batches
batch_size = 5
templates = df_templates.rdd.map(lambda row: row.template).collect()
batches = [templates[i:i+batch_size] for i in range(0, len(templates), batch_size)]

# Initialize Bedrock client
bedrock = boto3.client('bedrock-runtime', region_name='eu-central-1')

def invoke_claude(prompt: str) -> str:
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 50000,
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
    }
    print(f"Sending claude request: {body}")
    response = bedrock.invoke_model(
        modelId="anthropic.claude-3-haiku-20240307-v1:0", #"anthropic.claude-3-sonnet-20240229-v1:0",
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json"
    )
    return json.loads(response['body'].read()).get("content", [])[0].get("text", "")

def invoke_claude_with_backoff(prompt: str, retries=5, base_delay=2):
    for attempt in range(retries):
        try:
            return invoke_claude(prompt)
        except Exception as e:
            if "ThrottlingException" in str(e):
                delay = base_delay * (2 ** attempt)
                print(f"[Throttled] Retrying after {delay}s (attempt {attempt+1})")
                time.sleep(delay)
            else:
                raise
    return {}


# Define agent steps
def run_agent_pipeline(log_batch):
    logs_as_text = "\n".join(log_batch)

    # Agent 1: Pattern Identification
    # prompt1 = (
    #     "You are a log pattern identifier. Analyze these logs and:\n"
    #     "1. Identify the 5-7 most significant patterns or issues\n"
    #     "2. For each pattern, provide 2-3 representative examples\n"
    #     "3. Formulate 3 specific questions that would help understand each pattern\n"
    #     "Output in JSON format with 'patterns' array containing objects with 'name', 'description', 'examples', and 'questions'.\n\n"
    #     f"Logs:\n{logs_as_text}"
    # )
    # print("Invoked claude prompt 1:")
    # result1 = invoke_claude_with_backoff(prompt1)
    # print(f"Result1: {result1}")
    # patterns = json.loads(result1).get("patterns", [])
    # print(f"Patterns1: {result1}")

    # Agent 2: Deep Analysis
    prompt2 = (
        "You are a deep log analyzer. Based on the patterns and questions identified:\n"
        "1. Analyze each template in detail\n"
        "2. Determine if this template is from an error and if yes, what are root causes and contributing factors\n"
        "3. Assess the business and technical impact\n"
        "4. Explain relationships between different templates\n"
        "5. Output them in a prioritized list (the most important is at the top)\n"
        "6. Determine the level of the template (how critical is the template)\n"
        "Do not output anything else except for JSON. Keep in mind JSON does not support multi-line strings so do not put new lines in string values.\n"
        "Output in JSON format only an array containing objects with 'patternName' (string type), 'level' (one of INFO, WARNING, ERROR, CRITICAL do not make up other levels), 'count' (for the count field copy the input count value you are analyzing, do not make up new information for this field) (number type),'template' (for the template field copy the input template value you are analyzing, do not make up new information for this field) (string type), 'possibleRootCause' (string type), 'impact' (string type), and 'relationships' (string type).\n\n"
        f"log templates:\n{logs_as_text}"
    )
    # time.sleep(2)
    print("Invoked claude prompt 2:")
    result2 = invoke_claude_with_backoff(prompt2)
    print(f"Result2: {result2}")
    analyses = json.loads(result2)
    print(f"Analyses: {result2}")

    # Agent 3: Action Planning
    # prompt3 = (
    #     "You are an action planner. Based on the detailed analysis:\n"
    #     "1. Create a prioritized remediation plan for each issue\n"
    #     "2. Specify immediate actions vs. long-term fixes\n"
    #     "3. Identify required resources and expertise\n"
    #     "Output in JSON format with 'actions' array containing objects with 'patternName', 'template' (for template copy the input template you are analyzing, do not make up new information for this field), 'priority', 'immediateActions', 'longTermFixes', 'resources', and 'successCriteria'.\n\n"
    #     f"Analysis:\n{json.dumps(analyses)}"
    # )
    # time.sleep(2)
    # print("Invoked claude prompt 3:")
    # result3 = invoke_claude_with_backoff(prompt3)
    # print(f"Result3: {result3}")
    # actions = json.loads(result3).get("actions", [])
    # print(f"Actions: {result3}")

    # Get me analyses from analyses
    return analyses

def ensure_all_fields(row):
    default_keys = ["patternName", "template", "possibleRootCause", "impact", "relationships", "count", "level"]
    return {k: row.get(k, None) for k in default_keys}

# Process each batch and collect results
results = []
for batch in batches:
    try:
        raw_result = run_agent_pipeline(batch)
        validated_result = [ensure_all_fields(r) for r in raw_result if isinstance(r, dict)]
        results.extend(validated_result)
    except Exception as e:
        print(f"Error: {e}")

# Save to S3
results_df = spark.createDataFrame(results)
print("Results:")
results_df.show()

now_utc = datetime.utcnow()
partition_path = now_utc.strftime("%Y/%m/%d/")
timestamp_str = now_utc.strftime("%Y-%m-%dT%H-%M-%S")

print(f"Saving to: s3://{output_path.strip()}/{partition_path}{timestamp_str}-log-analysis")
results_df.write.mode("append").json(f"s3://{output_path.strip()}/{partition_path}{timestamp_str}-log-analysis")
