import pytest
import boto3
import json
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import from_json, col
from datetime import datetime
import tempfile
import os
import sys

# Import the functions from your main script
from analyze import (
    invoke_claude,
    invoke_claude_with_backoff,
    run_agent_pipeline,
    ensure_all_fields
)

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (SparkSession.builder
            .master("local[2]")
            .appName("TestLogTemplateAgentPipeline")
            .getOrCreate())

@pytest.fixture
def mock_s3():
    """Create a mocked S3 client."""
    with patch('boto3.client') as mock_client:
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        yield mock_s3

@pytest.fixture
def mock_bedrock():
    """Create a mocked Bedrock client."""
    with patch('boto3.client') as mock_client:
        mock_bedrock = Mock()
        mock_client.return_value = mock_bedrock
        yield mock_bedrock

@pytest.fixture
def sample_template_data():
    """Provide sample template data for testing."""
    return [
        '{"template": "Error connecting to database", "count": 5}',
        '{"template": "User authentication failed", "count": 3}',
        '{"template": "Service started successfully", "count": 10}'
    ]

@pytest.fixture
def sample_s3_structure(mock_s3, sample_template_data):
    """Set up mock S3 structure with sample data."""
    mock_s3.get_paginator.return_value.paginate.return_value = [{
        'Contents': [
            {'Key': '2024/03/20/template-summary-1.json'},
            {'Key': '2024/03/20/template-summary-2.json'}
        ]
    }]
    return mock_s3

def test_list_template_files(sample_s3_structure):
    """Test listing template files from S3."""
    paginator = sample_s3_structure.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket='test-bucket')

    all_files = []
    for page in pages:
        for obj in page.get('Contents', []):
            if '-template-summary' in obj['Key']:
                all_files.append(f"s3://test-bucket/{obj['Key']}")

    assert len(all_files) == 2
    assert all('-template-summary' in f for f in all_files)

@pytest.mark.integration
def test_spark_dataframe_processing(spark, sample_template_data):
    """Test processing of template data with Spark."""
    # Create temporary file with sample data
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        for line in sample_template_data:
            f.write(line + '\n')

    try:
        # Read the data with Spark
        df = spark.read.text(f.name).withColumnRenamed("value", "raw")

        # Test schema
        schema = StructType().add("template", StringType()).add("count", IntegerType())
        df_parsed = df.withColumn("parsed", from_json(col("raw"), schema))
        df_templates = df_parsed.select(col("parsed.template").alias("template")).distinct()

        templates = df_templates.rdd.map(lambda row: row.template).collect()
        assert len(templates) == 3
        assert "Error connecting to database" in templates
    finally:
        os.unlink(f.name)

@pytest.mark.integration
def test_claude_invocation(mock_bedrock):
    """Test Claude model invocation."""
    mock_response = {
        'body': Mock(
            read=Mock(return_value=json.dumps({
                "content": [{"text": json.dumps([{
                    "patternName": "Database Connection Error",
                    "level": "ERROR",
                    "count": 5,
                    "template": "Error connecting to database",
                    "possibleRootCause": "Network connectivity issues",
                    "impact": "Service unavailability",
                    "relationships": "Related to authentication failures"
                }])}]
            }))
        )
    }
    mock_bedrock.invoke_model.return_value = mock_response

    result = invoke_claude("Test prompt")
    assert isinstance(result, str)
    assert "Database Connection Error" in result

@pytest.mark.integration
def test_claude_with_backoff(mock_bedrock):
    """Test Claude invocation with backoff retry logic."""
    mock_bedrock.invoke_model.side_effect = [
        Exception("ThrottlingException"),
        Exception("ThrottlingException"),
        Mock(body=Mock(
            read=Mock(return_value=json.dumps({
                "content": [{"text": "Success"}]
            }))
        ))
    ]

    result = invoke_claude_with_backoff("Test prompt", retries=3, base_delay=0.1)
    assert result == "Success"
    assert mock_bedrock.invoke_model.call_count == 3

def test_ensure_all_fields():
    """Test field validation function."""
    test_row = {
        "patternName": "Test Pattern",
        "level": "ERROR",
        "template": "Test template",
        "count": 5
    }

    result = ensure_all_fields(test_row)
    assert all(k in result for k in [
        "patternName", "template", "possibleRootCause",
        "impact", "relationships", "count", "level"
    ])
    assert result["patternName"] == "Test Pattern"
    assert result["possibleRootCause"] is None  # Missing field should be None

@pytest.mark.integration
def test_full_pipeline_integration(spark, mock_bedrock, sample_template_data):
    """Test the full analysis pipeline integration."""
    # Set up mock Bedrock response
    mock_bedrock.invoke_model.return_value = Mock(
        body=Mock(
            read=Mock(return_value=json.dumps({
                "content": [{"text": json.dumps([{
                    "patternName": "Test Pattern",
                    "level": "ERROR",
                    "count": 5,
                    "template": "Error connecting to database",
                    "possibleRootCause": "Network issues",
                    "impact": "Service disruption",
                    "relationships": "None"
                }])}]
            }))
        )
    )

    # Create test data
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        for line in sample_template_data:
            f.write(line + '\n')

    try:
        # Process the data
        df = spark.read.text(f.name).withColumnRenamed("value", "raw")
        schema = StructType().add("template", StringType()).add("count", IntegerType())
        df_parsed = df.withColumn("parsed", from_json(col("raw"), schema))
        df_templates = df_parsed.select(col("parsed.template").alias("template")).distinct()

        templates = df_templates.rdd.map(lambda row: row.template).collect()
        batches = [templates[i:i+2] for i in range(0, len(templates), 2)]

        results = []
        for batch in batches:
            raw_result = run_agent_pipeline(batch)
            validated_result = [ensure_all_fields(r) for r in raw_result if isinstance(r, dict)]
            results.extend(validated_result)

        assert len(results) > 0
        assert all(isinstance(r, dict) for r in results)
    finally:
        spark.stop()
