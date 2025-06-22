import pytest
import boto3
import json
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import from_json, col
import tempfile
import os
import sys

test_input_path = "test-input-bucket"
test_output_path = "test-output-bucket"
sys.argv = ["analyze.py", test_input_path, test_output_path]

# Import the script to test
from analyze import invoke_claude, invoke_claude_with_backoff, run_agent_pipeline, ensure_all_fields

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (SparkSession.builder
            .master("local[2]")
            .appName("TestLogTemplateAgentPipeline")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
            .getOrCreate())

@pytest.fixture
def s3_test_bucket():
    """Create a test bucket and clean it up after tests."""
    bucket_name = f"test-bucket-{os.urandom(8).hex()}"
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name

    # Clean up
    objects = s3.list_objects_v2(Bucket=bucket_name).get('Contents', [])
    for obj in objects:
        s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
    s3.delete_bucket(Bucket=bucket_name)

@pytest.fixture
def sample_log_data():
    return [
        {"template": "Error connecting to database", "count": 5},
        {"template": "User authentication failed", "count": 3},
        {"template": "Service started successfully", "count": 10}
    ]

class TestLogAnalysisPipeline:
    @pytest.mark.integration
    def test_s3_integration(self, s3_test_bucket, sample_log_data):
        """Test S3 read/write operations"""
        s3 = boto3.client('s3')

        # Upload test data
        test_key = "2024/03/21/test-template-summary.json"
        s3.put_object(
            Bucket=s3_test_bucket,
            Key=test_key,
            Body="\n".join(json.dumps(item) for item in sample_log_data)
        )

        # Verify file exists
        response = s3.list_objects_v2(Bucket=s3_test_bucket)
        assert len(response['Contents']) == 1
        assert response['Contents'][0]['Key'] == test_key

    @pytest.mark.integration
    def test_bedrock_integration(self):
        """Test Bedrock API integration"""
        with patch('boto3.client') as mock_client:
            mock_bedrock = Mock()
            mock_response = {
                'body': MagicMock(
                    read=Mock(return_value=json.dumps({
                        "content": [{"text": json.dumps([{
                            "patternName": "Database Error",
                            "level": "ERROR",
                            "count": 5,
                            "template": "Error connecting to database",
                            "possibleRootCause": "Network issues",
                            "impact": "Service disruption",
                            "relationships": "None"
                        }])}]
                    }))
                )
            }
            mock_bedrock.invoke_model.return_value = mock_response
            mock_client.return_value = mock_bedrock

            result = invoke_claude("Test prompt")
            parsed_result = json.loads(result)
            assert isinstance(parsed_result, list)
            assert len(parsed_result) == 1
            assert parsed_result[0]["level"] == "ERROR"

    @pytest.mark.integration
    def test_full_pipeline(self, spark, s3_test_bucket, sample_log_data):
        """Test the complete pipeline integration"""
        s3 = boto3.client('s3')

        # Setup test data in S3
        input_key = "2024/03/21/test-template-summary.json"
        s3.put_object(
            Bucket=s3_test_bucket,
            Key=input_key,
            Body="\n".join(json.dumps(item) for item in sample_log_data)
        )

        # Mock Bedrock responses
        with patch('boto3.client') as mock_client:
            mock_bedrock = Mock()
            mock_response = {
                'body': MagicMock(
                    read=Mock(return_value=json.dumps({
                        "content": [{"text": json.dumps([{
                            "patternName": "Test Pattern",
                            "level": "ERROR",
                            "count": 5,
                            "template": sample_log_data[0]["template"],
                            "possibleRootCause": "Network issues",
                            "impact": "Service disruption",
                            "relationships": "None"
                        }])}]
                    }))
                )
            }
            mock_bedrock.invoke_model.return_value = mock_response
            mock_client.return_value = mock_bedrock

            # Read data using Spark
            input_path = f"s3://{s3_test_bucket}/{input_key}"
            df = spark.read.json(input_path)

            # Process templates
            templates = df.select("template").distinct().collect()
            template_list = [row.template for row in templates]

            # Run pipeline on templates
            results = []
            for batch in [template_list]:
                raw_result = run_agent_pipeline(batch)
                validated_result = [ensure_all_fields(r) for r in raw_result if isinstance(r, dict)]
                results.extend(validated_result)

            # Verify results
            assert len(results) > 0
            for result in results:
                assert all(key in result for key in [
                    "patternName", "level", "count", "template",
                    "possibleRootCause", "impact", "relationships"
                ])

    @pytest.mark.integration
    def test_error_handling(self, spark, s3_test_bucket):
        """Test error handling in the pipeline"""
        with patch('boto3.client') as mock_client:
            mock_bedrock = Mock()
            mock_bedrock.invoke_model.side_effect = Exception("Bedrock API Error")
            mock_client.return_value = mock_bedrock

            # Test with invalid data
            invalid_templates = ["Invalid template 1", "Invalid template 2"]

            # Verify error handling
            with pytest.raises(Exception) as exc_info:
                run_agent_pipeline(invalid_templates)
            assert "Bedrock API Error" in str(exc_info.value)

def test_field_validation():
    """Test field validation function"""
    test_data = {
        "patternName": "Test Pattern",
        "level": "ERROR",
        "template": "Test template"
    }

    result = ensure_all_fields(test_data)

    # Verify all required fields are present
    required_fields = [
        "patternName", "template", "possibleRootCause",
        "impact", "relationships", "count", "level"
    ]
    for field in required_fields:
        assert field in result

    # Verify default values for missing fields
    assert result["count"] is None
    assert result["possibleRootCause"] is None
