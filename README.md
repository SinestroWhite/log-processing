# Welcome to your CDK TypeScript project

This is a blank project for CDK development with TypeScript.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `npx cdk deploy`  deploy this stack to your default AWS account/region
* `npx cdk diff`    compare deployed stack with current state
* `npx cdk synth`   emits the synthesized CloudFormation template

## Run EMR Serverless Jobs Manually

### Start log processing (template mining)
```bash
aws emr-serverless start-job-run \
    --region eu-central-1 \
    --application-id "00ft25t5p3a9jg15" \
    --name "SparkJobRun" \
    --execution-role-arn "arn:aws:iam::278838288143:role/LogProcessingStack-EmrJobRoleD77C0CE1-wjaGCBhPmugN" \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://logprocessingstack-code-bucket/log-processing/drain3-process.py",
            "entryPointArguments": [
                "logprocessingstack-raw-logs",
                "logprocessingstack-processed-logs"
                ],
            "sparkSubmitParameters": "--conf spark.archives=s3://logprocessingstack-code-bucket/pyspark_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
        }
    }'
```

### Start log analysis (with bedrock)
```bash
  aws emr-serverless start-job-run \
    --region eu-central-1 \
    --application-id "00ft25t5p3a9jg15" \
    --name "SparkJobRun" \
    --execution-role-arn "arn:aws:iam::278838288143:role/LogProcessingStack-EmrJobRoleD77C0CE1-wjaGCBhPmugN" \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://logprocessingstack-code-bucket/log-analysis/analyze.py",
            "entryPointArguments": [
                "logprocessingstack-processed-logs",
                "logprocessingstack-enriched-logs"
                ],
            "sparkSubmitParameters": "--conf spark.archives=s3://logprocessingstack-code-bucket/pyspark_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
        }
    }'
```