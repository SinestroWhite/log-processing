import * as cdk from "aws-cdk-lib"
import type { Construct } from "constructs"
import * as s3 from "aws-cdk-lib/aws-s3"
import * as dynamodb from "aws-cdk-lib/aws-dynamodb"
import * as lambda from "aws-cdk-lib/aws-lambda"
import * as lambdaNodejs from "aws-cdk-lib/aws-lambda-nodejs"
import * as iam from "aws-cdk-lib/aws-iam"
import * as stepfunctions from "aws-cdk-lib/aws-stepfunctions"
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks"
import * as path from "path"

import { createHash } from 'crypto';

export class LogProcessingCdkAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create a deterministic bucket name
    let s3BucketPrefix = 'logs-bucket'
    let maxS3BucketNameLength = 60
    let shortHash = LogProcessingCdkAppStack.hash(s3BucketPrefix).toString().slice(0, maxS3BucketNameLength-s3BucketPrefix.length)
    let s3BucketName = `${s3BucketPrefix}-${shortHash}`

    // S3 bucket for storing raw log files
    const logBucket = new s3.Bucket(this, "LogBucket", {
      bucketName: s3BucketName,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned: true,
    })

    // DynamoDB table to track processed files
    const processedFilesTable = new dynamodb.Table(this, "ProcessedFilesTable", {
      tableName: 'processed-files-table',
      partitionKey: { name: "fileKey", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    })

    // DynamoDB table to store enriched log entries
    const logEntriesTable = new dynamodb.Table(this, "LogEntriesTable", {
      tableName: 'log-entries-table',
      partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "timestamp", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    })

    // Create IAM role for Bedrock access
    const bedrockRole = new iam.Role(this, "BedrockAccessRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
    })

    bedrockRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonBedrockFullAccess"))
    bedrockRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"))

    // Lambda function to scan S3 for new/changed files
    const scanS3Function = new lambdaNodejs.NodejsFunction(this, "ScanS3Function", {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, "../lambda/scan-s3.ts"),
      handler: "handler",
      timeout: cdk.Duration.minutes(5),
      environment: {
        PROCESSED_FILES_TABLE: processedFilesTable.tableName,
        LOG_BUCKET: logBucket.bucketName,
      },
      bundling: {
        forceDockerBundling: false
      }
    })

    // Grant permissions
    logBucket.grantRead(scanS3Function)
    processedFilesTable.grantReadWriteData(scanS3Function)

    // Lambda function to process log files
    const processLogFileFunction = new lambdaNodejs.NodejsFunction(this, "ProcessLogFileFunction", {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, "../lambda/process-log-file.ts"),
      handler: "handler",
      timeout: cdk.Duration.minutes(5),
      memorySize: 1024,
      environment: {
        LOG_BUCKET_NAME: logBucket.bucketName,
        BEDROCK_MODEL_ID: 'anthropic.claude-3-sonnet-20240229-v1:0',
        REGION: 'eu-central-1'
      },
    })

    // Grant permissions
    logBucket.grantRead(processLogFileFunction)
    logEntriesTable.grantWriteData(processLogFileFunction)

    processLogFileFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: [
        'arn:aws:bedrock:eu-central-1::foundation-model/*'
      ]
    }));

    // // Lambda function to call Bedrock for classification and enrichment
    // const callBedrockFunction = new lambdaNodejs.NodejsFunction(this, "CallBedrockFunction", {
    //   runtime: lambda.Runtime.NODEJS_20_X,
    //   entry: path.join(__dirname, "../lambda/callBedrock.ts"),
    //   handler: "handler",
    //   timeout: cdk.Duration.minutes(5),
    //   memorySize: 1024,
    //   role: bedrockRole,
    //   environment: {
    //     LOG_ENTRIES_TABLE: logEntriesTable.tableName,
    //   },
    // })
    //
    // // Grant permissions
    // logEntriesTable.grantReadWriteData(callBedrockFunction)
    //
    // // Step Functions workflow definition
    // const scanS3Task = new tasks.LambdaInvoke(this, "ScanS3Task", {
    //   lambdaFunction: scanS3Function,
    //   outputPath: "$.Payload",
    // })
    //
    // const processLogFileTask = new tasks.LambdaInvoke(this, "ProcessLogFileTask", {
    //   lambdaFunction: processLogFileFunction,
    //   outputPath: "$.Payload",
    // })
    //
    // const callBedrockTask = new tasks.LambdaInvoke(this, "CallBedrockTask", {
    //   lambdaFunction: callBedrockFunction,
    //   outputPath: "$.Payload",
    // })
    //
    // const processFilesMap = new stepfunctions.Map(this, "ProcessFilesMap", {
    //   maxConcurrency: 10,
    //   itemsPath: "$.filesToProcess",
    // })
    //
    // processFilesMap.iterator(processLogFileTask.next(callBedrockTask))
    //
    // const noFilesToProcessChoice = new stepfunctions.Choice(this, "NoFilesToProcess")
    //     .when(stepfunctions.Condition.isPresent("$.filesToProcess[0]"), processFilesMap)
    //     .otherwise(new stepfunctions.Succeed(this, "NoNewFilesToProcess"))
    //
    // const workflow = new stepfunctions.StateMachine(this, "LogProcessingWorkflow", {
    //   definition: scanS3Task.next(noFilesToProcessChoice),
    //   timeout: cdk.Duration.minutes(30),
    // })
    //
    // // Outputs
    // new cdk.CfnOutput(this, "LogBucketName", {
    //   value: logBucket.bucketName,
    //   description: "Name of the S3 bucket where log files are stored",
    // })
    //
    // new cdk.CfnOutput(this, "ProcessedFilesTableName", {
    //   value: processedFilesTable.tableName,
    //   description: "Name of the DynamoDB table tracking processed files",
    // })
    //
    // new cdk.CfnOutput(this, "LogEntriesTableName", {
    //   value: logEntriesTable.tableName,
    //   description: "Name of the DynamoDB table storing enriched log entries",
    // })
    //
    // new cdk.CfnOutput(this, "StateMachineArn", {
    //   value: workflow.stateMachineArn,
    //   description: "ARN of the Step Functions state machine",
    // })
  }

  private static hash(string: string): string {
    return createHash('sha256').update(string).digest('hex');
  }
}
