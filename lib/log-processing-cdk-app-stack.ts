import * as cdk from "aws-cdk-lib"
import * as s3 from "aws-cdk-lib/aws-s3"
import * as kinesisFirehose from "aws-cdk-lib/aws-kinesisfirehose"
import * as emrserverless from 'aws-cdk-lib/aws-emrserverless';
import * as stepfunctions from "aws-cdk-lib/aws-stepfunctions"
import * as stepfunctionsTasks from "aws-cdk-lib/aws-stepfunctions-tasks"
import * as glue from "aws-cdk-lib/aws-glue"
import * as athena from "aws-cdk-lib/aws-athena"
import * as iam from "aws-cdk-lib/aws-iam"
import * as logs from "aws-cdk-lib/aws-logs"
import * as events from "aws-cdk-lib/aws-events"
import * as targets from 'aws-cdk-lib/aws-events-targets';
import type { Construct } from "constructs"
import {Stack} from "aws-cdk-lib";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as path from "node:path";

export class LogProcessingStack extends Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props)

    // S3 Buckets for different stages
    const rawLogsBucket = new s3.Bucket(this, "RawLogsBucket", {
      bucketName: `${this.stackName.toLowerCase()}-raw-logs`,
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          id: "raw-logs-lifecycle",
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30),
            }
          ],
          expiration: cdk.Duration.days(365),
        },
      ],
    })

    const processedBucket = new s3.Bucket(this, "ProcessedBucket", {
      bucketName: `${this.stackName.toLowerCase()}-processed-logs`,
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          id: "processed-logs-lifecycle",
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30),
            },
          ],
          expiration: cdk.Duration.days(365),
        },
      ],
    })

    const enrichedBucket = new s3.Bucket(this, "EnrichedBucket", {
      bucketName: `${this.stackName.toLowerCase()}-enriched-logs`,
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          id: "enriched-logs-lifecycle",
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(90),
            },
          ],
          expiration: cdk.Duration.days(365),
        },
      ],
    })

    // Kinesis Data Firehose for log ingestion
    const firehoseRole = new iam.Role(this, "FirehoseRole", {
      assumedBy: new iam.ServicePrincipal("firehose.amazonaws.com"),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess")],
    })

    const logFirehose = new kinesisFirehose.CfnDeliveryStream(this, "LogFirehose", {
      deliveryStreamName: `${this.stackName.toLowerCase()}-log-ingestion`,
      deliveryStreamType: "DirectPut",
      extendedS3DestinationConfiguration: {
        bucketArn: rawLogsBucket.bucketArn,
        prefix: '!{timestamp:yyyy/MM/dd}/',
        errorOutputPrefix: "errors/!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd}/",
        bufferingHints: {
          sizeInMBs: 128,
          intervalInSeconds: 300,
        },
        compressionFormat: "GZIP",
        roleArn: firehoseRole.roleArn,
      },
    });

    const emrApplication = new emrserverless.CfnApplication(this, "EMRApplication", {
      name: "log-template-mining",
      type: "Spark",
      releaseLabel: "emr-6.15.0",
      maximumCapacity: {
        cpu: "4 vCPU", // At 200 TBs/day it should be 2000 vCPUs
        memory: "16 GB", // At 200 TBs/day it should be 4100 GBs
      },
      autoStartConfiguration: {
        enabled: true,
      },
      autoStopConfiguration: {
        enabled: true,
        idleTimeoutMinutes: 15,
      },
    });

    const emrLogGroup = new logs.LogGroup(this, 'EmrDrainLogGroup', {
      logGroupName: '/emr-serverless/drain-jobs',
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const emrCodeBucket = new s3.Bucket(this, 'EMRCodeBucket', {
      bucketName: `${this.stackName.toLowerCase()}-code-bucket`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const emrCodeDeployment = new s3deploy.BucketDeployment(this, 'DeployJobCode', {
      destinationBucket: emrCodeBucket,
      sources: [s3deploy.Source.asset(path.join(__dirname, '..', 'spark-jobs', 'pyspark_venv.zip'))],
    });

    const jobRole = new iam.Role(this, 'EmrJobRole', {
      assumedBy: new iam.ServicePrincipal('emr-serverless.amazonaws.com'),
    });
    rawLogsBucket.grantRead(jobRole);
    processedBucket.grantReadWrite(jobRole);
    emrLogGroup.grantWrite(jobRole);
    emrCodeBucket.grantRead(jobRole);

    // EventBridge rule to start the job daily at 02:00 UTC
    // At 200TBs/day it should run every 25 minutes
    const emrRule = new events.Rule(this, 'DailyDrain', {
      schedule: events.Schedule.cron({ minute: '0', hour: '2' }),
      targets: [
        new targets.AwsApi({
          service: 'EMR-Serverless',
          action: 'startJobRun',
          policyStatement: new iam.PolicyStatement({
            actions: ['emr-serverless:StartJobRun'],
            resources: [`${emrApplication.attrArn}/*`],
          }),
          parameters: {
            applicationId: emrApplication.ref,
            executionRoleArn: jobRole.roleArn,
            jobDriver: {
              sparkSubmit: {
                entryPoint: `s3://${emrCodeBucket.bucketName}/`,
                entryPointArguments: [
                  `--input-prefix=s3://${rawLogsBucket.bucketName}/`,
                  `--output-prefix=s3://${processedBucket.bucketName}/`,
                  '--partitions', '4000', // At 200 TBs/day it should be 8000 partitions (partitions ≈ 2 × vCPU)
                ],
                sparkSubmitParameters:
                    '--conf spark.executor.memoryOverhead=1G ' +
                    '--conf spark.serializer=org.apache.spark.serializer.KryoSerializer',
              },
            },
            configurationOverrides: {
              monitoringConfiguration: {
                cloudWatchLoggingConfiguration: {
                  enabled: true,
                  logGroupName: emrLogGroup.logGroupName,
                  logStreamNamePrefix: 'run',
                },
              },
            },
          },
        }),
      ],
    });
    emrRule.node.addDependency(emrCodeDeployment);

    //
    // // Glue Database and Tables
    // const glueDatabase = new glue.CfnDatabase(this, "LogDatabase", {
    //   catalogId: this.account,
    //   databaseInput: {
    //     name: "log_processing_db",
    //     description: "Database for log processing pipeline",
    //   },
    // })
    //
    // const processedRowsTable = new glue.CfnTable(this, "ProcessedRowsTable", {
    //   catalogId: this.account,
    //   databaseName: glueDatabase.ref,
    //   tableInput: {
    //     name: "processed_rows",
    //     description: "Processed log rows with template IDs",
    //     storageDescriptor: {
    //       columns: [
    //         { name: "template_id", type: "string" },
    //         { name: "template_text", type: "string" },
    //         { name: "ts", type: "timestamp" },
    //         { name: "service", type: "string" },
    //         { name: "level", type: "string" },
    //         { name: "message", type: "string" },
    //         { name: "parameters", type: "array<string>" },
    //       ],
    //       location: `s3://${processedBucket.bucketName}/processed_rows/`,
    //       inputFormat: "org.apache.hadoop.mapred.TextInputFormat",
    //       outputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    //       serdeInfo: {
    //         serializationLibrary: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    //       },
    //     },
    //     partitionKeys: [
    //       { name: "year", type: "string" },
    //       { name: "month", type: "string" },
    //       { name: "day", type: "string" },
    //     ],
    //   },
    // })
    //
    // const templateStatsTable = new glue.CfnTable(this, "TemplateStatsTable", {
    //   catalogId: this.account,
    //   databaseName: glueDatabase.ref,
    //   tableInput: {
    //     name: "template_stats",
    //     description: "Template statistics for Bedrock analysis",
    //     storageDescriptor: {
    //       columns: [
    //         { name: "template_id", type: "string" },
    //         { name: "template_text", type: "string" },
    //         { name: "cnt_24h", type: "bigint" },
    //         { name: "cnt_7d", type: "bigint" },
    //         { name: "rarity", type: "double" },
    //         { name: "first_seen", type: "timestamp" },
    //         { name: "last_seen", type: "timestamp" },
    //         { name: "service", type: "string" },
    //       ],
    //       location: `s3://${processedBucket.bucketName}/template_stats/`,
    //       inputFormat: "org.apache.hadoop.mapred.TextInputFormat",
    //       outputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    //       serdeInfo: {
    //         serializationLibrary: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    //       },
    //     },
    //     partitionKeys: [{ name: "date", type: "string" }],
    //   },
    // })
    //
    // // Step Functions for Bedrock triage
    // const bedrockTriageRole = new iam.Role(this, "BedrockTriageRole", {
    //   assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
    //   managedPolicies: [
    //     iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess"),
    //     iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonBedrockFullAccess"),
    //   ],
    // })
    //
    // // Step Functions state machine definition
    // const readTemplateStats = new stepfunctionsTasks.AthenaStartQueryExecution(this, "ReadTemplateStats", {
    //   queryString: stepfunctions.JsonPath.format(
    //       "SELECT template_id, template_text, cnt_24h, cnt_7d, rarity FROM template_stats WHERE date = '{}'",
    //       stepfunctions.JsonPath.stringAt("$.date"),
    //   ),
    //   queryExecutionContext: {
    //     database: glueDatabase.ref,
    //   },
    //   resultConfiguration: {
    //     outputLocation: `s3://${processedBucket.bucketName}/athena-results/`,
    //   },
    // })
    //
    // const bedrockTriage = new stepfunctionsTasks.BedrockInvokeModel(this, "BedrockTriage", {
    //   model: stepfunctionsTasks.BedrockModel.fromString("anthropic.claude-3-sonnet-20240229-v1:0"),
    //   body: stepfunctions.TaskInput.fromObject({
    //     anthropic_version: "bedrock-2023-05-31",
    //     max_tokens: 4000,
    //     messages: [
    //       {
    //         role: "user",
    //         content: stepfunctions.JsonPath.format(
    //             "You are a log-analysis expert. For each JSON object decide if the template is IMPORTANT, SUSPICIOUS, or NOISE.\n\n{}",
    //             stepfunctions.JsonPath.stringAt("$.templates"),
    //         ),
    //       },
    //     ],
    //   }),
    //   resultSelector: {
    //     "classifications.$": "States.StringToJson($.Body.content[0].text)",
    //   },
    // })
    //
    // const saveClassifications = new stepfunctionsTasks.S3PutObject(this, "SaveClassifications", {
    //   bucket: processedBucket,
    //   key: stepfunctions.JsonPath.format(
    //       "interesting/date={}/classifications.jsonl",
    //       stepfunctions.JsonPath.stringAt("$.date"),
    //   ),
    //   body: stepfunctions.TaskInput.fromJsonPathAt("$.classifications"),
    // })
    //
    // const triageWorkflow = new stepfunctions.StateMachine(this, "TriageWorkflow", {
    //   stateMachineName: "bedrock-triage-workflow",
    //   role: bedrockTriageRole,
    //   definition: readTemplateStats.next(bedrockTriage).next(saveClassifications),
    // })
    //
    // // EventBridge rule to trigger EMR job daily
    // const emrTriggerRole = new iam.Role(this, "EMRTriggerRole", {
    //   assumedBy: new iam.ServicePrincipal("events.amazonaws.com"),
    //   managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonEMRServerlessServiceRolePolicy")],
    // })
    //
    // const dailyEMRRule = new events.Rule(this, "DailyEMRRule", {
    //   schedule: events.Schedule.cron({
    //     minute: "0",
    //     hour: "2", // 2 AM daily
    //   }),
    // })
    //
    // // Athena Workgroup for queries
    // const athenaWorkgroup = new athena.CfnWorkGroup(this, "LogAnalysisWorkgroup", {
    //   name: "log-analysis-workgroup",
    //   description: "Workgroup for log analysis queries",
    //   workGroupConfiguration: {
    //     resultConfiguration: {
    //       outputLocation: `s3://${processedBucket.bucketName}/athena-results/`,
    //     },
    //     enforceWorkGroupConfiguration: true,
    //     publishCloudWatchMetrics: true,
    //   },
    // })
    //
    // // CloudWatch Log Group for real-time monitoring
    // const realtimeLogGroup = new logs.LogGroup(this, "RealtimeLogGroup", {
    //   logGroupName: "/aws/logs/realtime-errors",
    //   retention: logs.RetentionDays.SIX_HOURS,
    // })
  }
}
