import * as cdk from "aws-cdk-lib"
import * as s3 from "aws-cdk-lib/aws-s3"
import * as emrserverless from 'aws-cdk-lib/aws-emrserverless';
import * as iam from "aws-cdk-lib/aws-iam"
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import type { Construct } from "constructs"
import {Stack} from "aws-cdk-lib";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as path from "node:path";

interface ProcessingAndEnrichmentStackProps extends cdk.StackProps {
  rawLogsBucket: s3.Bucket;
}

export class ProcessingAndEnrichmentStack extends Stack {
  constructor(scope: Construct, id: string, props: ProcessingAndEnrichmentStackProps) {
    super(scope, id, props)

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

    const vpc = new ec2.Vpc(this, 'PublicOnlyVPC', {
      maxAzs: 2,
      subnetConfiguration: [
        {
          name: 'PublicSubnet',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
      ],
      natGateways: 0,
    });

    const sg = new ec2.SecurityGroup(this, 'EMRSecurityGroup', {
      vpc,
      description: 'Allow internet access from EMR Serverless',
      allowAllOutbound: true,
    });

    const emrApplication = new emrserverless.CfnApplication(this, "EMRApplication", {
      name: "log-template-mining",
      type: "Spark",
      releaseLabel: "emr-7.8.0",
      maximumCapacity: {
        cpu: "400 vCPU", // At 200 TBs/day it should be 2000 vCPUs
        memory: "3000 GB", // At 200 TBs/day it should be 4100 GBs
      },
      autoStartConfiguration: {
        enabled: true,
      },
      autoStopConfiguration: {
        enabled: true,
        idleTimeoutMinutes: 15,
      },
      networkConfiguration: {
        subnetIds: vpc.selectSubnets({
          subnetType: ec2.SubnetType.PUBLIC,
        }).subnetIds,
        securityGroupIds: [sg.securityGroupId]
      },
    });

    const emrCodeBucket = new s3.Bucket(this, 'EMRCodeBucket', {
      bucketName: `${this.stackName.toLowerCase()}-code-bucket`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const emrCodeDeployment = new s3deploy.BucketDeployment(this, 'DeployJobCode', {
      destinationBucket: emrCodeBucket,
      sources: [
        s3deploy.Source.asset(path.join(__dirname, '..', 'spark-jobs'))
      ],
    });

    const jobRole = new iam.Role(this, 'EmrJobRole', {
      assumedBy: new iam.ServicePrincipal('emr-serverless.amazonaws.com'),
    });
    props.rawLogsBucket.grantRead(jobRole);
    processedBucket.grantReadWrite(jobRole);
    emrCodeBucket.grantRead(jobRole);
    enrichedBucket.grantReadWrite(jobRole);

    jobRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "bedrock:InvokeModel",
        // If you plan to use streaming models:
        // "bedrock:InvokeModelWithResponseStream"
      ],
      resources: [
        "*", // or restrict to specific model ARN if you want tighter security
      ]
    }));

    // EventBridge rule to start the job daily at 02:00 UTC
    // At 200TBs/day it should run every 25 minutes
    // const emrRule = new events.Rule(this, 'DailyDrain', {
    //   schedule: events.Schedule.rate(cdk.Duration.minutes(5)), //.cron({ minute: '0', hour: '2' }),
    //   targets: [
    //     new targets.AwsApi({
    //       service: 'EMR-Serverless',
    //       action: 'startJobRun',
    //       policyStatement: new iam.PolicyStatement({
    //         actions: ['emr-serverless:StartJobRun'],
    //         resources: [`${emrApplication.attrArn}/*`],
    //       }),
    //       parameters: {
    //         applicationId: emrApplication.ref,
    //         executionRoleArn: jobRole.roleArn,
    //         jobDriver: {
    //           sparkSubmit: {
    //             entryPoint: `s3://${emrCodeBucket.bucketName}/darin3-process.py`,
    //             entryPointArguments: [
    //               `s3a://${rawLogsBucket.bucketName}/`,
    //               `s3a://${processedBucket.bucketName}/`,
    //               // '--partitions', '4000', // At 200 TBs/day it should be 8000 partitions (partitions ≈ 2 × vCPU)
    //             ],
    //             sparkSubmitParameters:
    //                 // '--conf spark.executor.memoryOverhead=1G ' +
    //                 // '--conf spark.serializer=org.apache.spark.serializer.KryoSerializer',
    //               `--conf spark.archives=s3://${emrCodeBucket.bucketName}/pyspark_venv.zip#environment ` +
    //                 '--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python ' +
    //                 '--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python ' +
    //                 '--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python'
    //           },
    //         },
    //         configurationOverrides: {
    //           monitoringConfiguration: {
    //             cloudWatchLoggingConfiguration: {
    //               enabled: true,
    //               logGroupName: emrLogGroup.logGroupName,
    //               logStreamNamePrefix: 'run',
    //             },
    //           },
    //         },
    //       },
    //     }),
    //   ],
    // });
    // emrRule.node.addDependency(emrCodeDeployment);

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
