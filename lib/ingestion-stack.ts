import * as cdk from "aws-cdk-lib"
import * as s3 from "aws-cdk-lib/aws-s3"
import * as kinesisFirehose from "aws-cdk-lib/aws-kinesisfirehose"
import * as iam from "aws-cdk-lib/aws-iam"
import * as logs from "aws-cdk-lib/aws-logs"
import type { Construct } from "constructs"
import {Stack} from "aws-cdk-lib";

export class IngestionStack extends Stack {
  public readonly rawLogsBucket: s3.Bucket;
  public readonly logFirehose: kinesisFirehose.CfnDeliveryStream;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props)

    // S3 Buckets for different stages
    this.rawLogsBucket = new s3.Bucket(this, "RawLogsBucket", {
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

    // Kinesis Data Firehose for log ingestion
    const firehoseRole = new iam.Role(this, "FirehoseRole", {
      assumedBy: new iam.ServicePrincipal("firehose.amazonaws.com"),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess")],
    })

    const firehoseLogGroup = new logs.LogGroup(this, 'FirehoseLogGroup', {
      logGroupName: '/aws/kinesisfirehose/log-ingestion',
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    firehoseRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'logs:PutLogEvents',
        'logs:CreateLogStream',
      ],
      resources: [firehoseLogGroup.logGroupArn],
    }));

    this.logFirehose = new kinesisFirehose.CfnDeliveryStream(this, "LogFirehose", {
      deliveryStreamName: `${this.stackName.toLowerCase()}-log-ingestion`,
      deliveryStreamType: "DirectPut",
      extendedS3DestinationConfiguration: {
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: firehoseLogGroup.logGroupName,
          logStreamName: 'S3Delivery',
        },
        bucketArn: this.rawLogsBucket.bucketArn,
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
  }
}
