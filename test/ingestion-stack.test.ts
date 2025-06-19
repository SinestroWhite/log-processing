import * as cdk from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { IngestionStack } from '../lib/ingestion-stack';

describe('IngestionStack', () => {
    let stack: IngestionStack;
    let template: Template;

    beforeEach(() => {
        const app = new cdk.App();
        stack = new IngestionStack(app, 'TestStack');
        template = Template.fromStack(stack);
    });

    test('creates S3 bucket with correct configuration', () => {
        template.hasResourceProperties('AWS::S3::Bucket', {
            BucketEncryption: {
                ServerSideEncryptionConfiguration: [
                    {
                        ServerSideEncryptionByDefault: {
                            SSEAlgorithm: 'AES256',
                        },
                    },
                ],
            },
            LifecycleConfiguration: {
                Rules: [
                    {
                        Id: 'raw-logs-lifecycle',
                        Status: 'Enabled',
                        ExpirationInDays: 365,
                        Transitions: [
                            {
                                StorageClass: 'STANDARD_IA',
                                TransitionInDays: 30,
                            },
                        ],
                    },
                ],
            },
        });
    });

    test('creates Kinesis Firehose with correct configuration', () => {
        template.hasResourceProperties('AWS::KinesisFirehose::DeliveryStream', {
            DeliveryStreamType: 'DirectPut',
            ExtendedS3DestinationConfiguration: {
                BucketARN: {
                    'Fn::GetAtt': [
                        Match.anyValue(),
                        'Arn',
                    ],
                },
                BufferingHints: {
                    IntervalInSeconds: 300,
                    SizeInMBs: 128,
                },
                CloudWatchLoggingOptions: {
                    Enabled: true,
                    LogGroupName: Match.anyValue(),
                    LogStreamName: 'S3Delivery',
                },
                CompressionFormat: 'GZIP',
                Prefix: '!{timestamp:yyyy/MM/dd}/',
                ErrorOutputPrefix: 'errors/!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd}/',
            },
        });
    });

    test('creates CloudWatch Log Group with correct configuration', () => {
        template.hasResourceProperties('AWS::Logs::LogGroup', {
            LogGroupName: '/aws/kinesisfirehose/log-ingestion',
            RetentionInDays: 30,
        });
    });

    test('creates IAM role with correct permissions', () => {
        // Test the IAM role basic properties
        template.hasResourceProperties('AWS::IAM::Role', {
            AssumeRolePolicyDocument: {
                Statement: [
                    {
                        Action: 'sts:AssumeRole',
                        Effect: 'Allow',
                        Principal: {
                            Service: 'firehose.amazonaws.com',
                        },
                    },
                ],
                Version: '2012-10-17',
            },
        });

        // Test that the role has the correct policy
        template.hasResourceProperties('AWS::IAM::Policy', {
            PolicyDocument: {
                Statement: Match.arrayWith([
                    {
                        Action: [
                            'logs:PutLogEvents',
                            'logs:CreateLogStream',
                        ],
                        Effect: 'Allow',
                        Resource: {
                            'Fn::GetAtt': [
                                Match.anyValue(),
                                'Arn',
                            ],
                        },
                    },
                ]),
            },
        });
    });

    test('bucket has removal policy set to destroy', () => {
        template.hasResource('AWS::S3::Bucket', {
            DeletionPolicy: 'Delete',
            UpdateReplacePolicy: 'Delete',
        });
    });

    test('log group has removal policy set to destroy', () => {
        template.hasResource('AWS::Logs::LogGroup', {
            DeletionPolicy: 'Delete',
            UpdateReplacePolicy: 'Delete',
        });
    });

    test('stack has the correct number of resources', () => {
        // Count the main resources we expect
        template.resourceCountIs('AWS::S3::Bucket', 1);
        template.resourceCountIs('AWS::KinesisFirehose::DeliveryStream', 1);
        template.resourceCountIs('AWS::Logs::LogGroup', 1);
        template.resourceCountIs('AWS::IAM::Role', 2);
    });
});