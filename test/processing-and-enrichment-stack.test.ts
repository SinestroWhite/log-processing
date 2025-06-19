import * as cdk from "aws-cdk-lib";
import { Match, Template } from "aws-cdk-lib/assertions";
import * as s3 from "aws-cdk-lib/aws-s3";
import { ProcessingAndEnrichmentStack } from "../lib/processing-and-enrichment-stack";

describe("ProcessingAndEnrichmentStack", () => {
    let app: cdk.App;
    let stack: ProcessingAndEnrichmentStack;
    let template: Template;

    beforeEach(() => {
        app = new cdk.App();
        // Create a mock raw logs bucket to pass as a prop
        const mockStack = new cdk.Stack(app, "MockStack");
        const rawLogsBucket = new s3.Bucket(mockStack, "MockRawLogsBucket");

        stack = new ProcessingAndEnrichmentStack(app, "TestStack", {
            rawLogsBucket: rawLogsBucket,
        });
        template = Template.fromStack(stack);
    });

    test("creates processed logs bucket with correct configuration", () => {
        template.hasResourceProperties("AWS::S3::Bucket", {
            BucketName: Match.stringLikeRegexp("teststack-processed-logs"),
            LifecycleConfiguration: {
                Rules: [
                    {
                        Id: "processed-logs-lifecycle",
                        Status: "Enabled",
                        ExpirationInDays: 365,
                        Transitions: [
                            {
                                StorageClass: "STANDARD_IA",
                                TransitionInDays: 30,
                            },
                        ],
                    },
                ],
            },
        });
    });

    test("creates enriched logs bucket with correct configuration", () => {
        template.hasResourceProperties("AWS::S3::Bucket", {
            BucketName: Match.stringLikeRegexp("teststack-enriched-logs"),
            LifecycleConfiguration: {
                Rules: [
                    {
                        Id: "enriched-logs-lifecycle",
                        Status: "Enabled",
                        ExpirationInDays: 365,
                        Transitions: [
                            {
                                StorageClass: "STANDARD_IA",
                                TransitionInDays: 90,
                            },
                        ],
                    },
                ],
            },
        });
    });

    test("creates VPC with correct configuration", () => {
        template.hasResourceProperties("AWS::EC2::VPC", {
            CidrBlock: Match.anyValue(),
            EnableDnsHostnames: true,
            EnableDnsSupport: true,
            Tags: Match.arrayWith([
                {
                    Key: "Name",
                    Value: Match.stringLikeRegexp("TestStack/PublicOnlyVPC"),
                },
            ]),
        });
    });

    test("creates security group with correct configuration", () => {
        template.hasResourceProperties("AWS::EC2::SecurityGroup", {
            GroupDescription: "Allow internet access from EMR Serverless",
            SecurityGroupEgress: [
                {
                    CidrIp: "0.0.0.0/0",
                    Description: "Allow all outbound traffic by default",
                    IpProtocol: "-1",
                },
            ],
        });
    });

    test("creates EMR Serverless application with correct configuration", () => {
        template.hasResourceProperties("AWS::EMRServerless::Application", {
            Name: "log-template-mining",
            Type: "Spark",
            ReleaseLabel: "emr-7.8.0",
            MaximumCapacity: {
                Cpu: "400 vCPU",
                Memory: "3000 GB",
            },
            AutoStartConfiguration: {
                Enabled: true,
            },
            AutoStopConfiguration: {
                Enabled: true,
                IdleTimeoutMinutes: 15,
            },
        });
    });

    test("creates EMR code bucket with correct configuration", () => {
        template.hasResourceProperties("AWS::S3::Bucket", {
            BucketName: Match.stringLikeRegexp("teststack-code-bucket"),
            BucketEncryption: {
                ServerSideEncryptionConfiguration: [
                    {
                        ServerSideEncryptionByDefault: {
                            SSEAlgorithm: "AES256",
                        },
                    },
                ],
            },
        });
    });

    test("creates EMR job role with correct permissions", () => {
        // Test the IAM role
        template.hasResourceProperties("AWS::IAM::Role", {
            AssumeRolePolicyDocument: {
                Statement: [
                    {
                        Action: "sts:AssumeRole",
                        Effect: "Allow",
                        Principal: {
                            Service: "emr-serverless.amazonaws.com",
                        },
                    },
                ],
            },
        });

        // Test the role's policies
        template.hasResourceProperties("AWS::IAM::Policy", {
            PolicyDocument: {
                Statement: Match.arrayWith([
                    {
                        Action: "bedrock:InvokeModel",
                        Effect: "Allow",
                        Resource: "*",
                    },
                ]),
            },
        });
    });

    test("creates bucket deployment for EMR code", () => {
        template.hasResourceProperties("Custom::CDKBucketDeployment", {
            DestinationBucketName: {
                Ref: Match.anyValue(),
            },
            SourceBucketNames: Match.anyValue(),
            SourceObjectKeys: Match.anyValue(),
        });
    });

    test("stack has required number of resources", () => {
        // Count the main resources we expect to find
        const resources = template.toJSON().Resources;
        expect(Object.keys(resources).length).toBeGreaterThanOrEqual(8); // Adjust this number based on your resources
    });
});