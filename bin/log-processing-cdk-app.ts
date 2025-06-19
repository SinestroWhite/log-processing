#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import {IngestionStack} from "../lib/ingestion-stack";
import {ProcessingAndEnrichmentStack} from "../lib/processing-and-enrichment-stack";

const app = new cdk.App();

const ingestionStack = new IngestionStack(app, 'IngestionStack', {
  env: {
    account: '278838288143',
    region: 'eu-central-1'
  },
});

new ProcessingAndEnrichmentStack(app, 'ProcessingAndEnrichmentStack', {
  rawLogsBucket: ingestionStack.rawLogsBucket,
  env: {
    account: '278838288143',
    region: 'eu-central-1'
  },
});