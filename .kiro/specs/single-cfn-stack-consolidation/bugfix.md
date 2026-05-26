# Bugfix Requirements Document

## Introduction

The Iceberg data pipeline deployment currently requires 4 manual, sequentially-ordered steps: uploading artifacts via `deploy.sh`, deploying the Glue infrastructure stack (`iceberg-pipeline-glue.yaml`), manually triggering the Glue job from the AWS console to create the Iceberg table and materialized view in the Glue Catalog, and finally deploying the Firehose streaming pipeline stack (`iceberg-pipeline-firehose.yaml`). The critical hard dependency is Step 3: Amazon Data Firehose's `IcebergDestinationConfiguration` requires the target Glue Catalog table (`stream_analytics.application_logs`) to already exist at the time the Firehose delivery stream resource is created. This forces a manual console intervention between two CloudFormation deployments, making the pipeline impossible to deploy in a single automated operation and error-prone when the Glue job is skipped or fails silently before the Firehose stack is deployed.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a user attempts to deploy the full Iceberg pipeline from scratch THEN the system requires 4 separate manual steps with no single-command deployment path

1.2 WHEN the Glue infrastructure stack (`iceberg-pipeline-glue.yaml`) has been deployed THEN the system requires the user to manually navigate to the AWS Glue console and trigger the job run before proceeding

1.3 WHEN the Firehose stack (`iceberg-pipeline-firehose.yaml`) is deployed before the Glue job has completed successfully THEN the system fails with a resource creation error because the Glue Catalog table `stream_analytics.application_logs` does not yet exist

1.4 WHEN the Glue job run fails silently or is skipped THEN the system provides no automated feedback during deployment, leaving the user to discover the missing table only when the Firehose stack deployment fails

1.5 WHEN a CI/CD pipeline or automated deployment process attempts to deploy the full stack THEN the system cannot complete unattended because Step 3 requires a human to manually trigger the Glue job from the console

### Expected Behavior (Correct)

2.1 WHEN a user deploys the consolidated CloudFormation template THEN the system SHALL provision all pipeline resources (S3 buckets, IAM roles, Glue job, Firehose delivery stream, Lambda function, SQS queues, subscription filter) in a single stack deployment operation

2.2 WHEN the Glue job resource has been created by CloudFormation THEN the system SHALL automatically trigger the Glue job run via a CloudFormation Custom Resource backed by a Lambda function, without requiring manual console interaction

2.3 WHEN the Custom Resource Lambda triggers the Glue job THEN the system SHALL poll for job completion and signal CloudFormation success only after the Glue job run reaches `SUCCEEDED` state, ensuring the Glue Catalog table exists before Firehose resource creation begins

2.4 WHEN the Glue job run fails or times out during stack deployment THEN the system SHALL signal a CloudFormation failure, causing the stack to roll back and surfacing the error to the operator

2.5 WHEN the Firehose delivery stream resource is created THEN the system SHALL have a `DependsOn` relationship (or equivalent CloudFormation dependency) on the Custom Resource, guaranteeing the Glue Catalog table `stream_analytics.application_logs` exists at Firehose creation time

2.6 WHEN a CI/CD pipeline or automated deployment process invokes the consolidated template THEN the system SHALL complete the full deployment unattended without requiring human intervention between steps

### Unchanged Behavior (Regression Prevention)

3.1 WHEN the Glue job runs as part of the Custom Resource trigger THEN the system SHALL CONTINUE TO create the `stream_analytics` database, the `application_logs` Iceberg table, and the `application_logs_mv` materialized view with the same schema and S3 warehouse location as the existing standalone Glue job execution

3.2 WHEN the Firehose delivery stream is active THEN the system SHALL CONTINUE TO deliver records to the `stream_analytics.application_logs` Iceberg table using the same `IcebergDestinationConfiguration` (catalog ARN, database name, table name, S3 bucket, buffering hints) as the existing `iceberg-pipeline-firehose.yaml` stack

3.3 WHEN the Lambda function receives a CloudWatch Logs subscription filter event THEN the system SHALL CONTINUE TO parse, transform, and forward log records to the Firehose delivery stream with the same logic as the existing `lambda_function.py`

3.4 WHEN the Lambda function receives an SQS event from the dead-letter queue THEN the system SHALL CONTINUE TO reprocess failed events using the same DLQ retry logic as the existing stack

3.5 WHEN the consolidated stack is deployed with Lake Formation enabled THEN the system SHALL CONTINUE TO apply the same Lake Formation IAM role permissions and bucket policy as the existing `iceberg-pipeline-glue.yaml` stack

3.6 WHEN the consolidated stack is deployed with Lake Formation disabled THEN the system SHALL CONTINUE TO apply the same no-Lake-Formation IAM permissions and bucket policy as the existing `iceberg-pipeline-glue-no-lakeformation.yaml` stack

3.7 WHEN the S3 script bucket already contains the Glue ETL script and Lambda zip at the expected keys THEN the system SHALL CONTINUE TO reference those artifacts at `scripts/sample-glue-job-iceberg-materializedview-builder.py` and `lambda/lambda_function.zip` without requiring changes to the `deploy.sh` upload paths
