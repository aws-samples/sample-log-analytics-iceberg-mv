# Sample Log Analytics with Apache Iceberg Materialized Views

An automated deployment of a real-time data pipeline that streams CloudWatch Logs into Apache Iceberg tables with materialized views, using AWS Glue, Amazon Data Firehose, and AWS Lambda. The entire pipeline deploys with a single CloudFormation stack — no manual script uploads or multi-step orchestration needed.

## Architecture

```
Application → CloudWatch Logs → Lambda → Firehose → Apache Iceberg (S3 + Glue Catalog) → Athena
                                   ↕                        ↑
                              SQS (DLQ)              Glue MV Refresh
                                                     (every 30 min)
```

**Components:**
- **AWS Glue Job (MV Builder)** — Creates the Iceberg database, base table with sample data, and a materialized view for pre-aggregated analytics
- **AWS Glue Job (MV Refresh)** — Refreshes the materialized view on a 30-minute schedule via a Glue trigger
- **AWS Lambda** — Processes CloudWatch Logs events, extracts structured fields, and forwards to Firehose
- **Amazon Data Firehose** — Buffers and delivers records to the Iceberg table with Iceberg destination
- **CloudWatch Subscription Filter** — Connects a log group to the Lambda function
- **Iceberg Materialized View** — Pre-computes aggregations (order count and total amount per customer) for fast query performance
- **Dead Letter Queue (SQS)** — Captures failed Lambda invocations for automatic retry, with a permanent failure queue after 3 attempts
- **Custom Resource (Artifact Uploader)** — Automatically uploads Glue scripts and Lambda zip to S3 during stack deployment

## Project Structure

```
├── cloudformation/
│   ├── iceberg-pipeline.yaml               # Consolidated single-stack template (recommended)
│   ├── iceberg-pipeline-glue.yaml          # Glue stack (with Lake Formation) - legacy
│   ├── iceberg-pipeline-glue-no-lakeformation.yaml  # Glue stack (no Lake Formation) - legacy
│   └── iceberg-pipeline-firehose.yaml      # Firehose + Lambda stack - legacy
├── scripts/
│   ├── deploy.sh                           # Legacy upload script (no longer needed)
│   ├── sample-glue-job-iceberg-materializedview-builder.py  # Glue ETL script
│   ├── glue-job-mv-refresh.py             # Glue MV refresh script
│   ├── lambda_function.py                  # Lambda function code
│   └── send_test_logs.py                   # Test script to send sample logs
├── tests/                                  # pytest test suite
└── requirements.txt                        # Python dependencies
```

## Prerequisites

- An [AWS account](https://aws.amazon.com/) with an IAM user or role that has permissions to:
  - **CloudFormation** — create, update, and delete stacks
  - **IAM** — create roles and policies (`iam:CreateRole`, `iam:PutRolePolicy`, `iam:AttachRolePolicy`, `iam:PassRole`)
  - **S3** — create buckets and upload objects
  - **AWS Glue** — create and run jobs, manage databases and tables
  - **Amazon Data Firehose** — create and manage delivery streams
  - **AWS Lambda** — create and manage functions
  - **CloudWatch Logs** — create log groups, subscription filters
  - **Amazon SQS** — create queues
  - **Amazon Athena** — run queries to verify Iceberg table data

> **Tip:** For a quick start, use an IAM principal with `AdministratorAccess`. For production, scope permissions down to the specific resources created by the stack.

## Deployment

### Step 1: Deploy the pipeline stack

Deploy the entire solution with a single CloudFormation stack. The template automatically creates S3 buckets, uploads all scripts, provisions IAM roles, configures Firehose, and runs the Glue job to create the Iceberg table and materialized view.

**Via CLI:**

```bash
aws cloudformation deploy \
  --template-file cloudformation/iceberg-pipeline.yaml \
  --stack-name iceberg-pipeline \
  --parameter-overrides \
    IcebergDataBucketName="your-company-iceberg-data-ACCOUNT_ID-REGION" \
    IcebergErrorsBucketName="your-company-iceberg-errors-ACCOUNT_ID-REGION" \
    GlueScriptBucketName="your-company-scripts-ACCOUNT_ID-REGION" \
  --capabilities CAPABILITY_NAMED_IAM
```

**Via Console:**

1. Go to **CloudFormation console** → **Create stack** → **Upload a template file**
2. Upload `cloudformation/iceberg-pipeline.yaml`
3. Review parameters — they are grouped into **[REQUIRED]** and **Safe defaults**:
   - Set `CreateScriptBucket` to `false` if reusing an existing S3 bucket
   - Set `EnableLakeFormation` to `true` if your account uses Lake Formation
   - Provide globally unique S3 bucket names
4. Check IAM capabilities → **Submit**

The stack takes approximately 10–15 minutes to complete.

### Step 2: Test the end-to-end pipeline

Send sample log events matching the Iceberg table schema to the CloudWatch Log Group:

```bash
python3 scripts/send_test_logs.py
```

The subscription filter triggers the Lambda, which forwards records to Firehose for delivery into the Iceberg table.

### Step 3: Verify data delivery

Allow approximately 30 seconds for the Firehose buffer to flush, then query in Amazon Athena:

```sql
-- Verify base table
SELECT * FROM stream_analytics.application_logs ORDER BY order_date DESC LIMIT 10;

-- Verify materialized view
SELECT * FROM stream_analytics.application_logs_mv ORDER BY customer_name;
```

### Automated materialized view refresh

The stack provisions a scheduled Glue trigger that automatically runs the MV refresh job every 30 minutes. As new data streams in through Firehose, the trigger keeps the materialized view current without manual intervention.

## Key Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `IcebergDataBucketName` | Yes | S3 bucket for Iceberg table data (must be globally unique) |
| `IcebergErrorsBucketName` | Yes | S3 bucket for failed records (must be globally unique) |
| `GlueScriptBucketName` | Yes | S3 bucket for scripts (must be globally unique) |
| `CreateScriptBucket` | Yes | Set to `false` if the script bucket already exists |
| `EnableLakeFormation` | Yes | Set to `true` if using Lake Formation |
| `CreateSubscriptionLogGroup` | Yes | Set to `false` if the log group already exists |

> **Naming pattern:** `{company}-{project}-{purpose}-{account-id}-{region}`

## Legacy Multi-Stack Deployment

The `cloudformation/` directory also contains the original split templates for reference:

| Template | Description |
|----------|-------------|
| `iceberg-pipeline-glue.yaml` | Glue stack with Lake Formation |
| `iceberg-pipeline-glue-no-lakeformation.yaml` | Glue stack without Lake Formation |
| `iceberg-pipeline-firehose.yaml` | Firehose + Lambda stack |

These require the manual `scripts/deploy.sh` step and multi-stack deployment. The consolidated `iceberg-pipeline.yaml` is recommended.

## Cleanup

```bash
# Delete the stack
aws cloudformation delete-stack --stack-name iceberg-pipeline

# Empty and delete S3 buckets (replace with your bucket names)
aws s3 rm s3://your-company-scripts-ACCOUNT_ID-REGION --recursive
aws s3 rb s3://your-company-scripts-ACCOUNT_ID-REGION
aws s3 rm s3://your-company-iceberg-data-ACCOUNT_ID-REGION --recursive
aws s3 rb s3://your-company-iceberg-data-ACCOUNT_ID-REGION
aws s3 rm s3://your-company-iceberg-errors-ACCOUNT_ID-REGION --recursive
aws s3 rb s3://your-company-iceberg-errors-ACCOUNT_ID-REGION
```

## Running Tests

```bash
pip install -r requirements.txt
python -m pytest tests/ -v
```

## License

MIT

## Disclaimer

This is sample code not intended for production use without additional security testing.
