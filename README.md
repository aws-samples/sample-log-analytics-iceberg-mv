# Apache Iceberg Streaming Pipeline with Materialized Views on AWS

An automated deployment of a real-time data pipeline that streams CloudWatch Logs into Apache Iceberg tables with materialized views, using AWS Glue, Amazon Data Firehose, and AWS Lambda. The pipeline ingests streaming data into Iceberg, then leverages Iceberg materialized views to pre-compute aggregations for fast analytical queries.

## Architecture

```
CloudWatch Logs → Lambda → Firehose → Apache Iceberg (S3 + Glue Catalog)
```

**Components:**
- **AWS Glue Job** — Creates the Iceberg database, base table, and materialized view for pre-aggregated analytics
- **AWS Lambda** — Processes CloudWatch Logs events, extracts structured fields, and forwards to Firehose
- **Amazon Data Firehose** — Buffers and delivers records to the Iceberg table
- **CloudWatch Subscription Filter** — Connects a log group to the Lambda function
- **Iceberg Materialized View** — Pre-computes aggregations (order count and total amount per customer) for fast query performance

## Project Structure

```
├── cloudformation/
│   ├── iceberg-pipeline-glue.yaml          # Glue stack (with Lake Formation)
│   ├── iceberg-pipeline-glue-no-lakeformation.yaml  # Glue stack (no Lake Formation)
│   └── iceberg-pipeline-firehose.yaml      # Firehose + Lambda stack
├── scripts/
│   ├── deploy.sh                           # Uploads scripts to S3
│   ├── sample-glue-job-iceberg-materializedview-builder.py  # Glue ETL script
│   ├── lambda_function.py                  # Lambda function code
│   └── send_test_logs.py                   # Test script to send sample logs
├── tests/                                  # pytest test suite
└── requirements.txt                        # Python dependencies
```

## Prerequisites

- AWS CLI v2 configured with credentials
- Python 3.x installed
- Sufficient IAM permissions (S3, Glue, Lambda, Firehose, IAM, CloudWatch Logs)

## Deployment

### Step 1: Upload scripts to S3

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh --s3-bucket your-unique-bucket-name --region us-east-1
```

This creates the S3 bucket and uploads the Glue script and Lambda package.

### Step 2: Deploy the Glue stack

1. Go to **CloudFormation console** → **Create stack** → **Upload a template file**
2. Upload `cloudformation/iceberg-pipeline-glue-no-lakeformation.yaml` (use `iceberg-pipeline-glue.yaml` if Lake Formation is enabled)
3. Set parameters:
   - **AccountId**: your AWS account ID
   - **GlueScriptBucketName**: same bucket name from Step 1
4. Leave the **IAM role** field empty → check IAM capabilities → **Submit**

### Step 3: Run the Glue job

1. Go to **AWS Glue console** → **Jobs**
2. Select the job and click **Run**
3. Wait for completion — this creates the Iceberg database and table

> **Why this step?** The Firehose delivery stream validates that the destination Iceberg table exists at creation time. The Glue job creates the table, so it must run before the Firehose stack is deployed.

### Step 4: Deploy the Firehose stack

1. Go to **CloudFormation console** → **Create stack** → **Upload a template file**
2. Upload `cloudformation/iceberg-pipeline-firehose.yaml`
3. Set parameters:
   - **AccountId**: your AWS account ID
   - **LambdaScriptBucketName**: same bucket name from Step 1
   - **StreamDatabaseName**: same as `WarehouseDatabaseName` from Step 2 (default: `stream_analytics`)
   - **CreateSubscriptionLogGroup**: `true` if `/aws/application/logs` doesn't exist, `false` if it does
4. Leave the **IAM role** field empty → check IAM capabilities → **Submit**

### Step 5: Test the pipeline

```bash
python3 scripts/send_test_logs.py
```

This sends sample records (`id`, `customer_name`, `amount`, `order_date`) to CloudWatch Logs. The pipeline processes them through Lambda → Firehose → Iceberg.

### Verify delivery

Check Firehose metrics:

```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Firehose \
  --metric-name DeliveryToIcebergTables.Success \
  --dimensions Name=DeliveryStreamName,Value=iceberg-logs-stream \
  --start-time $(date -u -v-1H +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 60 \
  --statistics Sum \
  --region us-east-1
```

Query the Iceberg table in Athena:

```sql
SELECT * FROM stream_analytics.application_logs ORDER BY order_date DESC LIMIT 10;
```

Query the materialized view for pre-aggregated results:

```sql
SELECT * FROM stream_analytics.application_logs_mv ORDER BY customer_name;
```

## Template Variants

| Template | Lake Formation | Use when |
|----------|---------------|----------|
| `iceberg-pipeline-glue.yaml` | Enabled | Account has Lake Formation configured |
| `iceberg-pipeline-glue-no-lakeformation.yaml` | Disabled | Standard IAM-only access control |

## Cleanup

Delete stacks in reverse order:

```bash
aws cloudformation delete-stack --stack-name iceberg-pipeline-firehose --region us-east-1
aws cloudformation delete-stack --stack-name iceberg-pipeline-glue --region us-east-1
```

Empty and delete the S3 buckets:

```bash
aws s3 rm s3://your-unique-bucket-name --recursive
aws s3 rb s3://your-unique-bucket-name
```

## Running Tests

```bash
pip install -r requirements.txt
python -m pytest tests/ -v
```

## License

MIT
