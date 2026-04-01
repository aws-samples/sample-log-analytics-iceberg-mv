# AWS Glue Iceberg Materialized View Builder

This project demonstrates an end-to-end pipeline for ingesting CloudWatch Logs into Apache Iceberg tables and building materialized views using AWS Glue.

## Components

### 1. Lambda Function (`lambda_function.py`)

A CloudWatch Logs subscription handler that transforms and forwards log events to Kinesis Data Firehose for Iceberg ingestion.

**What it does:**
- Receives CloudWatch Logs subscription events (base64-encoded, gzip-compressed)
- Parses log messages (supports both JSON and plain text formats)
- Transforms events into a structured schema: `event_timestamp`, `log_level`, `message`, `application_name`, `user_id`, `session_id`, `processed_at`
- Sends transformed records to Kinesis Data Firehose in batches of 500

**Environment Variables:**
| Variable | Description | Default |
|---|---|---|
| `FIREHOSE_STREAM_NAME` | Kinesis Data Firehose delivery stream name | `iceberg-logs-stream` |

### 2. Glue Job (`sample-glue-job-iceberg-materializedview-builder.py`)

An AWS Glue ETL job that creates an Iceberg base table, inserts sample data, and builds a materialized view with aggregations.

**What it does:**
1. Creates the `stream_analytics` database in the Glue Catalog
2. Creates an Iceberg base table (`application_logs`) with columns: `id`, `customer_name`, `amount`, `order_date`
3. Inserts sample order data
4. Creates a materialized view (`application_logs_mv`) that aggregates order count and total amount per customer
5. Performs a `FULL` refresh of the materialized view
6. Verifies results at each step with built-in error tracking

**Configuration:**
| Setting | Value |
|---|---|
| Catalog | `glue_catalog` |
| Database | `stream_analytics` |
| Base Table | `application_logs` |
| Materialized View | `application_logs_mv` |
| Warehouse Location | `s3://iceberg-mv-stream-analytics/stream-analytics.db` |

## Deployment

1.	Navigate to AWS Glue Console in us-east-1 and select "ETL jobs"
2.	Click "Visual ETL" or "Script editor" and choose "Spark script editor" with Python
3.	Name it "stream_analytics-iceberg-glue" and assign an IAM role with S3/Glue permissions.
4.	Set Glue version 5.1 with Iceberg support
5.	Navigate to Job details → Advanced properties → Add Job parameters:
    Key	Value

--conf	spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions 
    --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog 
    --conf spark.sql.catalog.glue_catalog.type=glue 
    --conf spark.sql.catalog.glue_catalog.warehouse=s3://my-company-iceberg-data-bucket/stream-analytics.db 
    --conf spark.sql.catalog.glue_catalog.glue.region=us-east-1 
    --conf spark.sql.catalog.glue_catalog.glue.id=ACCOUNT_ID 
    --conf spark.sql.catalog.glue_catalog.glue.account-id=ACCOUNT_ID 
    --conf spark.sql.catalog.glue_catalog.client.region=us-east-1 
    --conf spark.sql.catalog.glue_catalog.glue.lakeformation-enabled=true 
    --conf spark.sql.defaultCatalog=glue_catalog --conf spark.sql.optimizer.answerQueriesWithMVs.enabled=true 
    --conf spark.sql.materializedViews.metadataCache.enabled=true

--datalake-formats	iceberg

6.	Replace the job with the sample-glue-job-iceberg-materializedview-builder.py script, and update the required values
7.	Save and run the job, then verify the database, table and materialized view were created in the Glue Data Catalog.
8.	Verify that the table and Iceberg materialized view were created in Athena


## Prerequisites

- An S3 bucket for the Iceberg warehouse (`iceberg-mv-stream-analytics`)
- A Kinesis Data Firehose delivery stream configured to write to the Iceberg table (for the Lambda path)
- AWS Glue 5.1
