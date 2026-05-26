import sys
import time
import traceback
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "iceberg-data-bucket", "warehouse-database-name", "stream-table-name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CATALOG   = "glue_catalog"
iceberg_data_bucket = args.get("iceberg_data_bucket") or args.get("iceberg-data-bucket") or args["iceberg_data_bucket"]
warehouse_db_name = args.get("warehouse_database_name") or args.get("warehouse-database-name") or args["warehouse_database_name"]
stream_table_name = args.get("stream_table_name") or args.get("stream-table-name") or args["stream_table_name"]
DATABASE  = warehouse_db_name
BASETBL   = stream_table_name
MVVIEW    = f"{stream_table_name}_mv"
WAREHOUSE = f"s3://{iceberg_data_bucket}/{warehouse_db_name}.db"
print(f"=== S3 data bucket: {iceberg_data_bucket}, DATABASE: {warehouse_db_name}, WAREHOUSE: {WAREHOUSE} ===")

def run_step(step_name, fn):
    """Execute a step. Raises on failure to stop the job immediately."""
    print(f"=== {step_name}: STARTED ===")
    start = time.time()
    try:
        fn()
        elapsed = round(time.time() - start, 2)
        print(f"=== {step_name}: COMPLETED ({elapsed}s) ===")
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        print(f"=== {step_name}: FAILED ({elapsed}s) — {e} ===")
        traceback.print_exc()
        raise


try:
    # ── Step 1: Create database ──────────────────────────────────────────
    run_step("Step 1: Create database", lambda:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}"))

    # Wait for S3 bucket DNS propagation (newly created buckets may need time)
    print("=== Waiting 30s for S3 bucket DNS propagation ===")
    time.sleep(30)

    # ── Step 2: Create base Iceberg table ────────────────────────────────
    run_step("Step 2: Create base Iceberg table", lambda:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{DATABASE}.{BASETBL} (
                id            INT,
                customer_name STRING,
                amount        INT,
                order_date    DATE
            )
            USING iceberg
            LOCATION '{WAREHOUSE}/{BASETBL}'
        """))
    time.sleep(5)

    # ── Step 3: Insert sample data ───────────────────────────────────────
    run_step("Step 3: Insert sample data", lambda:
        spark.sql(f"""
            INSERT INTO {CATALOG}.{DATABASE}.{BASETBL} VALUES
            (1, 'Alice',   150, DATE '2026-01-01'),
            (2, 'Bob',     200, DATE '2026-01-02'),
            (3, 'Alice',   300, DATE '2026-01-03'),
            (4, 'Charlie', 100, DATE '2026-01-04'),
            (5, 'Bob',     250, DATE '2026-01-05')
        """))

    # ── Step 4: Verify base table ────────────────────────────────────────
    run_step("Step 4: Verify base table", lambda:
        spark.sql(f"SELECT * FROM {CATALOG}.{DATABASE}.{BASETBL}").show())

    # ── Step 5: Drop MV if exists (clean rerun) ─────────────────────────
    run_step("Step 5: Drop MV if exists", lambda:
        spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {CATALOG}.{DATABASE}.{MVVIEW}"))

    # ── Step 6: Create materialized view ─────────────────────────────────
    time.sleep(5)
    run_step("Step 6: Create Materialized View", lambda:
        spark.sql(f"""
            CREATE MATERIALIZED VIEW {CATALOG}.{DATABASE}.{MVVIEW}
            AS SELECT
                customer_name,
                COUNT(*)     AS mv_order_count,
                SUM(amount)  AS mv_total_amount
            FROM {CATALOG}.{DATABASE}.{BASETBL}
            GROUP BY customer_name
        """))
    time.sleep(5)

    # ── Step 7: Verify MV contents ───────────────────────────────────────
    run_step("Step 7: Verify MV contents", lambda:
        spark.sql(f"SELECT * FROM {CATALOG}.{DATABASE}.{MVVIEW} ORDER BY customer_name").show())
    time.sleep(5)

    # ── Step 8: Test FULL refresh ────────────────────────────────────────
    run_step("Step 8: FULL refresh MV", lambda:
        spark.sql(f"REFRESH MATERIALIZED VIEW {CATALOG}.{DATABASE}.{MVVIEW} FULL"))
    time.sleep(5)

    run_step("Step 9: Verify post-refresh MV", lambda:
        spark.sql(f"SELECT * FROM {CATALOG}.{DATABASE}.{MVVIEW} ORDER BY customer_name").show())

    print("\n=== JOB COMPLETED SUCCESSFULLY ===")

except Exception as e:
    print(f"=== JOB FAILED: {e} ===")
    traceback.print_exc()
    raise
finally:
    job.commit()
