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
MVVIEW    = f"{stream_table_name}_mv"
WAREHOUSE = f"s3://{iceberg_data_bucket}/{warehouse_db_name}.db"
print(f"=== MV Refresh Job — DATABASE: {DATABASE}, MV: {MVVIEW}, WAREHOUSE: {WAREHOUSE} ===")


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
    # ── Step 1: Refresh materialized view ────────────────────────────────
    run_step("Step 1: FULL refresh MV", lambda:
        spark.sql(f"REFRESH MATERIALIZED VIEW {CATALOG}.{DATABASE}.{MVVIEW} FULL"))
    time.sleep(20)

    # ── Step 2: Verify refreshed MV contents ─────────────────────────────
    run_step("Step 2: Verify refreshed MV", lambda:
        spark.sql(f"SELECT * FROM {CATALOG}.{DATABASE}.{MVVIEW} ORDER BY customer_name").show())

    print("\n=== MV REFRESH JOB COMPLETED SUCCESSFULLY ===")

except Exception as e:
    print(f"=== MV REFRESH JOB FAILED: {e} ===")
    traceback.print_exc()
    raise
finally:
    job.commit()
