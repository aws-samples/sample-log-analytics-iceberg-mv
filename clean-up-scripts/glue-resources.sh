# Delete the materialized view
aws glue delete-table \
  --database-name <stream_analytics-replace-with-original> \
  --name <app_logs_mv-replace-with-original>

# Delete the Iceberg table
aws glue delete-table \
  --database-name <stream_analytics-replace-with-original> \
  --name <application_logs--replace-with-original>

# Delete the database
aws glue delete-database \
  --name <stream_analytics-replace-with-original>

# Delete the Glue job
aws glue delete-job \
  --job-name <stream_analytics-iceberg-glue-replace-with-original>

