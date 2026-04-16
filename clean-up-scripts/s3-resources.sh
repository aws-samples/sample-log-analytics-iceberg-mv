# Empty the data bucket
aws s3 rm <s3://my-company-iceberg-data-bucket-replace-with-original> --recursive

# Empty the errors bucket
aws s3 rm <s3://my-company-iceberg-errors-bucket-replace-with-original> --recursive

# Delete the buckets
aws s3 rb <s3://my-company-iceberg-data-bucket-replace-with-original>
aws s3 rb <s3://my-company-iceberg-errors-bucket-replace-with-original>

