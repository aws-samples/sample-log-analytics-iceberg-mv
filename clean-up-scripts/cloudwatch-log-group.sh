aws logs delete-log-group \
  --log-group-name /aws/kinesisfirehose/<iceberg-logs-stream-replace-with-original>

aws logs delete-log-group \
  --log-group-name /aws/lambda/<CloudWatchLogsToIceberg-replace-with-original>

