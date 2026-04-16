# Detach policies from Lambda role
aws iam detach-role-policy \
  --role-name <LambdaFirehoseProcessorRole-replace-with-original> \
  --policy-arn <arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole-replace-with-original>

# Delete inline policies
aws iam delete-role-policy \
  --role-name <LambdaFirehoseProcessorRole-replace-with-original> \
  --policy-name <FirehoseWriteAccess-replace-with-original>

aws iam delete-role-policy \
  --role-name <FirehoseIcebergDeliveryRole-replace-with-original> \
  --policy-name <S3AndGlueAccess-replace-with-original>

# Delete the roles
aws iam delete-role --role-name <LambdaFirehoseProcessorRole-replace-with-original>
aws iam delete-role --role-name <FirehoseIcebergDeliveryRole-replace-with-original>

