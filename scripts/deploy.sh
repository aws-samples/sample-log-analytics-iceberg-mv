#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GLUE_SCRIPT="${SCRIPT_DIR}/sample-glue-job-iceberg-materializedview-builder.py"
LAMBDA_SCRIPT="${SCRIPT_DIR}/lambda_function.py"

usage() {
  cat <<EOF
Usage: $(basename "$0") --s3-bucket <bucket> [--region <region>]

Required parameters:
  --s3-bucket    S3 bucket name (created by the prereqs stack)

Optional parameters:
  --region       AWS region (default: us-east-1)

This script uploads the Glue ETL script and Lambda function to S3.
Run this AFTER deploying iceberg-pipeline-prereqs.yaml.

Example:
  $(basename "$0") --s3-bucket my-scripts-bucket --region us-east-1
EOF
  exit 1
}

S3_BUCKET=""
REGION="us-east-1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --s3-bucket)
      S3_BUCKET="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    *)
      echo "Error: Unknown parameter '$1'" >&2
      usage
      ;;
  esac
done

if [[ -z "$S3_BUCKET" ]]; then
  echo "Error: --s3-bucket is required" >&2
  usage
fi

if [[ ! -f "$GLUE_SCRIPT" ]]; then
  echo "Error: Glue script not found at ${GLUE_SCRIPT}" >&2
  exit 1
fi

if [[ ! -f "$LAMBDA_SCRIPT" ]]; then
  echo "Error: Lambda function not found at ${LAMBDA_SCRIPT}" >&2
  exit 1
fi

echo "Step 1: Ensuring S3 bucket '${S3_BUCKET}' exists..."
if ! aws s3api head-bucket --bucket "${S3_BUCKET}" 2>/dev/null; then
  echo "Bucket does not exist. Creating '${S3_BUCKET}'..."
  if ! aws s3 mb "s3://${S3_BUCKET}" --region "${REGION}"; then
    echo "Error: Failed to create S3 bucket '${S3_BUCKET}'. The name may already be taken globally." >&2
    exit 1
  fi
  echo "Bucket '${S3_BUCKET}' created successfully."
else
  echo "Bucket '${S3_BUCKET}' already exists."
fi

echo "Step 2: Uploading Glue ETL script to s3://${S3_BUCKET}/scripts/..."
if ! aws s3 cp "$GLUE_SCRIPT" "s3://${S3_BUCKET}/scripts/sample-glue-job-iceberg-materializedview-builder.py"; then
  echo "Error: Failed to upload Glue script to S3" >&2
  exit 1
fi

echo "Step 3: Packaging and uploading Lambda function to s3://${S3_BUCKET}/lambda/..."
LAMBDA_ZIP=$(mktemp /tmp/lambda_function_XXXXXX.zip)
trap "rm -f ${LAMBDA_ZIP}" EXIT
if ! python3 -c "
import zipfile, os, sys
src = sys.argv[1]
dst = sys.argv[2]
with zipfile.ZipFile(dst, 'w', zipfile.ZIP_DEFLATED) as zf:
    zf.write(src, os.path.basename(src))
" "$LAMBDA_SCRIPT" "$LAMBDA_ZIP"; then
  echo "Error: Failed to create Lambda zip package" >&2
  exit 1
fi
if ! aws s3 cp "$LAMBDA_ZIP" "s3://${S3_BUCKET}/lambda/lambda_function.zip"; then
  echo "Error: Failed to upload Lambda package to S3" >&2
  exit 1
fi

echo ""
echo "Done! Next steps:"
echo "  1. Deploy cloudformation/iceberg-pipeline-glue.yaml via the CloudFormation console"
echo "     Set GlueScriptBucketName to: ${S3_BUCKET}"
echo "  2. Run the Glue job to create the Iceberg table"
echo "  3. Deploy cloudformation/iceberg-pipeline-firehose.yaml via the CloudFormation console"
echo "     Set LambdaScriptBucketName to: ${S3_BUCKET}"
