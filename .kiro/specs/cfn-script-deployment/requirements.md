# Requirements Document

## Introduction

This feature automates the manual `scripts/deploy.sh` artifact upload step by introducing a CloudFormation Custom Resource into the `cloudformation/iceberg-pipeline.yaml` template. The Custom Resource Lambda will upload the Glue ETL script and Lambda function zip to S3 as the first step of stack deployment, eliminating the need to run `deploy.sh` separately before deploying the stack. This enables a single `aws cloudformation deploy` command to provision the entire pipeline.

## Glossary

- **Custom_Resource_Lambda**: The AWS Lambda function that acts as a CloudFormation Custom Resource handler, responsible for uploading deployment artifacts to S3 during stack creation and update operations
- **Artifact_Uploader**: The logical CloudFormation Custom Resource (AWS::CloudFormation::CustomResource) that triggers the Custom_Resource_Lambda during stack lifecycle events
- **Glue_Script_Artifact**: The Glue ETL script file (`sample-glue-job-iceberg-materializedview-builder.py`) that must be uploaded to S3 for the Glue job to reference
- **Lambda_Zip_Artifact**: The zipped Lambda function deployment package (`lambda_function.zip`) created from `lambda_function.py` that must be uploaded to S3 for the Lambda function resource to reference
- **Script_Bucket**: The S3 bucket (referenced by the GlueScriptBucketName parameter) where deployment artifacts are stored
- **Stack**: The CloudFormation stack defined by `cloudformation/iceberg-pipeline.yaml`
- **Inline_Code**: Lambda function source code embedded directly in the CloudFormation template using the `ZipFile` property of `AWS::Lambda::Function`

## Requirements

### Requirement 1: Custom Resource Lambda Definition

**User Story:** As a DevOps engineer, I want a Custom Resource Lambda function defined inline in the CloudFormation template, so that no external dependencies are needed to deploy the stack.

#### Acceptance Criteria

1. THE Stack SHALL define a Lambda function resource with inline Python code that handles CloudFormation Custom Resource lifecycle events (Create, Update, Delete)
2. THE Custom_Resource_Lambda SHALL use the Python 3.12 runtime
3. THE Custom_Resource_Lambda SHALL have an IAM role with permissions to write objects to the Script_Bucket
4. THE Custom_Resource_Lambda SHALL have a timeout of at least 120 seconds to allow for artifact packaging and upload

### Requirement 2: Glue Script Upload

**User Story:** As a DevOps engineer, I want the Glue ETL script automatically uploaded to S3 during stack deployment, so that the Glue job can reference it without manual intervention.

#### Acceptance Criteria

1. WHEN a Create or Update event is received, THE Custom_Resource_Lambda SHALL upload the Glue_Script_Artifact to the path `s3://{GlueScriptBucketName}/scripts/sample-glue-job-iceberg-materializedview-builder.py`
2. THE Custom_Resource_Lambda SHALL embed the Glue_Script_Artifact content as a string constant within its inline code
3. WHEN the upload of the Glue_Script_Artifact succeeds, THE Custom_Resource_Lambda SHALL log the S3 destination path

### Requirement 3: Lambda Zip Upload

**User Story:** As a DevOps engineer, I want the Lambda function zip package automatically created and uploaded to S3 during stack deployment, so that the Lambda function resource can reference it without manual intervention.

#### Acceptance Criteria

1. WHEN a Create or Update event is received, THE Custom_Resource_Lambda SHALL create a zip archive containing `lambda_function.py` in memory
2. WHEN the zip archive is created, THE Custom_Resource_Lambda SHALL upload it to the path `s3://{LambdaScriptBucketName}/lambda/lambda_function.zip`
3. THE Custom_Resource_Lambda SHALL embed the Lambda function source code as a string constant within its inline code
4. THE Custom_Resource_Lambda SHALL produce a valid zip file that AWS Lambda can execute with handler `lambda_function.lambda_handler`

### Requirement 4: Deployment Ordering

**User Story:** As a DevOps engineer, I want the artifact upload to complete before any dependent resources are created, so that the Glue job and Lambda function can successfully reference their S3 artifacts.

#### Acceptance Criteria

1. THE Artifact_Uploader SHALL be defined as a `AWS::CloudFormation::CustomResource` resource in the Stack
2. THE IcebergGlueJob resource SHALL declare a DependsOn relationship on the Artifact_Uploader
3. THE CloudWatchLogsToIcebergFunction resource SHALL declare a DependsOn relationship on the Artifact_Uploader
4. WHEN the Artifact_Uploader reports SUCCESS, THE Stack SHALL proceed to create dependent resources

### Requirement 5: Error Handling and CloudFormation Signaling

**User Story:** As a DevOps engineer, I want the Custom Resource to properly signal success or failure to CloudFormation, so that stack deployment fails gracefully if artifact upload encounters errors.

#### Acceptance Criteria

1. WHEN all artifacts are uploaded successfully, THE Custom_Resource_Lambda SHALL send a SUCCESS response to the CloudFormation pre-signed URL
2. IF an error occurs during artifact upload, THEN THE Custom_Resource_Lambda SHALL send a FAILED response to the CloudFormation pre-signed URL with a descriptive error message
3. WHEN a Delete event is received, THE Custom_Resource_Lambda SHALL send a SUCCESS response without performing any artifact operations
4. THE Custom_Resource_Lambda SHALL include the PhysicalResourceId, StackId, RequestId, and LogicalResourceId in all responses to CloudFormation

### Requirement 6: Custom Resource IAM Permissions

**User Story:** As a DevOps engineer, I want the Custom Resource Lambda to have least-privilege IAM permissions, so that it can only write to the specific S3 paths it needs.

#### Acceptance Criteria

1. THE Stack SHALL define an IAM role for the Custom_Resource_Lambda with an AssumeRolePolicyDocument allowing the `lambda.amazonaws.com` service principal
2. THE IAM role SHALL grant `s3:PutObject` permission scoped to `arn:aws:s3:::{GlueScriptBucketName}/scripts/*` and `arn:aws:s3:::{LambdaScriptBucketName}/lambda/*`
3. THE IAM role SHALL include the `AWSLambdaBasicExecutionRole` managed policy for CloudWatch Logs access
4. THE IAM role SHALL NOT grant permissions beyond S3 PutObject and basic Lambda execution

### Requirement 7: Stack Bucket Dependency

**User Story:** As a DevOps engineer, I want the Custom Resource to run after the S3 bucket is created, so that the upload target exists when artifacts are written.

#### Acceptance Criteria

1. WHEN the GlueScriptBucketName references a bucket created within the same Stack, THE Artifact_Uploader SHALL declare a DependsOn relationship on that bucket resource
2. WHEN the LambdaScriptBucketName references a bucket created within the same Stack, THE Artifact_Uploader SHALL declare a DependsOn relationship on that bucket resource
