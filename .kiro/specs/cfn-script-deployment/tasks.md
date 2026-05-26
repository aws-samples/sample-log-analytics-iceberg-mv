# Implementation Plan: CFN Script Deployment

## Overview

Replace the manual `scripts/deploy.sh` artifact upload step with a CloudFormation Custom Resource that automatically uploads the Glue ETL script and Lambda function zip to S3 during stack deployment. Implementation adds three new resources (ArtifactUploaderRole, ArtifactUploaderFunction, ArtifactUploaderCustomResource) to `cloudformation/iceberg-pipeline.yaml` and updates dependency ordering for existing resources.

## Tasks

- [x] 1. Add ArtifactUploaderRole to the CloudFormation template
  - [x] 1.1 Define the IAM role resource in `cloudformation/iceberg-pipeline.yaml`
    - Add `ArtifactUploaderRole` of type `AWS::IAM::Role` in the Resources section
    - AssumeRolePolicyDocument allows `lambda.amazonaws.com` service principal
    - Attach `AWSLambdaBasicExecutionRole` managed policy
    - Add inline policy granting `s3:PutObject` scoped to `arn:aws:s3:::${GlueScriptBucketName}/scripts/*` and `arn:aws:s3:::${LambdaScriptBucketName}/lambda/*`
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [x] 2. Add ArtifactUploaderFunction to the CloudFormation template
  - [x] 2.1 Define the Lambda function resource in `cloudformation/iceberg-pipeline.yaml`
    - Add `ArtifactUploaderFunction` of type `AWS::Lambda::Function`
    - Runtime: python3.12, Handler: index.handler, Timeout: 120
    - Role: `!GetAtt ArtifactUploaderRole.Arn`
    - Code.ZipFile: inline Python handler (~1100 chars) that:
      - On Delete: sends SUCCESS response immediately (no-op)
      - On Create/Update: decodes GlueScriptEncoded and LambdaScriptEncoded from ResourceProperties (base64 → zlib decompress), uploads Glue script to `scripts/sample-glue-job-iceberg-materializedview-builder.py`, creates in-memory zip with `lambda_function.py` entry, uploads to `lambda/lambda_function.zip`
      - On any exception: sends FAILED response with error message
      - All responses include PhysicalResourceId, StackId, RequestId, LogicalResourceId
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 3.1, 3.2, 3.3, 5.1, 5.2, 5.3, 5.4_

- [x] 3. Add ArtifactUploaderCustomResource and encode script content
  - [x] 3.1 Encode source files and add the Custom Resource to the template
    - Add `ArtifactUploaderCustomResource` of type `AWS::CloudFormation::CustomResource`
    - ServiceToken: `!GetAtt ArtifactUploaderFunction.Arn`
    - GlueBucket: `!Ref GlueScriptBucketName`
    - LambdaBucket: `!Ref LambdaScriptBucketName`
    - GlueScriptEncoded: base64(zlib.compress(scripts/sample-glue-job-iceberg-materializedview-builder.py, level=9))
    - LambdaScriptEncoded: base64(zlib.compress(scripts/lambda_function.py, level=9))
    - DependsOn the S3 bucket resources if they are created within the stack
    - _Requirements: 4.1, 2.2, 3.3, 7.1, 7.2_

  - [x] 3.2 Add DependsOn relationships to downstream resources
    - Add `ArtifactUploaderCustomResource` to the DependsOn list of `IcebergGlueJob`
    - Add `ArtifactUploaderCustomResource` to the DependsOn list of `CloudWatchLogsToIcebergFunction`
    - _Requirements: 4.2, 4.3, 4.4_

- [x] 4. Checkpoint - Verify template structure
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Write property-based tests for Custom Resource handler logic
  - [x] 5.1 Add conftest fixture for the consolidated template
    - Update `tests/conftest.py` to add a fixture that loads `cloudformation/iceberg-pipeline.yaml`
    - _Requirements: 1.1, 4.1_

  - [ ]* 5.2 Write property test for artifact upload path correctness
    - **Property 1: Artifact Upload Path Correctness**
    - Generate random valid S3 bucket names and Create/Update event types
    - Mock boto3 S3 client, execute handler, verify put_object called with correct bucket/key
    - Verify SUCCESS response sent
    - **Validates: Requirements 2.1, 3.2, 5.1**

  - [ ]* 5.3 Write property test for zip round-trip preservation
    - **Property 2: Zip Round-Trip Preservation**
    - Generate random Python source strings (printable ASCII, various lengths)
    - Create zip using same logic as handler, extract and compare to original
    - Verify filename is `lambda_function.py`
    - **Validates: Requirements 3.1, 3.4**

  - [ ]* 5.4 Write property test for error signaling
    - **Property 3: Error Signaling**
    - Generate random error messages, mock S3 to raise ClientError
    - Execute handler, verify FAILED response contains the error message
    - **Validates: Requirements 5.2**

  - [ ]* 5.5 Write property test for delete event no-op
    - **Property 4: Delete Event No-Op**
    - Generate random ResourceProperties content
    - Execute handler with Delete event, verify SUCCESS and zero S3 calls
    - **Validates: Requirements 5.3**

  - [ ]* 5.6 Write property test for response structure completeness
    - **Property 5: Response Structure Completeness**
    - Generate random events (all types, random IDs, random properties)
    - Execute handler for both success and failure paths
    - Verify response contains PhysicalResourceId, StackId, RequestId, LogicalResourceId
    - **Validates: Requirements 5.4**

- [x] 6. Write unit tests for template structure verification
  - [ ]* 6.1 Write unit tests for ArtifactUploader resources
    - Verify `ArtifactUploaderRole`, `ArtifactUploaderFunction`, `ArtifactUploaderCustomResource` exist in template
    - Verify ArtifactUploaderFunction runtime is python3.12
    - Verify ArtifactUploaderFunction timeout >= 120
    - Verify ArtifactUploaderRole trust policy allows lambda.amazonaws.com
    - Verify ArtifactUploaderRole has AWSLambdaBasicExecutionRole managed policy
    - Verify inline policy grants s3:PutObject scoped to correct ARN patterns
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 6.1, 6.2, 6.3, 6.4_

  - [ ]* 6.2 Write unit tests for DependsOn and encoded content
    - Verify IcebergGlueJob DependsOn includes ArtifactUploaderCustomResource
    - Verify CloudWatchLogsToIcebergFunction DependsOn includes ArtifactUploaderCustomResource
    - Verify GlueScriptEncoded property decodes to actual Glue script content
    - Verify LambdaScriptEncoded property decodes to actual Lambda function content
    - _Requirements: 4.2, 4.3, 2.2, 3.3_

- [x] 7. Final checkpoint - Run all tests and verify
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The inline Lambda handler must stay under 4096 characters (ZipFile limit)
- Encoded script content is passed via Custom Resource Properties (no size limit in template body)
- Property tests use the `hypothesis` library already in requirements.txt
- The conftest.py fixture needs updating to point to `iceberg-pipeline.yaml` for these tests
