# Single CloudFormation Stack Consolidation Bugfix Design

## Overview

The Iceberg data pipeline currently requires 4 manual, sequential deployment steps because
Amazon Data Firehose's `IcebergDestinationConfiguration` requires the target Glue Catalog
table (`stream_analytics.application_logs`) to exist at the time the Firehose delivery stream
resource is created. This forces a human to manually trigger the Glue job between two separate
CloudFormation stack deployments, making fully-automated (CI/CD) deployment impossible.

The fix consolidates `iceberg-pipeline-glue.yaml` and `iceberg-pipeline-firehose.yaml` into a
single template (`cloudformation/iceberg-pipeline.yaml`). A CloudFormation Custom Resource
backed by a new Lambda function automatically triggers the Glue job and polls until it reaches
`SUCCEEDED` state before CloudFormation proceeds to create the Firehose delivery stream. The
Lake Formation vs. no-Lake-Formation variants are unified via a single `EnableLakeFormation`
condition parameter, replacing the two separate Glue template files.

---

## Glossary

- **Bug_Condition (C)**: The condition that triggers the bug — a deployment attempt where the
  consolidated template does not exist and the Firehose resource creation is attempted before
  the Glue Catalog table exists.
- **Property (P)**: The desired behavior — a single `aws cloudformation deploy` invocation
  completes the full pipeline deployment unattended, with the Glue Catalog table guaranteed to
  exist before Firehose resource creation begins.
- **Preservation**: All existing runtime behaviors (Firehose delivery, Lambda log processing,
  DLQ retry, Glue job schema/output) that must remain unchanged by the fix.
- **Custom Resource**: A `AWS::CloudFormation::CustomResource` backed by a Lambda function
  (`GlueJobTriggerFunction`) that starts the Glue job run and polls for completion.
- **GlueJobTriggerFunction**: The new Lambda function (inline `ZipFile` code) responsible for
  starting the Glue job, polling `glue:GetJobRun` until `SUCCEEDED` or a terminal failure
  state, and sending a `SUCCESS` or `FAILED` signal to the CloudFormation pre-signed S3 URL.
- **IcebergGlueJob**: The `AWS::Glue::Job` resource that creates the Iceberg table and
  materialized view in the Glue Catalog.
- **GlueJobTriggerCustomResource**: The `AWS::CloudFormation::CustomResource` resource that
  invokes `GlueJobTriggerFunction` and blocks CloudFormation until the Glue job completes.
- **IcebergFirehoseStream**: The `AWS::KinesisFirehose::DeliveryStream` resource that depends
  on `GlueJobTriggerCustomResource`, ensuring the Glue Catalog table exists first.
- **EnableLakeFormation**: A `String` parameter (`true`/`false`) that controls whether Lake
  Formation IAM permissions and bucket policy statements are included via CloudFormation
  `Condition` blocks, replacing the two separate Glue template files.
- **DependsOn chain**: `IcebergGlueJob` → `GlueJobTriggerCustomResource` → `IcebergFirehoseStream`.

---

## Bug Details

### Bug Condition

The bug manifests when a user or CI/CD system attempts to deploy the full Iceberg pipeline
from scratch. Because no single consolidated template exists, the user must deploy two
separate stacks and manually trigger the Glue job between them. If the Firehose stack is
deployed before the Glue job completes, CloudFormation fails because the Glue Catalog table
does not yet exist. If the Glue job is skipped, the failure is silent until the Firehose
stack deployment errors out.

**Formal Specification:**
```
FUNCTION isBugCondition(deploymentAttempt)
  INPUT: deploymentAttempt of type DeploymentContext
  OUTPUT: boolean

  RETURN deploymentAttempt.templateCount > 1
         AND deploymentAttempt.requiresManualGlueJobTrigger = true
         AND deploymentAttempt.firehoseCreatedBeforeGlueTableExists = true
END FUNCTION
```

### Examples

- **Example 1 — Manual step skipped**: User deploys `iceberg-pipeline-glue.yaml`, forgets to
  run the Glue job, then deploys `iceberg-pipeline-firehose.yaml`. Firehose stack fails with
  `Table stream_analytics.application_logs does not exist`.
- **Example 2 — CI/CD pipeline blocked**: Automated pipeline deploys the Glue stack, then
  immediately deploys the Firehose stack without waiting for a manual Glue job trigger. The
  Firehose resource creation fails because the table does not exist yet.
- **Example 3 — Glue job fails silently**: User triggers the Glue job manually but it fails
  mid-run. The user does not notice and deploys the Firehose stack anyway. The Firehose stack
  fails because the table was never created.
- **Edge case — Re-deployment**: On a re-deployment (stack update), the Glue job has already
  run and the table exists. The Custom Resource must handle this idempotently (re-trigger is
  acceptable; the Glue job is idempotent via `CREATE TABLE IF NOT EXISTS`).

---

## Expected Behavior

### Preservation Requirements

**Unchanged Behaviors:**
- The Glue job MUST continue to create the `stream_analytics` database, the
  `application_logs` Iceberg table, and the `application_logs_mv` materialized view with the
  same schema and S3 warehouse location as the existing standalone execution.
- The Firehose delivery stream MUST continue to use the same `IcebergDestinationConfiguration`
  (catalog ARN, database name, table name, S3 bucket, buffering hints, retry options) as the
  existing `iceberg-pipeline-firehose.yaml` stack.
- The Lambda function (`CloudWatchLogsToIcebergFunction`) MUST continue to parse, transform,
  and forward CloudWatch Logs records to Firehose using the same logic as `lambda_function.py`.
- The DLQ retry mechanism (SQS `FailedEventsQueue` → `PermanentFailureQueue` → Lambda
  `EventSourceMapping`) MUST continue to reprocess failed events with the same configuration.
- When `EnableLakeFormation=true`, the same Lake Formation IAM permissions and bucket policy
  statements as `iceberg-pipeline-glue.yaml` MUST be applied.
- When `EnableLakeFormation=false`, the same no-Lake-Formation IAM permissions and bucket
  policy as `iceberg-pipeline-glue-no-lakeformation.yaml` MUST be applied.
- The `deploy.sh` artifact upload paths (`scripts/sample-glue-job-iceberg-materializedview-builder.py`
  and `lambda/lambda_function.zip`) MUST remain unchanged.

**Scope:**
All inputs that do NOT involve the initial deployment ordering (i.e., all runtime behaviors
after the stack is deployed) should be completely unaffected by this fix. This includes:
- Firehose record delivery to the Iceberg table
- Lambda CloudWatch Logs processing and Firehose forwarding
- SQS DLQ reprocessing
- Glue job schema and output (table structure, materialized view)
- IAM role permissions for Glue, Firehose, and Lambda

---

## Hypothesized Root Cause

The root cause is architectural, not a code defect in any single function:

1. **Missing orchestration layer**: There is no mechanism in the existing two-stack design to
   automatically trigger the Glue job and wait for it to complete before Firehose creation.
   CloudFormation has no native resource type that can "run a Glue job and wait."

2. **Hard dependency not modeled in CloudFormation**: The Firehose `IcebergDestinationConfiguration`
   has an implicit runtime dependency on the Glue Catalog table existing, but this dependency
   is not expressed as a CloudFormation `DependsOn` or resource reference — because the table
   is created by a Glue job run, not by a `AWS::Glue::Table` resource.

3. **Two-template split prevents single-command deployment**: Because the Glue infrastructure
   and Firehose infrastructure live in separate templates, there is no way to express the
   cross-stack dependency without a Custom Resource or manual intervention.

4. **No failure propagation**: If the Glue job fails, the two-stack design provides no
   mechanism to surface that failure to the CloudFormation deployment, so the operator may
   not notice until the Firehose stack fails for an apparently unrelated reason.

---

## Correctness Properties

Property 1: Bug Condition - Single-Command Deployment Completes Unattended

_For any_ deployment attempt where `isBugCondition` holds (i.e., the user is deploying the
full Iceberg pipeline from scratch or re-deploying), the consolidated template SHALL complete
the full deployment — including Glue job execution and Firehose stream creation — in a single
`aws cloudformation deploy` invocation without requiring manual console interaction, and SHALL
signal failure to CloudFormation if the Glue job does not reach `SUCCEEDED` state.

**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6**

Property 2: Preservation - Runtime Behavior Unchanged

_For any_ input that is NOT related to the deployment orchestration (i.e., all runtime
operations after the stack is deployed: Firehose record delivery, Lambda log processing, DLQ
retry, Glue job schema/output, IAM permissions), the consolidated template SHALL produce
exactly the same behavior as the original two-stack deployment, preserving all existing
functionality for Firehose delivery, Lambda processing, DLQ retry, Lake Formation and
no-Lake-Formation configurations, and artifact upload paths.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7**

---

## Fix Implementation

### Changes Required

**New File**: `cloudformation/iceberg-pipeline.yaml`

This single consolidated template merges `iceberg-pipeline-glue.yaml`,
`iceberg-pipeline-glue-no-lakeformation.yaml`, and `iceberg-pipeline-firehose.yaml` into one
file, adding the Custom Resource infrastructure.

**Deprecated Files** (kept for reference, not deleted):
- `cloudformation/iceberg-pipeline-glue.yaml`
- `cloudformation/iceberg-pipeline-glue-no-lakeformation.yaml`
- `cloudformation/iceberg-pipeline-firehose.yaml`

### Specific Changes

1. **New Parameter — `EnableLakeFormation`**:
   - Type: `String`, AllowedValues: `["true", "false"]`, Default: `"true"`
   - Drives a `Condition: LakeFormationEnabled: !Equals [!Ref EnableLakeFormation, "true"]`
   - Used to conditionally include Lake Formation IAM policies and bucket policy statements,
     replacing the two separate Glue template files.

2. **New Parameter — `GlueJobTriggerFunctionName`**:
   - Type: `String`, Default: `iceberg-glue-job-trigger`
   - Name for the Custom Resource Lambda function.

3. **New IAM Role — `GlueJobTriggerRole`**:
   - Trusted principal: `lambda.amazonaws.com`
   - Managed policy: `AWSLambdaBasicExecutionRole`
   - Inline policy `GlueJobAccess`:
     - `glue:StartJobRun` on the Glue job ARN
     - `glue:GetJobRun` on the Glue job ARN (wildcard run ID: `arn:aws:glue:...:job/${GlueJobName}/jobrun/*`)

4. **New Lambda Function — `GlueJobTriggerFunction`**:
   - Runtime: `python3.12`
   - Handler: `index.handler`
   - Timeout: `900` seconds (15 minutes — accommodates Glue job startup + execution time)
   - Code: inline `ZipFile` (no S3 artifact needed)
   - Logic:
     - On `Create` and `Update` events: call `glue:StartJobRun`, then poll `glue:GetJobRun`
       every 30 seconds until state is `SUCCEEDED`, `FAILED`, `ERROR`, `TIMEOUT`, or
       `STOPPED`. Send `SUCCESS` or `FAILED` to the CloudFormation pre-signed URL.
     - On `Delete` events: send `SUCCESS` immediately (no cleanup needed).
     - Idempotency: the Glue job script uses `CREATE TABLE IF NOT EXISTS` and
       `DROP MATERIALIZED VIEW IF EXISTS`, so re-runs are safe.
     - Error handling: catch all exceptions, send `FAILED` signal with error message to
       prevent CloudFormation from waiting until its own 1-hour timeout.

   **Pseudocode:**
   ```
   FUNCTION handler(event, context)
     IF event.RequestType IN ['Create', 'Update'] THEN
       jobRunId := glue.start_job_run(JobName=GlueJobName)
       LOOP
         response := glue.get_job_run(JobName=GlueJobName, RunId=jobRunId)
         state := response.JobRun.JobRunState
         IF state == 'SUCCEEDED' THEN
           send_cfn_response(event, SUCCESS)
           RETURN
         ELSE IF state IN ['FAILED', 'ERROR', 'TIMEOUT', 'STOPPED'] THEN
           send_cfn_response(event, FAILED, reason=state)
           RETURN
         END IF
         sleep(30)
       END LOOP
     ELSE IF event.RequestType == 'Delete' THEN
       send_cfn_response(event, SUCCESS)
     END IF
   END FUNCTION
   ```

5. **New Custom Resource — `GlueJobTriggerCustomResource`**:
   - Type: `AWS::CloudFormation::CustomResource`
   - `ServiceToken`: `!GetAtt GlueJobTriggerFunction.Arn`
   - Properties passed to Lambda: `GlueJobName: !Ref GlueJobName`
   - `DependsOn: IcebergGlueJob` — ensures the Glue job resource exists before triggering
   - CloudFormation resource timeout: default (1 hour) is sufficient given Lambda timeout of
     15 minutes; Lambda will always signal before CloudFormation times out.

6. **Modified Resource — `IcebergFirehoseStream`**:
   - Add `DependsOn: GlueJobTriggerCustomResource` to the existing `DependsOn` list.
   - This is the critical dependency that guarantees the Glue Catalog table exists before
     Firehose creation begins.

7. **Conditional Lake Formation resources** (replacing two separate template files):
   - `IcebergDataBucketPolicy`: use `!If [LakeFormationEnabled, <lf-policy>, <no-lf-policy>]`
     to switch between the Lake Formation service role principal and the Glue job role
     principal in the bucket policy.
   - `GlueJobRole` Lake Formation inline policies: wrapped in `!If [LakeFormationEnabled, ...]`
     to conditionally include `lakeformation:GetDataAccess`,
     `lakeformation:GetTemporaryGluePartitionCredentials`,
     `lakeformation:GetTemporaryGlueTableCredentials`.
   - Glue job `--conf` argument for `lakeformation-enabled`: use
     `!If [LakeFormationEnabled, "true", "false"]`.

### DependsOn Chain

```
IcebergGlueJob
  ↓ (DependsOn)
GlueJobTriggerCustomResource   ← blocks until Glue job reaches SUCCEEDED
  ↓ (DependsOn)
IcebergFirehoseStream          ← Glue Catalog table guaranteed to exist
```

### Timeout Considerations

| Component | Timeout | Rationale |
|---|---|---|
| `GlueJobTriggerFunction` Lambda | 900s (15 min) | Glue job takes ~3–5 min; 15 min provides ample headroom |
| CloudFormation Custom Resource | 1 hour (default) | Lambda always signals before CFN times out |
| Glue job itself | No explicit timeout set | Controlled by Lambda polling; Lambda will signal FAILED if Glue job enters terminal state |

### Stack Update and Delete Idempotency

- **Stack Update**: The Custom Resource `Update` event re-triggers the Glue job. The Glue
  script uses `CREATE TABLE IF NOT EXISTS` and `DROP MATERIALIZED VIEW IF EXISTS`, making
  re-runs safe. The materialized view is dropped and recreated on each run.
- **Stack Delete**: The Custom Resource `Delete` event sends `SUCCESS` immediately. No Glue
  job cleanup is needed. S3 buckets with `DeletionPolicy: Retain` (recommended) preserve data.
- **Concurrent runs**: Each `StartJobRun` call returns a unique `RunId`; the Lambda polls
  only that specific run, so concurrent stack operations do not interfere.

---

## Testing Strategy

### Validation Approach

The testing strategy follows a two-phase approach: first, surface counterexamples that
demonstrate the bug on the unfixed (two-stack) deployment model, then verify the fix works
correctly and preserves existing behavior.

### Exploratory Bug Condition Checking

**Goal**: Surface counterexamples that demonstrate the bug BEFORE implementing the fix.
Confirm or refute the root cause analysis. If we refute, we will need to re-hypothesize.

**Test Plan**: Write tests that validate the CloudFormation template structure — specifically
that the `DependsOn` chain is absent in the old templates and present in the new consolidated
template. Run these tests against the UNFIXED templates to observe failures.

**Test Cases**:
1. **Missing consolidated template test**: Assert that `cloudformation/iceberg-pipeline.yaml`
   does not exist in the unfixed codebase (will pass on unfixed code, confirming the bug).
2. **Missing DependsOn test**: Parse `iceberg-pipeline-firehose.yaml` and assert that
   `IcebergFirehoseStream` has no dependency on any Glue job trigger resource (will pass on
   unfixed code, confirming the structural gap).
3. **Missing Custom Resource test**: Assert that neither existing template contains a
   `AWS::CloudFormation::CustomResource` or `AWS::Lambda::Function` with Glue trigger logic
   (will pass on unfixed code, confirming the missing orchestration layer).
4. **Two-template requirement test**: Assert that deploying the pipeline requires more than
   one template file (will pass on unfixed code, confirming the multi-step deployment problem).

**Expected Counterexamples**:
- No `GlueJobTriggerCustomResource` resource exists in any template
- `IcebergFirehoseStream` has no dependency on a Glue job completion signal
- Possible causes: no Custom Resource, no polling Lambda, no DependsOn chain

### Fix Checking

**Goal**: Verify that for all inputs where the bug condition holds (fresh deployment attempt),
the fixed consolidated template produces the expected behavior.

**Pseudocode:**
```
FOR ALL deploymentAttempt WHERE isBugCondition(deploymentAttempt) DO
  result := deploy(iceberg-pipeline.yaml, deploymentAttempt.parameters)
  ASSERT result.firehoseStreamExists = true
  ASSERT result.glueCatalogTableExists = true
  ASSERT result.deploymentStepCount = 1
  ASSERT result.requiresManualIntervention = false
END FOR
```

### Preservation Checking

**Goal**: Verify that for all inputs where the bug condition does NOT hold (runtime operations
after deployment), the consolidated template produces the same behavior as the original
two-stack deployment.

**Pseudocode:**
```
FOR ALL runtimeInput WHERE NOT isBugCondition(runtimeInput) DO
  ASSERT consolidatedStack(runtimeInput) = originalTwoStackDeployment(runtimeInput)
END FOR
```

**Testing Approach**: Property-based testing is recommended for preservation checking because:
- It generates many test cases automatically across the input domain (e.g., random log events,
  random Firehose record batches, random parameter combinations)
- It catches edge cases that manual unit tests might miss
- It provides strong guarantees that behavior is unchanged for all non-deployment inputs

**Test Plan**: Observe behavior of existing tests against the original templates, then verify
the same tests pass against the consolidated template.

**Test Cases**:
1. **Template structure preservation**: Verify all resources from both original templates are
   present in the consolidated template with the same logical configuration.
2. **Lake Formation condition preservation**: Verify that `EnableLakeFormation=true` produces
   the same IAM policies and bucket policy as `iceberg-pipeline-glue.yaml`, and
   `EnableLakeFormation=false` produces the same as `iceberg-pipeline-glue-no-lakeformation.yaml`.
3. **Firehose configuration preservation**: Verify `IcebergDestinationConfiguration` in the
   consolidated template matches the original `iceberg-pipeline-firehose.yaml` exactly.
4. **Lambda function preservation**: Verify `CloudWatchLogsToIcebergFunction` configuration
   (runtime, handler, environment variables, DLQ, memory, timeout) is unchanged.
5. **Parameter preservation**: Verify all parameters from both original templates are present
   in the consolidated template with the same defaults and descriptions.

### Unit Tests

- Test that `cloudformation/iceberg-pipeline.yaml` is valid CloudFormation YAML (parseable,
  no syntax errors).
- Test that `IcebergFirehoseStream` has `GlueJobTriggerCustomResource` in its `DependsOn`.
- Test that `GlueJobTriggerCustomResource` has `IcebergGlueJob` in its `DependsOn`.
- Test that `GlueJobTriggerFunction` has a timeout of 900 seconds.
- Test that `GlueJobTriggerRole` has `glue:StartJobRun` and `glue:GetJobRun` permissions.
- Test that the `EnableLakeFormation` parameter exists with `AllowedValues: ["true", "false"]`.
- Test that the `LakeFormationEnabled` condition is defined and references `EnableLakeFormation`.
- Test edge cases: `EnableLakeFormation=true` includes Lake Formation policies;
  `EnableLakeFormation=false` excludes them.

### Property-Based Tests

- Generate random combinations of parameter values and verify the template renders without
  errors (CloudFormation `ValidateTemplate` or cfn-lint).
- Generate random log event payloads and verify the Lambda function (`lambda_function.py`)
  processes them identically before and after the consolidation (no changes to Lambda code).
- Generate random Glue job state sequences (`RUNNING` → `SUCCEEDED`, `RUNNING` → `FAILED`,
  etc.) and verify the Custom Resource Lambda signals the correct CloudFormation response.
- Test that for any non-`Create`/`Update` Custom Resource event type (`Delete`), the Lambda
  always signals `SUCCESS` regardless of Glue job state.

### Integration Tests

- Deploy the consolidated template to a test AWS account and verify the full pipeline
  completes in a single `aws cloudformation deploy` invocation.
- Verify the Glue Catalog table `stream_analytics.application_logs` exists after stack
  creation.
- Verify the Firehose delivery stream is active and can accept records.
- Verify that sending a test CloudWatch Logs event results in a record delivered to the
  Iceberg table via Firehose.
- Verify stack update (re-deploy) completes successfully with the Glue job re-running
  idempotently.
- Verify stack delete completes successfully without errors.
