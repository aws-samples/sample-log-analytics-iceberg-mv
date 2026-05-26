"""Property-based tests for preservation of existing resource configurations.

These tests verify that the consolidated template (`cloudformation/iceberg-pipeline.yaml`)
preserves all configurations from the original templates exactly. They define the
preservation contract BEFORE the fix is implemented.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7**

Testing methodology: observation-first. We observe the exact configurations in the
original templates, then assert those same configurations must appear in the consolidated
template. Tests will FAIL on unfixed code because the consolidated template does not exist
yet — this is expected and documents the baseline contract.
"""

from pathlib import Path

import pytest
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st
from ruamel.yaml import YAML


# ── Paths ─────────────────────────────────────────────────────────────────────

CLOUDFORMATION_DIR = Path(__file__).resolve().parent.parent / "cloudformation"
FIREHOSE_TEMPLATE_PATH = CLOUDFORMATION_DIR / "iceberg-pipeline-firehose.yaml"
GLUE_TEMPLATE_PATH = CLOUDFORMATION_DIR / "iceberg-pipeline-glue.yaml"
GLUE_NO_LF_TEMPLATE_PATH = CLOUDFORMATION_DIR / "iceberg-pipeline-glue-no-lakeformation.yaml"
CONSOLIDATED_TEMPLATE_PATH = CLOUDFORMATION_DIR / "iceberg-pipeline.yaml"


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_template(path: Path) -> dict:
    """Load a CloudFormation YAML template and return as a dict."""
    yaml = YAML()
    with open(path, "r") as f:
        data = yaml.load(f)
    return dict(data) if data else {}


def get_consolidated_template() -> dict:
    """Load the consolidated template. Raises FileNotFoundError if it doesn't exist."""
    if not CONSOLIDATED_TEMPLATE_PATH.exists():
        pytest.fail(
            f"Consolidated template does not exist at {CONSOLIDATED_TEMPLATE_PATH}. "
            "This is expected on unfixed code — the preservation contract is documented."
        )
    return load_template(CONSOLIDATED_TEMPLATE_PATH)


# ── Observed Baseline Values (from original templates) ────────────────────────

# Observed from iceberg-pipeline-firehose.yaml: IcebergFirehoseStream configuration
OBSERVED_FIREHOSE_DELIVERY_STREAM_TYPE = "DirectPut"
OBSERVED_FIREHOSE_RETRY_DURATION = 3600
OBSERVED_FIREHOSE_PROCESSING_ENABLED = False
OBSERVED_FIREHOSE_COMPRESSION = "UNCOMPRESSED"

# Observed from iceberg-pipeline-firehose.yaml: Lambda function configuration
OBSERVED_LAMBDA_RUNTIME = "python3.11"
OBSERVED_LAMBDA_HANDLER = "lambda_function.lambda_handler"
OBSERVED_LAMBDA_ENV_VAR = "FIREHOSE_STREAM_NAME"
OBSERVED_LAMBDA_S3_KEY = "lambda/lambda_function.zip"

# Observed from iceberg-pipeline-firehose.yaml: SQS queue configuration
OBSERVED_MESSAGE_RETENTION_PERIOD = 1209600
OBSERVED_SQS_SSE_ENABLED = True
OBSERVED_MAX_RECEIVE_COUNT = 3

# Observed from iceberg-pipeline-glue.yaml: Spark configuration properties
OBSERVED_SPARK_PROPERTIES = [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.type=glue",
    "spark.sql.catalog.glue_catalog.warehouse=",
    "spark.sql.catalog.glue_catalog.glue.region=",
    "spark.sql.catalog.glue_catalog.glue.id=",
    "spark.sql.catalog.glue_catalog.glue.account-id=",
    "spark.sql.catalog.glue_catalog.client.region=",
    "spark.sql.catalog.glue_catalog.glue.lakeformation-enabled=",
    "spark.sql.defaultCatalog=glue_catalog",
    "spark.sql.optimizer.answerQueriesWithMVs.enabled=true",
    "spark.sql.materializedViews.metadataCache.enabled=true",
]

# Observed shared parameter defaults across templates
OBSERVED_SHARED_PARAMETER_DEFAULTS = {
    "IcebergDataBucketName": "my-company-iceberg-data-bucket",
    "IcebergErrorsBucketName": "my-company-iceberg-errors-bucket",
    "GlueJobName": "my-iceberg-glue-job",
    "WorkerType": "G.1X",
    "NumberOfWorkers": 2,
    "GlueScriptBucketName": "my-company-glue-scripts-bucket",
    "GlueJobRoleName": "my-iceberg-glue-job-role",
    "DatabaseName": "stream_analytics",
    "WarehouseDatabaseName": "stream_analytics",
    "FirehoseStreamName": "iceberg-logs-stream",
    "FirehoseBufferSizeMBs": 5,
    "FirehoseBufferIntervalSeconds": 30,
    "StreamDatabaseName": "stream_analytics",
    "StreamTableName": "application_logs",
    "LambdaFunctionName": "CloudWatchLogsToIceberg",
    "LambdaScriptBucketName": "my-company-glue-scripts-bucket",
    "LambdaTimeout": 300,
    "LambdaMemorySize": 512,
    "SubscriptionLogGroupName": "/aws/application/logs",
    "CreateSubscriptionLogGroup": "true",
    "LambdaFirehoseProcessorRoleName": "LambdaFirehoseProcessorRole",
    "FirehoseIcebergDeliveryRoleName": "FirehoseIcebergDeliveryRole",
}


# ── Test Classes ──────────────────────────────────────────────────────────────


class TestFirehoseConfigurationPreservation:
    """Assert that IcebergFirehoseStream in the consolidated template has the same
    IcebergDestinationConfiguration structure as iceberg-pipeline-firehose.yaml.

    **Validates: Requirements 3.2**
    """

    @given(buffer_size=st.integers(min_value=1, max_value=128))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_firehose_has_iceberg_destination_configuration(self, buffer_size):
        """The consolidated template must have IcebergDestinationConfiguration on the
        Firehose stream resource, matching the original template structure."""
        consolidated = get_consolidated_template()
        resources = consolidated.get("Resources", {})

        assert "IcebergFirehoseStream" in resources, (
            "IcebergFirehoseStream resource must exist in consolidated template"
        )

        firehose = resources["IcebergFirehoseStream"]
        props = firehose.get("Properties", {})

        assert "IcebergDestinationConfiguration" in props, (
            "IcebergFirehoseStream must have IcebergDestinationConfiguration"
        )

        iceberg_config = props["IcebergDestinationConfiguration"]

        # Verify all required sub-keys from the original template
        assert "CatalogConfiguration" in iceberg_config, (
            "IcebergDestinationConfiguration must have CatalogConfiguration"
        )
        assert "DestinationTableConfigurationList" in iceberg_config, (
            "IcebergDestinationConfiguration must have DestinationTableConfigurationList"
        )
        assert "BufferingHints" in iceberg_config, (
            "IcebergDestinationConfiguration must have BufferingHints"
        )
        assert "S3Configuration" in iceberg_config, (
            "IcebergDestinationConfiguration must have S3Configuration"
        )
        assert "RetryOptions" in iceberg_config, (
            "IcebergDestinationConfiguration must have RetryOptions"
        )

    @given(interval=st.integers(min_value=1, max_value=900))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_firehose_delivery_stream_type_preserved(self, interval):
        """DeliveryStreamType must remain DirectPut as in the original."""
        consolidated = get_consolidated_template()
        firehose = consolidated["Resources"]["IcebergFirehoseStream"]["Properties"]

        assert firehose.get("DeliveryStreamType") == OBSERVED_FIREHOSE_DELIVERY_STREAM_TYPE

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_firehose_retry_options_preserved(self, dummy):
        """RetryOptions.DurationInSeconds must be 3600 as in the original."""
        consolidated = get_consolidated_template()
        iceberg_config = consolidated["Resources"]["IcebergFirehoseStream"]["Properties"][
            "IcebergDestinationConfiguration"
        ]
        retry = iceberg_config["RetryOptions"]

        assert retry["DurationInSeconds"] == OBSERVED_FIREHOSE_RETRY_DURATION

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_firehose_processing_configuration_preserved(self, dummy):
        """ProcessingConfiguration.Enabled must be false as in the original."""
        consolidated = get_consolidated_template()
        iceberg_config = consolidated["Resources"]["IcebergFirehoseStream"]["Properties"][
            "IcebergDestinationConfiguration"
        ]
        processing = iceberg_config.get("ProcessingConfiguration", {})

        assert processing.get("Enabled") == OBSERVED_FIREHOSE_PROCESSING_ENABLED

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_firehose_s3_compression_preserved(self, dummy):
        """S3Configuration.CompressionFormat must be UNCOMPRESSED as in the original."""
        consolidated = get_consolidated_template()
        iceberg_config = consolidated["Resources"]["IcebergFirehoseStream"]["Properties"][
            "IcebergDestinationConfiguration"
        ]
        s3_config = iceberg_config["S3Configuration"]

        assert s3_config["CompressionFormat"] == OBSERVED_FIREHOSE_COMPRESSION


class TestLambdaFunctionPreservation:
    """Assert that CloudWatchLogsToIcebergFunction has the same runtime, handler,
    environment variable, DLQ config, and S3 code reference as the original.

    **Validates: Requirements 3.3**
    """

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_lambda_runtime_preserved(self, dummy):
        """Lambda runtime must be python3.11 as in the original."""
        consolidated = get_consolidated_template()
        resources = consolidated.get("Resources", {})

        assert "CloudWatchLogsToIcebergFunction" in resources, (
            "CloudWatchLogsToIcebergFunction must exist in consolidated template"
        )

        lambda_props = resources["CloudWatchLogsToIcebergFunction"]["Properties"]
        assert lambda_props["Runtime"] == OBSERVED_LAMBDA_RUNTIME

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_lambda_handler_preserved(self, dummy):
        """Lambda handler must be lambda_function.lambda_handler as in the original."""
        consolidated = get_consolidated_template()
        lambda_props = consolidated["Resources"]["CloudWatchLogsToIcebergFunction"]["Properties"]

        assert lambda_props["Handler"] == OBSERVED_LAMBDA_HANDLER

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_lambda_environment_variable_preserved(self, dummy):
        """Lambda must have FIREHOSE_STREAM_NAME environment variable."""
        consolidated = get_consolidated_template()
        lambda_props = consolidated["Resources"]["CloudWatchLogsToIcebergFunction"]["Properties"]

        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        assert OBSERVED_LAMBDA_ENV_VAR in env_vars, (
            f"Lambda must have {OBSERVED_LAMBDA_ENV_VAR} environment variable"
        )

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_lambda_dlq_config_preserved(self, dummy):
        """Lambda must have DeadLetterConfig as in the original."""
        consolidated = get_consolidated_template()
        lambda_props = consolidated["Resources"]["CloudWatchLogsToIcebergFunction"]["Properties"]

        assert "DeadLetterConfig" in lambda_props, (
            "Lambda must have DeadLetterConfig"
        )
        assert "TargetArn" in lambda_props["DeadLetterConfig"], (
            "DeadLetterConfig must have TargetArn"
        )

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_lambda_s3_code_reference_preserved(self, dummy):
        """Lambda code must reference lambda/lambda_function.zip as in the original."""
        consolidated = get_consolidated_template()
        lambda_props = consolidated["Resources"]["CloudWatchLogsToIcebergFunction"]["Properties"]

        code = lambda_props.get("Code", {})
        assert code.get("S3Key") == OBSERVED_LAMBDA_S3_KEY, (
            f"Lambda S3Key must be '{OBSERVED_LAMBDA_S3_KEY}'"
        )


class TestDLQPreservation:
    """Assert that FailedEventsQueue and PermanentFailureQueue SQS queues are present
    with the same MessageRetentionPeriod, SqsManagedSseEnabled, and RedrivePolicy.

    **Validates: Requirements 3.4**
    """

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_failed_events_queue_exists(self, dummy):
        """FailedEventsQueue must exist in the consolidated template."""
        consolidated = get_consolidated_template()
        resources = consolidated.get("Resources", {})

        assert "FailedEventsQueue" in resources, (
            "FailedEventsQueue must exist in consolidated template"
        )

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_permanent_failure_queue_exists(self, dummy):
        """PermanentFailureQueue must exist in the consolidated template."""
        consolidated = get_consolidated_template()
        resources = consolidated.get("Resources", {})

        assert "PermanentFailureQueue" in resources, (
            "PermanentFailureQueue must exist in consolidated template"
        )

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_failed_events_queue_retention_preserved(self, dummy):
        """FailedEventsQueue MessageRetentionPeriod must be 1209600."""
        consolidated = get_consolidated_template()
        queue_props = consolidated["Resources"]["FailedEventsQueue"]["Properties"]

        assert queue_props["MessageRetentionPeriod"] == OBSERVED_MESSAGE_RETENTION_PERIOD

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_permanent_failure_queue_retention_preserved(self, dummy):
        """PermanentFailureQueue MessageRetentionPeriod must be 1209600."""
        consolidated = get_consolidated_template()
        queue_props = consolidated["Resources"]["PermanentFailureQueue"]["Properties"]

        assert queue_props["MessageRetentionPeriod"] == OBSERVED_MESSAGE_RETENTION_PERIOD

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_failed_events_queue_sse_preserved(self, dummy):
        """FailedEventsQueue SqsManagedSseEnabled must be true."""
        consolidated = get_consolidated_template()
        queue_props = consolidated["Resources"]["FailedEventsQueue"]["Properties"]

        assert queue_props["SqsManagedSseEnabled"] == OBSERVED_SQS_SSE_ENABLED

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_permanent_failure_queue_sse_preserved(self, dummy):
        """PermanentFailureQueue SqsManagedSseEnabled must be true."""
        consolidated = get_consolidated_template()
        queue_props = consolidated["Resources"]["PermanentFailureQueue"]["Properties"]

        assert queue_props["SqsManagedSseEnabled"] == OBSERVED_SQS_SSE_ENABLED

    @given(dummy=st.integers(min_value=0, max_value=100))
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_failed_events_queue_redrive_policy_preserved(self, dummy):
        """FailedEventsQueue RedrivePolicy must have maxReceiveCount of 3."""
        consolidated = get_consolidated_template()
        queue_props = consolidated["Resources"]["FailedEventsQueue"]["Properties"]

        assert "RedrivePolicy" in queue_props, (
            "FailedEventsQueue must have RedrivePolicy"
        )
        redrive = queue_props["RedrivePolicy"]
        assert redrive["maxReceiveCount"] == OBSERVED_MAX_RECEIVE_COUNT


class TestParameterDefaultsPreservation:
    """For all parameters shared between the original templates and the consolidated
    template, assert that defaults are identical.

    **Validates: Requirements 3.5, 3.6, 3.7**
    """

    @given(
        param_name=st.sampled_from(list(OBSERVED_SHARED_PARAMETER_DEFAULTS.keys()))
    )
    @settings(max_examples=25, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_shared_parameter_defaults_preserved(self, param_name):
        """Each shared parameter must have the same default in the consolidated template."""
        consolidated = get_consolidated_template()
        params = consolidated.get("Parameters", {})

        expected_default = OBSERVED_SHARED_PARAMETER_DEFAULTS[param_name]

        assert param_name in params, (
            f"Parameter '{param_name}' must exist in consolidated template"
        )

        param_def = params[param_name]
        assert "Default" in param_def, (
            f"Parameter '{param_name}' must have a Default value"
        )

        actual_default = param_def["Default"]
        assert actual_default == expected_default, (
            f"Parameter '{param_name}' default must be '{expected_default}', "
            f"got '{actual_default}'"
        )


class TestGlueJobSparkConfigPreservation:
    """Assert that the --conf block in IcebergGlueJob.DefaultArguments contains all
    required Spark properties.

    **Validates: Requirements 3.1**
    """

    @given(
        spark_property=st.sampled_from(OBSERVED_SPARK_PROPERTIES)
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_spark_config_contains_required_properties(self, spark_property):
        """The --conf block must contain each required Spark property."""
        consolidated = get_consolidated_template()
        resources = consolidated.get("Resources", {})

        assert "IcebergGlueJob" in resources, (
            "IcebergGlueJob must exist in consolidated template"
        )

        glue_job = resources["IcebergGlueJob"]
        default_args = glue_job.get("Properties", {}).get("DefaultArguments", {})

        assert "--conf" in default_args, (
            "IcebergGlueJob.DefaultArguments must have --conf key"
        )

        conf_value = str(default_args["--conf"])
        assert spark_property in conf_value, (
            f"Spark config must contain '{spark_property}'"
        )


class TestS3BucketPublicAccessPreservation:
    """Assert that IcebergDataBucket and IcebergErrorsBucket both have all four
    PublicAccessBlockConfiguration settings set to true.

    **Validates: Requirements 3.5, 3.6**
    """

    PUBLIC_ACCESS_SETTINGS = [
        "BlockPublicAcls",
        "BlockPublicPolicy",
        "IgnorePublicAcls",
        "RestrictPublicBuckets",
    ]

    @given(
        bucket_name=st.sampled_from(["IcebergDataBucket", "IcebergErrorsBucket"]),
        setting=st.sampled_from(PUBLIC_ACCESS_SETTINGS),
    )
    @settings(max_examples=10, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_public_access_block_settings_preserved(self, bucket_name, setting):
        """Each S3 bucket must have all PublicAccessBlockConfiguration settings set to true."""
        consolidated = get_consolidated_template()
        resources = consolidated.get("Resources", {})

        assert bucket_name in resources, (
            f"{bucket_name} must exist in consolidated template"
        )

        bucket_props = resources[bucket_name].get("Properties", {})
        public_access = bucket_props.get("PublicAccessBlockConfiguration", {})

        assert setting in public_access, (
            f"{bucket_name} must have {setting} in PublicAccessBlockConfiguration"
        )
        assert public_access[setting] is True, (
            f"{bucket_name}.PublicAccessBlockConfiguration.{setting} must be true"
        )
