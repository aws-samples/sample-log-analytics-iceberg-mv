"""Fix verification tests for the consolidated CloudFormation template.

Validates that cloudformation/iceberg-pipeline.yaml correctly consolidates
both source templates and includes the Custom Resource DependsOn chain.
"""

from pathlib import Path

import pytest
from ruamel.yaml import YAML


CONSOLIDATED_TEMPLATE_PATH = (
    Path(__file__).resolve().parent.parent
    / "cloudformation"
    / "iceberg-pipeline.yaml"
)


@pytest.fixture
def consolidated_template():
    """Parse the consolidated CloudFormation template and return as dict."""
    yaml = YAML()
    with open(CONSOLIDATED_TEMPLATE_PATH, "r") as f:
        data = yaml.load(f)
    return dict(data) if data else {}


# ── Test 1: Valid YAML and parseable ──────────────────────────────────────────


def test_consolidated_template_is_valid_yaml(consolidated_template):
    """Test that cloudformation/iceberg-pipeline.yaml is valid YAML and parseable."""
    assert consolidated_template is not None
    assert "AWSTemplateFormatVersion" in consolidated_template
    assert "Resources" in consolidated_template
    assert "Parameters" in consolidated_template


# ── Test 2: IcebergFirehoseStream DependsOn contains GlueJobTriggerCustomResource


def test_firehose_depends_on_glue_job_trigger(consolidated_template):
    """Test that IcebergFirehoseStream.DependsOn contains GlueJobTriggerCustomResource."""
    resources = consolidated_template["Resources"]
    firehose = resources["IcebergFirehoseStream"]
    depends_on = firehose["DependsOn"]
    assert "GlueJobTriggerCustomResource" in depends_on


# ── Test 3: GlueJobTriggerCustomResource DependsOn contains IcebergGlueJob ────


def test_custom_resource_depends_on_glue_job(consolidated_template):
    """Test that GlueJobTriggerCustomResource.DependsOn contains IcebergGlueJob."""
    resources = consolidated_template["Resources"]
    custom_resource = resources["GlueJobTriggerCustomResource"]
    depends_on = custom_resource["DependsOn"]
    # DependsOn can be a string or a list
    if isinstance(depends_on, list):
        assert "IcebergGlueJob" in depends_on
    else:
        assert depends_on == "IcebergGlueJob"


# ── Test 4: GlueJobTriggerFunction has Timeout: 900 ──────────────────────────


def test_glue_job_trigger_function_timeout(consolidated_template):
    """Test that GlueJobTriggerFunction has Timeout: 900."""
    resources = consolidated_template["Resources"]
    trigger_function = resources["GlueJobTriggerFunction"]
    properties = trigger_function["Properties"]
    assert properties["Timeout"] == 900


# ── Test 5: GlueJobTriggerRole has glue:StartJobRun and glue:GetJobRun ───────


def test_glue_job_trigger_role_permissions(consolidated_template):
    """Test that GlueJobTriggerRole has glue:StartJobRun and glue:GetJobRun in its inline policy."""
    resources = consolidated_template["Resources"]
    role = resources["GlueJobTriggerRole"]
    policies = role["Properties"]["Policies"]

    # Find the GlueJobAccess policy
    glue_policy = None
    for policy in policies:
        if policy["PolicyName"] == "GlueJobAccess":
            glue_policy = policy
            break

    assert glue_policy is not None, "GlueJobAccess policy not found"

    statements = glue_policy["PolicyDocument"]["Statement"]
    all_actions = []
    for stmt in statements:
        actions = stmt["Action"]
        if isinstance(actions, list):
            all_actions.extend(actions)
        else:
            all_actions.append(actions)

    assert "glue:StartJobRun" in all_actions
    assert "glue:GetJobRun" in all_actions


# ── Test 6: EnableLakeFormation parameter exists with correct config ──────────


def test_enable_lake_formation_parameter(consolidated_template):
    """Test that EnableLakeFormation parameter exists with AllowedValues and Default."""
    parameters = consolidated_template["Parameters"]
    assert "EnableLakeFormation" in parameters

    param = parameters["EnableLakeFormation"]
    assert param["Default"] == "false"
    assert param["AllowedValues"] == ["true", "false"]


# ── Test 7: LakeFormationEnabled condition is defined and references param ────


def test_lake_formation_enabled_condition(consolidated_template):
    """Test that LakeFormationEnabled condition is defined and references EnableLakeFormation."""
    conditions = consolidated_template["Conditions"]
    assert "LakeFormationEnabled" in conditions

    # The condition should reference EnableLakeFormation via !Equals
    # In ruamel.yaml, this is parsed as a tagged object
    condition_value = conditions["LakeFormationEnabled"]

    # Convert the condition to string representation to check it references the parameter
    condition_str = str(condition_value)
    assert "EnableLakeFormation" in condition_str


# ── Test 8: EnableLakeFormation=true includes Lake Formation service role ─────


def test_bucket_policy_lake_formation_enabled(consolidated_template):
    """Test that when EnableLakeFormation=true the IcebergDataBucketPolicy includes the Lake Formation service role principal."""
    resources = consolidated_template["Resources"]
    bucket_policy = resources["IcebergDataBucketPolicy"]
    policy_doc = bucket_policy["Properties"]["PolicyDocument"]
    statements = policy_doc["Statement"]

    # The bucket policy uses !If to switch between principals
    # Check that the Lake Formation service role ARN pattern is referenced somewhere
    policy_str = str(statements)
    assert "lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess" in policy_str


# ── Test 9: EnableLakeFormation=false includes Glue job role principal ────────


def test_bucket_policy_glue_job_role(consolidated_template):
    """Test that when EnableLakeFormation=false the IcebergDataBucketPolicy includes the Glue job role principal."""
    resources = consolidated_template["Resources"]
    bucket_policy = resources["IcebergDataBucketPolicy"]
    policy_doc = bucket_policy["Properties"]["PolicyDocument"]
    statements = policy_doc["Statement"]

    # The bucket policy uses !If to switch between principals
    # Check that GlueJobRole is referenced (for the non-LakeFormation case)
    policy_str = str(statements)
    assert "GlueJobRole" in policy_str


# ── Test 10: All resources from iceberg-pipeline-firehose.yaml are present ────


def test_firehose_template_resources_present(consolidated_template):
    """Test that all resources from iceberg-pipeline-firehose.yaml are present in the consolidated template."""
    resources = consolidated_template["Resources"]

    expected_firehose_resources = [
        "IcebergFirehoseStream",
        "CloudWatchLogsToIcebergFunction",
        "LambdaFirehoseProcessorRole",
        "FirehoseIcebergDeliveryRole",
        "FailedEventsQueue",
        "PermanentFailureQueue",
        "DLQEventSourceMapping",
        "IcebergStreamingSubscriptionFilter",
    ]

    for resource_name in expected_firehose_resources:
        assert resource_name in resources, (
            f"Resource '{resource_name}' from iceberg-pipeline-firehose.yaml "
            f"is missing in the consolidated template"
        )


# ── Test 11: All resources from iceberg-pipeline-glue.yaml are present ────────


def test_glue_template_resources_present(consolidated_template):
    """Test that all resources from iceberg-pipeline-glue.yaml are present in the consolidated template."""
    resources = consolidated_template["Resources"]

    expected_glue_resources = [
        "IcebergDataBucket",
        "IcebergErrorsBucket",
        "GlueJobRole",
        "IcebergGlueJob",
    ]

    for resource_name in expected_glue_resources:
        assert resource_name in resources, (
            f"Resource '{resource_name}' from iceberg-pipeline-glue.yaml "
            f"is missing in the consolidated template"
        )
