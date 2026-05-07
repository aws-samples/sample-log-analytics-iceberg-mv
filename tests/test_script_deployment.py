"""Tests for ScriptLocation removal, local script path, job argument, and IAM policy.

Validates: Requirements 2.1, 2.3, 3.2, 3.4, 4.1, 4.2
"""

import pytest
from ruamel.yaml.comments import TaggedScalar


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_glue_job_props(template):
    """Return the Properties dict for the IcebergGlueJob resource."""
    return template["Resources"]["IcebergGlueJob"]["Properties"]


def _get_policies(template):
    """Return a dict mapping PolicyName → PolicyDocument for GlueJobRole inline policies."""
    policies = template["Resources"]["GlueJobRole"]["Properties"]["Policies"]
    return {p["PolicyName"]: p["PolicyDocument"] for p in policies}


def _get_policy_statements(template, policy_name):
    """Return the list of statements for a given inline policy."""
    doc = _get_policies(template)[policy_name]
    stmts = doc["Statement"]
    return stmts if isinstance(stmts, list) else [stmts]


# ---------------------------------------------------------------------------
# 1. ScriptLocation parameter removal  (Req 2.3)
# ---------------------------------------------------------------------------

class TestScriptLocationParameterRemoved:
    """Verify the ScriptLocation parameter has been removed from the template."""

    def test_script_location_not_in_parameters(self, template):
        """ScriptLocation must NOT be in the Parameters section."""
        assert "ScriptLocation" not in template["Parameters"], (
            "Parameter 'ScriptLocation' should have been removed from the template"
        )


# ---------------------------------------------------------------------------
# 2. Command.ScriptLocation is a local path  (Req 2.1)
# ---------------------------------------------------------------------------

class TestScriptLocationLocalPath:
    """Verify Command.ScriptLocation uses !Sub with the S3 URI."""

    EXPECTED_PATH = "s3://${GlueScriptBucketName}/scripts/sample-glue-job-iceberg-materializedview-builder.py"

    def test_script_location_uses_sub(self, template):
        """Command.ScriptLocation must be a TaggedScalar with !Sub tag."""
        script_loc = _get_glue_job_props(template)["Command"]["ScriptLocation"]
        assert isinstance(script_loc, TaggedScalar), (
            "Command.ScriptLocation should be a TaggedScalar (!Sub)"
        )
        assert script_loc.tag.value == "!Sub", (
            f"Command.ScriptLocation tag should be '!Sub', got '{script_loc.tag.value}'"
        )

    def test_script_location_value(self, template):
        """Command.ScriptLocation must equal the expected S3 URI pattern."""
        script_loc = _get_glue_job_props(template)["Command"]["ScriptLocation"]
        assert script_loc.value == self.EXPECTED_PATH, (
            f"Command.ScriptLocation should be '{self.EXPECTED_PATH}', "
            f"got '{script_loc.value}'"
        )


# ---------------------------------------------------------------------------
# 3. --iceberg-data-bucket job argument  (Reqs 3.2, 3.4)
# ---------------------------------------------------------------------------

class TestIcebergDataBucketArgument:
    """Verify DefaultArguments contains --iceberg-data-bucket with !Ref IcebergDataBucketName."""

    def test_iceberg_data_bucket_key_exists(self, template):
        """DefaultArguments must contain a --iceberg-data-bucket key."""
        default_args = _get_glue_job_props(template)["DefaultArguments"]
        assert "--iceberg-data-bucket" in default_args, (
            "DefaultArguments must contain '--iceberg-data-bucket'"
        )

    def test_iceberg_data_bucket_uses_ref(self, template):
        """--iceberg-data-bucket must use !Ref IcebergDataBucketName."""
        value = _get_glue_job_props(template)["DefaultArguments"]["--iceberg-data-bucket"]
        assert isinstance(value, TaggedScalar), (
            "--iceberg-data-bucket should be a TaggedScalar (!Ref)"
        )
        assert value.tag.value == "!Ref", (
            f"--iceberg-data-bucket tag should be '!Ref', got '{value.tag.value}'"
        )
        assert value.value == "IcebergDataBucketName", (
            f"--iceberg-data-bucket should reference 'IcebergDataBucketName', "
            f"got '{value.value}'"
        )


# ---------------------------------------------------------------------------
# 4. glue-script-bucket-read IAM policy  (Reqs 4.1, 4.2)
# ---------------------------------------------------------------------------

class TestGlueScriptBucketReadPolicy:
    """Verify GlueJobRole has the glue-script-bucket-read inline policy."""

    POLICY_NAME = "glue-script-bucket-read"

    def test_policy_exists(self, template):
        """GlueJobRole must have an inline policy named glue-script-bucket-read."""
        assert self.POLICY_NAME in _get_policies(template), (
            f"Inline policy '{self.POLICY_NAME}' is missing from GlueJobRole"
        )

    def test_policy_grants_s3_get_object(self, template):
        """The policy must grant s3:GetObject."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        actions = []
        for stmt in stmts:
            a = stmt["Action"]
            if isinstance(a, list):
                actions.extend([str(x) for x in a])
            else:
                actions.append(str(a))
        assert "s3:GetObject" in actions, (
            f"Policy '{self.POLICY_NAME}' must grant 's3:GetObject', "
            f"found actions: {actions}"
        )

    def test_policy_resource_uses_sub(self, template):
        """The resource ARN must use !Sub."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        resources = []
        for stmt in stmts:
            r = stmt["Resource"]
            if isinstance(r, list):
                resources.extend(r)
            else:
                resources.append(r)
        for res in resources:
            assert isinstance(res, TaggedScalar) and res.tag.value == "!Sub", (
                f"Resource ARN must use !Sub, got {res!r}"
            )

    def test_policy_resource_arn_pattern(self, template):
        """The resource ARN must be arn:aws:s3:::${GlueScriptBucketName}/*."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        resources = []
        for stmt in stmts:
            r = stmt["Resource"]
            if isinstance(r, list):
                resources.extend(r)
            else:
                resources.append(r)
        arn_values = [r.value for r in resources]
        expected = "arn:aws:s3:::${GlueScriptBucketName}/*"
        assert any(v == expected for v in arn_values), (
            f"Resource ARN must be '{expected}', got {arn_values}"
        )
