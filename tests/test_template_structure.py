"""Tests for CloudFormation template structure and parameter definitions.

Validates: Requirements 1.1, 1.2, 1.4, 2.1–2.7
"""


class TestTemplateStructure:
    """Verify the template has the required top-level keys and format version."""

    def test_format_version(self, template):
        """AWSTemplateFormatVersion must be '2010-09-09'."""
        assert template["AWSTemplateFormatVersion"] == "2010-09-09"

    def test_description_present_and_non_empty(self, template):
        """Description must be present and non-empty."""
        assert "Description" in template
        assert isinstance(template["Description"], str)
        assert len(template["Description"].strip()) > 0

    def test_parameters_section_exists(self, template):
        """Parameters section must exist."""
        assert "Parameters" in template

    def test_resources_section_exists(self, template):
        """Resources section must exist."""
        assert "Resources" in template


# Expected parameter definitions: name -> (has_default, default_value, type)
EXPECTED_PARAMS = {
    "AccountId": {
        "type": "String",
        "has_default": False,
        "default": None,
        "description_contains": "account ID",
    },
    "Region": {
        "type": "String",
        "has_default": True,
        "default": "us-east-1",
        "description_contains": "region",
    },
    "DatabaseName": {
        "type": "String",
        "has_default": True,
        "default": "iceberg_mv_test2",
        "description_contains": "database",
    },
    "IcebergDataBucketName": {
        "type": "String",
        "has_default": True,
        "default": "my-company-iceberg-data-bucket",
        "description_contains": "bucket",
    },
    "IcebergErrorsBucketName": {
        "type": "String",
        "has_default": True,
        "default": "my-company-iceberg-errors-bucket",
        "description_contains": "bucket",
    },
    "GlueJobRoleName": {
        "type": "String",
        "has_default": True,
        "default": "my-iceberg-glue-job-role",
        "description_contains": "role",
    },
}


class TestParameterDefinitions:
    """Verify all six parameters are defined with correct types, defaults, and descriptions."""

    def test_all_six_parameters_defined(self, template):
        """All six expected parameters must be present."""
        params = template["Parameters"]
        for name in EXPECTED_PARAMS:
            assert name in params, f"Parameter '{name}' is missing"

    def test_parameter_types(self, template):
        """Each parameter must have Type: String."""
        params = template["Parameters"]
        for name, expected in EXPECTED_PARAMS.items():
            assert params[name]["Type"] == expected["type"], (
                f"Parameter '{name}' should have Type '{expected['type']}'"
            )

    def test_parameter_defaults(self, template):
        """Parameters with defaults must have the correct default value."""
        params = template["Parameters"]
        for name, expected in EXPECTED_PARAMS.items():
            if expected["has_default"]:
                assert "Default" in params[name], (
                    f"Parameter '{name}' should have a Default value"
                )
                assert params[name]["Default"] == expected["default"], (
                    f"Parameter '{name}' default should be '{expected['default']}'"
                )

    def test_account_id_has_no_default(self, template):
        """AccountId must not have a default value (it is required)."""
        account_param = template["Parameters"]["AccountId"]
        assert "Default" not in account_param, (
            "AccountId should not have a Default value"
        )

    def test_parameter_descriptions(self, template):
        """Each parameter must have a non-empty Description."""
        params = template["Parameters"]
        for name, expected in EXPECTED_PARAMS.items():
            assert "Description" in params[name], (
                f"Parameter '{name}' should have a Description"
            )
            desc = params[name]["Description"]
            assert isinstance(desc, str) and len(desc.strip()) > 0, (
                f"Parameter '{name}' Description should be non-empty"
            )
