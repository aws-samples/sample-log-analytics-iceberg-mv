"""Tests for the GlueScriptBucketName parameter.

The GlueScriptBucket S3 resource is managed outside the stack (created by
the deploy script before stack deployment), so only the parameter is validated.

Validates: Requirements 1.3
"""


class TestGlueScriptBucketNameParameter:
    """Verify the GlueScriptBucketName parameter exists with correct attributes."""

    def test_parameter_exists(self, template):
        """GlueScriptBucketName must be defined in the Parameters section."""
        assert "GlueScriptBucketName" in template["Parameters"], (
            "Parameter 'GlueScriptBucketName' is missing from the template"
        )

    def test_parameter_type_is_string(self, template):
        """GlueScriptBucketName must have Type String."""
        param = template["Parameters"]["GlueScriptBucketName"]
        assert param["Type"] == "String", (
            f"GlueScriptBucketName Type should be 'String', got '{param['Type']}'"
        )

    def test_parameter_has_default_value(self, template):
        """GlueScriptBucketName must have a Default value."""
        param = template["Parameters"]["GlueScriptBucketName"]
        assert "Default" in param, (
            "GlueScriptBucketName must have a Default value"
        )
        assert param["Default"], (
            "GlueScriptBucketName Default value must not be empty"
        )

    def test_parameter_has_description(self, template):
        """GlueScriptBucketName must have a Description."""
        param = template["Parameters"]["GlueScriptBucketName"]
        assert "Description" in param, (
            "GlueScriptBucketName must have a Description"
        )
        assert param["Description"], (
            "GlueScriptBucketName Description must not be empty"
        )
