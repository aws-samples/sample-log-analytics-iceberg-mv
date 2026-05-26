"""Bug condition exploration tests for the single-CFN-stack consolidation bugfix.

These tests confirm the fix is in place by asserting the structural gaps are filled:
1. Consolidated template exists at cloudformation/iceberg-pipeline.yaml
2. IcebergFirehoseStream DependsOn includes GlueJobTriggerCustomResource
3. AWS::CloudFormation::CustomResource exists in the consolidated template
4. A Lambda function with Glue trigger logic (glue.start_job_run) exists

EXPECTED OUTCOME on FIXED code: All tests PASS (confirming the fix is applied).

Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5
"""

import glob
import os

import pytest
from ruamel.yaml import YAML

CLOUDFORMATION_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cloudformation"
)


def load_template(path):
    """Load and parse a CloudFormation YAML template."""
    yaml = YAML()
    with open(path, "r") as f:
        return yaml.load(f)


def get_all_template_paths():
    """Return all YAML template file paths in the cloudformation directory."""
    return glob.glob(os.path.join(CLOUDFORMATION_DIR, "*.yaml"))


class TestBugConditionExploration:
    """Tests that confirm the structural gaps have been filled by the fix."""

    def test_consolidated_template_exists(self):
        """Assert that cloudformation/iceberg-pipeline.yaml DOES exist.

        This confirms the consolidated template is present, enabling single-stack deployment.
        Validates: Requirements 1.1, 1.5
        """
        consolidated_path = os.path.join(CLOUDFORMATION_DIR, "iceberg-pipeline.yaml")
        assert os.path.exists(consolidated_path), (
            f"Expected consolidated template to exist at {consolidated_path}, "
            "but it was not found. The fix requires the template to be present."
        )

    def test_firehose_stream_depends_on_glue_trigger(self):
        """Assert that IcebergFirehoseStream.DependsOn DOES contain GlueJobTriggerCustomResource.

        This confirms the dependency chain ensures the Glue Catalog table exists
        before Firehose resource creation.
        Validates: Requirements 1.2, 1.3
        """
        consolidated_path = os.path.join(CLOUDFORMATION_DIR, "iceberg-pipeline.yaml")
        template = load_template(consolidated_path)
        resources = template.get("Resources", {})

        firehose_stream = resources.get("IcebergFirehoseStream", {})
        depends_on = firehose_stream.get("DependsOn", [])

        # Normalize to list if it's a string
        if isinstance(depends_on, str):
            depends_on = [depends_on]

        assert "GlueJobTriggerCustomResource" in depends_on, (
            f"IcebergFirehoseStream.DependsOn is {depends_on} but should contain "
            "'GlueJobTriggerCustomResource' to ensure the Glue Catalog table exists "
            "before Firehose creation."
        )

    def test_custom_resource_exists_in_consolidated_template(self):
        """Assert that the consolidated template DOES contain AWS::CloudFormation::CustomResource.

        This confirms the orchestration layer that automatically triggers the Glue job
        and waits for completion is present.
        Validates: Requirements 1.2, 1.4
        """
        consolidated_path = os.path.join(CLOUDFORMATION_DIR, "iceberg-pipeline.yaml")
        template = load_template(consolidated_path)
        resources = template.get("Resources", {})

        custom_resource_found = False
        for resource_name, resource_def in resources.items():
            resource_type = resource_def.get("Type", "")
            if resource_type == "AWS::CloudFormation::CustomResource":
                custom_resource_found = True
                break

        assert custom_resource_found, (
            "No AWS::CloudFormation::CustomResource found in the consolidated template. "
            "The fix requires a Custom Resource to orchestrate the Glue job trigger."
        )

    def test_lambda_with_glue_trigger_logic_exists(self):
        """Assert that a Lambda function with inline code referencing start_job_run DOES exist.

        This confirms the automated Glue job trigger mechanism is present in the
        consolidated template.
        Validates: Requirements 1.4, 1.5
        """
        consolidated_path = os.path.join(CLOUDFORMATION_DIR, "iceberg-pipeline.yaml")
        template = load_template(consolidated_path)
        resources = template.get("Resources", {})

        trigger_lambda_found = False
        for resource_name, resource_def in resources.items():
            resource_type = resource_def.get("Type", "")
            if resource_type != "AWS::Lambda::Function":
                continue

            # Check inline code (ZipFile) for start_job_run
            properties = resource_def.get("Properties", {})
            code = properties.get("Code", {})

            if isinstance(code, dict):
                zip_file = code.get("ZipFile", "")
                if isinstance(zip_file, str) and "start_job_run" in zip_file:
                    trigger_lambda_found = True
                    break

        assert trigger_lambda_found, (
            "No Lambda function with 'start_job_run' in its inline code found in the "
            "consolidated template. The fix requires a Lambda that triggers the Glue job."
        )
