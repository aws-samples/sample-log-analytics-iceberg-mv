"""Tests for the IcebergGlueJob resource and its parameters in the CloudFormation template.

Validates: Requirements 1.1–1.4, 2.1–2.3, 3.1–3.11, 4.1, 4.2, 5.1, 6.1–6.4
"""

import pytest
from ruamel.yaml.comments import TaggedScalar


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_glue_job(template):
    """Return the IcebergGlueJob resource dict."""
    return template["Resources"]["IcebergGlueJob"]


def _get_glue_job_props(template):
    """Return the Properties dict for the IcebergGlueJob resource."""
    return _get_glue_job(template)["Properties"]


def _get_conf_value(template):
    """Return the raw --conf TaggedScalar from DefaultArguments."""
    return _get_glue_job_props(template)["DefaultArguments"]["--conf"]


# ---------------------------------------------------------------------------
# Parameter definitions for the four new Glue job parameters
# ---------------------------------------------------------------------------

NEW_PARAM_CONFIGS = {
    "GlueJobName": {
        "type": "String",
        "has_default": True,
        "default": "my-iceberg-glue-job",
    },
    "WorkerType": {
        "type": "String",
        "has_default": True,
        "default": "G.1X",
    },
    "NumberOfWorkers": {
        "type": "Number",
        "has_default": True,
        "default": 2,
    },
}


# ---------------------------------------------------------------------------
# 1. Glue Job Resource Existence and Type  (Req 1.1)
# ---------------------------------------------------------------------------

class TestGlueJobResource:
    """Verify the IcebergGlueJob resource exists with the correct type."""

    def test_glue_job_resource_exists(self, template):
        """IcebergGlueJob must be defined in the Resources section."""
        assert "IcebergGlueJob" in template["Resources"], (
            "Resource 'IcebergGlueJob' is missing from the template"
        )

    def test_glue_job_type(self, template):
        """IcebergGlueJob must have Type AWS::Glue::Job."""
        job = _get_glue_job(template)
        assert job["Type"] == "AWS::Glue::Job", (
            f"IcebergGlueJob Type should be 'AWS::Glue::Job', got '{job['Type']}'"
        )


# ---------------------------------------------------------------------------
# 2. Glue Job Parameter Tests  (Reqs 2.1, 2.2, 2.3)
# ---------------------------------------------------------------------------

class TestGlueJobParameters:
    """Verify the four new parameters are defined with correct types, defaults, and descriptions."""

    @pytest.mark.parametrize("param_name", NEW_PARAM_CONFIGS.keys())
    def test_parameter_exists(self, template, param_name):
        """Each new parameter must be present in the Parameters section."""
        assert param_name in template["Parameters"], (
            f"Parameter '{param_name}' is missing from the template"
        )

    @pytest.mark.parametrize("param_name", NEW_PARAM_CONFIGS.keys())
    def test_parameter_type(self, template, param_name):
        """Each new parameter must have the correct Type."""
        expected = NEW_PARAM_CONFIGS[param_name]
        param = template["Parameters"][param_name]
        assert param["Type"] == expected["type"], (
            f"Parameter '{param_name}' should have Type '{expected['type']}', "
            f"got '{param['Type']}'"
        )

    @pytest.mark.parametrize("param_name", [
        name for name, cfg in NEW_PARAM_CONFIGS.items() if cfg["has_default"]
    ])
    def test_parameter_default(self, template, param_name):
        """Parameters with defaults must have the correct default value."""
        expected = NEW_PARAM_CONFIGS[param_name]
        param = template["Parameters"][param_name]
        assert "Default" in param, (
            f"Parameter '{param_name}' should have a Default value"
        )
        assert param["Default"] == expected["default"], (
            f"Parameter '{param_name}' default should be '{expected['default']}', "
            f"got '{param['Default']}'"
        )

    @pytest.mark.parametrize("param_name", NEW_PARAM_CONFIGS.keys())
    def test_parameter_has_description(self, template, param_name):
        """Each new parameter must have a non-empty Description."""
        param = template["Parameters"][param_name]
        assert "Description" in param, (
            f"Parameter '{param_name}' should have a Description"
        )
        desc = param["Description"]
        assert isinstance(desc, str) and len(desc.strip()) > 0, (
            f"Parameter '{param_name}' Description should be non-empty"
        )


# ---------------------------------------------------------------------------
# 3. Glue Job Core Properties  (Reqs 1.2, 1.3, 1.4, 2.1, 2.2, 2.3)
# ---------------------------------------------------------------------------

class TestGlueJobCoreProperties:
    """Verify the Glue job's top-level properties are correctly configured."""

    def test_glue_version(self, template):
        """GlueVersion must be '5.1'."""
        props = _get_glue_job_props(template)
        assert props["GlueVersion"] == "5.1", (
            f"GlueVersion should be '5.1', got '{props['GlueVersion']}'"
        )

    def test_role_uses_getatt(self, template):
        """Role must use !GetAtt GlueJobRole.Arn."""
        role = _get_glue_job_props(template)["Role"]
        assert isinstance(role, TaggedScalar), (
            "Role should be a TaggedScalar (!GetAtt)"
        )
        assert role.tag.value == "!GetAtt", (
            f"Role tag should be '!GetAtt', got '{role.tag.value}'"
        )
        assert role.value == "GlueJobRole.Arn", (
            f"Role should reference 'GlueJobRole.Arn', got '{role.value}'"
        )

    def test_name_uses_ref(self, template):
        """Name must use !Ref GlueJobName."""
        name = _get_glue_job_props(template)["Name"]
        assert isinstance(name, TaggedScalar), (
            "Name should be a TaggedScalar (!Ref)"
        )
        assert name.tag.value == "!Ref", (
            f"Name tag should be '!Ref', got '{name.tag.value}'"
        )
        assert name.value == "GlueJobName", (
            f"Name should reference 'GlueJobName', got '{name.value}'"
        )

    def test_command_name(self, template):
        """Command.Name must be 'glueetl'."""
        command = _get_glue_job_props(template)["Command"]
        assert command["Name"] == "glueetl", (
            f"Command.Name should be 'glueetl', got '{command['Name']}'"
        )

    def test_command_script_location_uses_sub(self, template):
        """Command.ScriptLocation must use !Sub with the GlueScriptBucketName parameter."""
        script_loc = _get_glue_job_props(template)["Command"]["ScriptLocation"]
        assert isinstance(script_loc, TaggedScalar), (
            "Command.ScriptLocation should be a TaggedScalar (!Sub)"
        )
        assert script_loc.tag.value == "!Sub", (
            f"Command.ScriptLocation tag should be '!Sub', got '{script_loc.tag.value}'"
        )
        expected = "s3://${GlueScriptBucketName}/scripts/sample-glue-job-iceberg-materializedview-builder.py"
        assert script_loc.value == expected, (
            f"Command.ScriptLocation should be '{expected}', got '{script_loc.value}'"
        )

    def test_worker_type_uses_ref(self, template):
        """WorkerType must use !Ref WorkerType."""
        worker_type = _get_glue_job_props(template)["WorkerType"]
        assert isinstance(worker_type, TaggedScalar), (
            "WorkerType should be a TaggedScalar (!Ref)"
        )
        assert worker_type.tag.value == "!Ref", (
            f"WorkerType tag should be '!Ref', got '{worker_type.tag.value}'"
        )
        assert worker_type.value == "WorkerType", (
            f"WorkerType should reference 'WorkerType', got '{worker_type.value}'"
        )

    def test_number_of_workers_uses_ref(self, template):
        """NumberOfWorkers must use !Ref NumberOfWorkers."""
        num_workers = _get_glue_job_props(template)["NumberOfWorkers"]
        assert isinstance(num_workers, TaggedScalar), (
            "NumberOfWorkers should be a TaggedScalar (!Ref)"
        )
        assert num_workers.tag.value == "!Ref", (
            f"NumberOfWorkers tag should be '!Ref', got '{num_workers.tag.value}'"
        )
        assert num_workers.value == "NumberOfWorkers", (
            f"NumberOfWorkers should reference 'NumberOfWorkers', got '{num_workers.value}'"
        )


# ---------------------------------------------------------------------------
# 4. Spark Configuration Tests  (Reqs 3.1–3.11, 4.1, 4.2)
# ---------------------------------------------------------------------------

REQUIRED_SPARK_PROPERTIES = [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.type=glue",
    "spark.sql.catalog.glue_catalog.warehouse=s3://${IcebergDataBucketName}/${WarehouseDatabaseName}.db",
    "spark.sql.catalog.glue_catalog.glue.region=${Region}",
    "spark.sql.catalog.glue_catalog.glue.id=${AccountId}",
    "spark.sql.catalog.glue_catalog.glue.account-id=${AccountId}",
    "spark.sql.catalog.glue_catalog.client.region=${Region}",
    "spark.sql.catalog.glue_catalog.glue.lakeformation-enabled=true",
    "spark.sql.defaultCatalog=glue_catalog",
    "spark.sql.optimizer.answerQueriesWithMVs.enabled=true",
    "spark.sql.materializedViews.metadataCache.enabled=true",
]


class TestSparkConfiguration:
    """Verify the --conf default argument contains all required Spark properties."""

    def test_conf_key_exists(self, template):
        """DefaultArguments must contain a --conf key."""
        default_args = _get_glue_job_props(template)["DefaultArguments"]
        assert "--conf" in default_args, (
            "DefaultArguments must contain a '--conf' key"
        )

    def test_conf_uses_sub_tag(self, template):
        """The --conf value must use the !Sub intrinsic function."""
        conf = _get_conf_value(template)
        assert isinstance(conf, TaggedScalar), (
            "--conf should be a TaggedScalar (!Sub)"
        )
        assert conf.tag.value == "!Sub", (
            f"--conf tag should be '!Sub', got '{conf.tag.value}'"
        )

    @pytest.mark.parametrize("spark_property", REQUIRED_SPARK_PROPERTIES, ids=[
        p.split("=")[0] for p in REQUIRED_SPARK_PROPERTIES
    ])
    def test_conf_contains_spark_property(self, template, spark_property):
        """The --conf string must contain each required Spark configuration property."""
        conf_value = _get_conf_value(template).value
        assert spark_property in conf_value, (
            f"--conf string is missing Spark property: '{spark_property}'"
        )

    def test_conf_contains_all_twelve_properties(self, template):
        """The --conf string must contain all 12 required Spark configuration properties."""
        conf_value = _get_conf_value(template).value
        missing = [p for p in REQUIRED_SPARK_PROPERTIES if p not in conf_value]
        assert not missing, (
            f"--conf string is missing {len(missing)} Spark properties: {missing}"
        )


# ---------------------------------------------------------------------------
# 5. Datalake Formats Test  (Req 5.1)
# ---------------------------------------------------------------------------

class TestDatalakeFormats:
    """Verify the --datalake-formats default argument is set to iceberg."""

    def test_datalake_formats_key_exists(self, template):
        """DefaultArguments must contain a --datalake-formats key."""
        default_args = _get_glue_job_props(template)["DefaultArguments"]
        assert "--datalake-formats" in default_args, (
            "DefaultArguments must contain a '--datalake-formats' key"
        )

    def test_datalake_formats_value_is_iceberg(self, template):
        """--datalake-formats must be set to 'iceberg'."""
        default_args = _get_glue_job_props(template)["DefaultArguments"]
        assert default_args["--datalake-formats"] == "iceberg", (
            f"--datalake-formats should be 'iceberg', got '{default_args['--datalake-formats']}'"
        )


# ---------------------------------------------------------------------------
# 6. Parameter Substitution Tests  (Reqs 6.1, 6.2, 6.3, 6.4)
# ---------------------------------------------------------------------------

class TestParameterSubstitution:
    """Verify the --conf !Sub string references the expected parameters."""

    def test_conf_contains_iceberg_data_bucket_name(self, template):
        """The --conf !Sub string must contain ${IcebergDataBucketName}."""
        conf_value = _get_conf_value(template).value
        assert "${IcebergDataBucketName}" in conf_value, (
            "--conf !Sub string must contain '${IcebergDataBucketName}'"
        )

    def test_conf_contains_account_id(self, template):
        """The --conf !Sub string must contain ${AccountId}."""
        conf_value = _get_conf_value(template).value
        assert "${AccountId}" in conf_value, (
            "--conf !Sub string must contain '${AccountId}'"
        )

    def test_conf_contains_region(self, template):
        """The --conf !Sub string must contain ${Region}."""
        conf_value = _get_conf_value(template).value
        assert "${Region}" in conf_value, (
            "--conf !Sub string must contain '${Region}'"
        )
