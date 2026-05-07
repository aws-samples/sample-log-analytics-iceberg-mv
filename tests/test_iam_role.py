"""Tests for the IAM role resource and inline policies in the CloudFormation template.

Validates: Requirements 5.1, 5.2, 5.3, 6.1–6.3, 7.1–7.3, 8.1–8.2, 9.1–9.3
"""

import pytest
from ruamel.yaml.comments import TaggedScalar


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_role(template):
    """Return the GlueJobRole resource dict."""
    return template["Resources"]["GlueJobRole"]


def _get_policies(template):
    """Return a dict mapping PolicyName → PolicyDocument for inline policies."""
    policies = _get_role(template)["Properties"]["Policies"]
    return {p["PolicyName"]: p["PolicyDocument"] for p in policies}


def _get_policy_statements(template, policy_name):
    """Return the list of statements for a given inline policy."""
    doc = _get_policies(template)[policy_name]
    stmts = doc["Statement"]
    return stmts if isinstance(stmts, list) else [stmts]


# ---------------------------------------------------------------------------
# 1. IAM Role existence and type  (Req 5.1)
# ---------------------------------------------------------------------------

class TestGlueJobRoleResource:
    """Verify the GlueJobRole resource exists with the correct type."""

    def test_role_resource_exists(self, template):
        """GlueJobRole must be defined in the Resources section."""
        assert "GlueJobRole" in template["Resources"], (
            "Resource 'GlueJobRole' is missing from the template"
        )

    def test_role_type(self, template):
        """GlueJobRole must have Type AWS::IAM::Role."""
        role = _get_role(template)
        assert role["Type"] == "AWS::IAM::Role", (
            f"GlueJobRole Type should be 'AWS::IAM::Role', got '{role['Type']}'"
        )


# ---------------------------------------------------------------------------
# 2. RoleName references GlueJobRoleName parameter  (Req 5.1)
# ---------------------------------------------------------------------------

class TestRoleNameReference:
    """Verify RoleName uses !Ref GlueJobRoleName."""

    def test_role_name_is_tagged_ref(self, template):
        """RoleName must use a !Ref intrinsic function."""
        role_name = _get_role(template)["Properties"]["RoleName"]
        assert isinstance(role_name, TaggedScalar), (
            "RoleName should be a TaggedScalar (!Ref)"
        )
        assert role_name.tag.value == "!Ref", (
            f"RoleName tag should be '!Ref', got '{role_name.tag.value}'"
        )

    def test_role_name_references_correct_parameter(self, template):
        """RoleName !Ref must point to GlueJobRoleName."""
        role_name = _get_role(template)["Properties"]["RoleName"]
        assert role_name.value == "GlueJobRoleName", (
            f"RoleName should reference 'GlueJobRoleName', got '{role_name.value}'"
        )


# ---------------------------------------------------------------------------
# 3. AssumeRolePolicyDocument  (Req 5.2)
# ---------------------------------------------------------------------------

class TestAssumeRolePolicy:
    """Verify the trust policy allows glue.amazonaws.com to assume the role."""

    def test_assume_role_policy_exists(self, template):
        """AssumeRolePolicyDocument must be present."""
        props = _get_role(template)["Properties"]
        assert "AssumeRolePolicyDocument" in props, (
            "GlueJobRole must have an AssumeRolePolicyDocument"
        )

    def test_assume_role_principal_is_glue(self, template):
        """The trust policy principal must be glue.amazonaws.com."""
        doc = _get_role(template)["Properties"]["AssumeRolePolicyDocument"]
        statements = doc["Statement"]
        principals = []
        for stmt in statements:
            svc = stmt.get("Principal", {}).get("Service")
            if isinstance(svc, list):
                principals.extend(svc)
            elif svc:
                principals.append(svc)
        assert "glue.amazonaws.com" in principals, (
            f"Trust policy must allow glue.amazonaws.com, found principals: {principals}"
        )

    def test_assume_role_action_is_sts(self, template):
        """The trust policy action must be sts:AssumeRole."""
        doc = _get_role(template)["Properties"]["AssumeRolePolicyDocument"]
        statements = doc["Statement"]
        actions = []
        for stmt in statements:
            action = stmt.get("Action")
            if isinstance(action, list):
                actions.extend(action)
            elif action:
                actions.append(str(action))
        assert "sts:AssumeRole" in actions, (
            f"Trust policy must include 'sts:AssumeRole', found: {actions}"
        )


# ---------------------------------------------------------------------------
# 4. ManagedPolicyArns  (Req 5.3)
# ---------------------------------------------------------------------------

class TestManagedPolicyArns:
    """Verify the AWSGlueServiceRole managed policy is attached."""

    EXPECTED_ARN = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"

    def test_managed_policy_arns_exists(self, template):
        """ManagedPolicyArns must be present on the role."""
        props = _get_role(template)["Properties"]
        assert "ManagedPolicyArns" in props, (
            "GlueJobRole must have ManagedPolicyArns"
        )

    def test_glue_service_role_policy_attached(self, template):
        """ManagedPolicyArns must include the AWSGlueServiceRole ARN."""
        arns = _get_role(template)["Properties"]["ManagedPolicyArns"]
        assert self.EXPECTED_ARN in arns, (
            f"ManagedPolicyArns must include '{self.EXPECTED_ARN}', got {arns}"
        )


# ---------------------------------------------------------------------------
# 5. Inline policy: glue-access-formv  (Reqs 6.1, 6.2, 6.3)
# ---------------------------------------------------------------------------

class TestGlueAccessPolicy:
    """Verify the glue-access-formv inline policy."""

    POLICY_NAME = "glue-access-formv"
    EXPECTED_ACTIONS = sorted([
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:GetTable",
        "glue:GetTables",
    ])
    EXPECTED_RESOURCE_SUFFIXES = [
        ":catalog",
        ":database/${DatabaseName}",
        ":table/${DatabaseName}/*",
    ]

    def test_policy_exists(self, template):
        """The glue-access-formv policy must be present."""
        assert self.POLICY_NAME in _get_policies(template), (
            f"Inline policy '{self.POLICY_NAME}' is missing"
        )

    def test_policy_actions(self, template):
        """The policy must grant exactly the four required Glue actions."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        actions = []
        for stmt in stmts:
            a = stmt["Action"]
            if isinstance(a, list):
                actions.extend([str(x) for x in a])
            else:
                actions.append(str(a))
        assert sorted(actions) == self.EXPECTED_ACTIONS, (
            f"Expected actions {self.EXPECTED_ACTIONS}, got {sorted(actions)}"
        )

    def test_policy_has_three_sub_resources(self, template):
        """The policy must have exactly 3 resource ARNs using !Sub."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        resources = []
        for stmt in stmts:
            r = stmt["Resource"]
            if isinstance(r, list):
                resources.extend(r)
            else:
                resources.append(r)
        assert len(resources) == 3, (
            f"Expected 3 resource ARNs, got {len(resources)}"
        )
        for res in resources:
            assert isinstance(res, TaggedScalar) and res.tag.value == "!Sub", (
                f"Each resource ARN must use !Sub, got {res!r}"
            )

    def test_policy_resource_arn_patterns(self, template):
        """Each resource ARN must match the expected suffix pattern."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        resources = []
        for stmt in stmts:
            r = stmt["Resource"]
            if isinstance(r, list):
                resources.extend(r)
            else:
                resources.append(r)
        resource_values = [r.value for r in resources]
        for suffix in self.EXPECTED_RESOURCE_SUFFIXES:
            assert any(rv.endswith(suffix) for rv in resource_values), (
                f"No resource ARN ends with '{suffix}'. Found: {resource_values}"
            )


# ---------------------------------------------------------------------------
# 6. Inline policy: iam-pass-role  (Reqs 7.1, 7.2, 7.3)
# ---------------------------------------------------------------------------

class TestIamPassRolePolicy:
    """Verify the iam-pass-role inline policy."""

    POLICY_NAME = "iam-pass-role"

    def test_policy_exists(self, template):
        """The iam-pass-role policy must be present."""
        assert self.POLICY_NAME in _get_policies(template), (
            f"Inline policy '{self.POLICY_NAME}' is missing"
        )

    def test_policy_action(self, template):
        """The policy must grant iam:PassRole."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        actions = []
        for stmt in stmts:
            a = stmt["Action"]
            if isinstance(a, list):
                actions.extend([str(x) for x in a])
            else:
                actions.append(str(a))
        assert "iam:PassRole" in actions, (
            f"Expected 'iam:PassRole' in actions, got {actions}"
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
                f"Resource must use !Sub, got {res!r}"
            )

    def test_policy_resource_arn_pattern(self, template):
        """The resource ARN must reference AccountId and GlueJobRoleName."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        resources = []
        for stmt in stmts:
            r = stmt["Resource"]
            if isinstance(r, list):
                resources.extend(r)
            else:
                resources.append(r)
        arn_value = resources[0].value
        assert "${AccountId}" in arn_value, (
            f"Resource ARN must contain '${{AccountId}}', got '{arn_value}'"
        )
        assert "${GlueJobRoleName}" in arn_value, (
            f"Resource ARN must contain '${{GlueJobRoleName}}', got '{arn_value}'"
        )


# ---------------------------------------------------------------------------
# 7. Inline policy: lake-formation-access  (Reqs 8.1, 8.2)
# ---------------------------------------------------------------------------

class TestLakeFormationPolicy:
    """Verify the lake-formation-access inline policy."""

    POLICY_NAME = "lake-formation-access"
    EXPECTED_ACTIONS = sorted([
        "lakeformation:GetDataAccess",
        "lakeformation:GetTemporaryGluePartitionCredentials",
        "lakeformation:GetTemporaryGlueTableCredentials",
    ])

    def test_policy_exists(self, template):
        """The lake-formation-access policy must be present."""
        assert self.POLICY_NAME in _get_policies(template), (
            f"Inline policy '{self.POLICY_NAME}' is missing"
        )

    def test_policy_actions(self, template):
        """The policy must grant the three Lake Formation actions."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        actions = []
        for stmt in stmts:
            a = stmt["Action"]
            if isinstance(a, list):
                actions.extend([str(x) for x in a])
            else:
                actions.append(str(a))
        assert sorted(actions) == self.EXPECTED_ACTIONS, (
            f"Expected actions {self.EXPECTED_ACTIONS}, got {sorted(actions)}"
        )

    def test_policy_resource_is_wildcard(self, template):
        """The policy resource must be '*'."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        for stmt in stmts:
            resource = stmt["Resource"]
            assert str(resource) == "*", (
                f"Resource must be '*', got '{resource}'"
            )


# ---------------------------------------------------------------------------
# 8. Inline policy: iceberg-s3-emr-access  (Reqs 9.1, 9.2, 9.3)
# ---------------------------------------------------------------------------

class TestIcebergS3EmrPolicy:
    """Verify the iceberg-s3-emr-access inline policy."""

    POLICY_NAME = "iceberg-s3-emr-access"

    def test_policy_exists(self, template):
        """The iceberg-s3-emr-access policy must be present."""
        assert self.POLICY_NAME in _get_policies(template), (
            f"Inline policy '{self.POLICY_NAME}' is missing"
        )

    def test_policy_has_two_statements(self, template):
        """The policy must contain exactly two statements (S3 + EMR)."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        assert len(stmts) == 2, (
            f"Expected 2 statements, got {len(stmts)}"
        )

    # -- S3 statement --

    def test_s3_statement_actions(self, template):
        """The S3 statement must grant s3:GetObject and s3:PutObject."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        s3_stmt = stmts[0]
        actions = s3_stmt["Action"]
        if not isinstance(actions, list):
            actions = [actions]
        actions = [str(a) for a in actions]
        assert sorted(actions) == sorted(["s3:GetObject", "s3:PutObject"]), (
            f"S3 statement actions should be ['s3:GetObject', 's3:PutObject'], got {actions}"
        )

    def test_s3_statement_resource(self, template):
        """The S3 statement resource must be arn:aws:s3:::*."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        s3_stmt = stmts[0]
        resource = s3_stmt["Resource"]
        assert str(resource) == "arn:aws:s3:::*", (
            f"S3 resource should be 'arn:aws:s3:::*', got '{resource}'"
        )

    # -- EMR statement --

    def test_emr_statement_action(self, template):
        """The EMR statement must grant elasticmapreduce:*."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        emr_stmt = stmts[1]
        action = emr_stmt["Action"]
        if isinstance(action, list):
            action = action[0]
        assert str(action) == "elasticmapreduce:*", (
            f"EMR action should be 'elasticmapreduce:*', got '{action}'"
        )

    def test_emr_statement_resource_is_wildcard(self, template):
        """The EMR statement resource must be '*'."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        emr_stmt = stmts[1]
        resource = emr_stmt["Resource"]
        assert str(resource) == "*", (
            f"EMR resource should be '*', got '{resource}'"
        )

    def test_emr_statement_has_principal_account_condition(self, template):
        """The EMR statement must have a Condition restricting aws:PrincipalAccount."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        emr_stmt = stmts[1]
        assert "Condition" in emr_stmt, (
            "EMR statement must include a Condition block"
        )
        condition = emr_stmt["Condition"]
        assert "StringEquals" in condition, (
            f"Condition must use 'StringEquals', got keys: {list(condition.keys())}"
        )
        string_equals = condition["StringEquals"]
        assert "aws:PrincipalAccount" in string_equals, (
            f"StringEquals must include 'aws:PrincipalAccount', got keys: {list(string_equals.keys())}"
        )

    def test_emr_condition_references_account_id(self, template):
        """The PrincipalAccount condition value must reference AccountId via !Sub."""
        stmts = _get_policy_statements(template, self.POLICY_NAME)
        emr_stmt = stmts[1]
        cond_value = emr_stmt["Condition"]["StringEquals"]["aws:PrincipalAccount"]
        assert isinstance(cond_value, TaggedScalar) and cond_value.tag.value == "!Sub", (
            f"PrincipalAccount condition must use !Sub, got {cond_value!r}"
        )
        assert "${AccountId}" in cond_value.value, (
            f"Condition value must contain '${{AccountId}}', got '{cond_value.value}'"
        )
