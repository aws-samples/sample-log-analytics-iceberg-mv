"""Tests for S3 bucket resources in the CloudFormation template.

Validates: Requirements 3.1, 3.3, 4.1, 4.3
"""

import pytest
from ruamel.yaml.comments import TaggedScalar


PUBLIC_ACCESS_BLOCK_KEYS = [
    "BlockPublicAcls",
    "BlockPublicPolicy",
    "IgnorePublicAcls",
    "RestrictPublicBuckets",
]

BUCKET_CONFIGS = [
    ("IcebergDataBucket", "IcebergDataBucketName"),
    ("IcebergErrorsBucket", "IcebergErrorsBucketName"),
]


class TestS3BucketResources:
    """Verify both S3 bucket resources exist with the correct type."""

    @pytest.mark.parametrize("bucket_name,_", BUCKET_CONFIGS, ids=[b[0] for b in BUCKET_CONFIGS])
    def test_bucket_resource_exists(self, template, bucket_name, _):
        """The bucket resource must be defined in the Resources section."""
        assert bucket_name in template["Resources"], (
            f"Resource '{bucket_name}' is missing from the template"
        )

    @pytest.mark.parametrize("bucket_name,_", BUCKET_CONFIGS, ids=[b[0] for b in BUCKET_CONFIGS])
    def test_bucket_type_is_s3(self, template, bucket_name, _):
        """Each bucket resource must have Type AWS::S3::Bucket."""
        resource = template["Resources"][bucket_name]
        assert resource["Type"] == "AWS::S3::Bucket", (
            f"Resource '{bucket_name}' should have Type 'AWS::S3::Bucket'"
        )


class TestS3BucketNameReferences:
    """Verify each bucket's BucketName references the correct parameter via !Ref."""

    @pytest.mark.parametrize("bucket_name,param_name", BUCKET_CONFIGS, ids=[b[0] for b in BUCKET_CONFIGS])
    def test_bucket_name_uses_ref_tag(self, template, bucket_name, param_name):
        """BucketName must use a !Ref intrinsic function."""
        bucket_name_value = template["Resources"][bucket_name]["Properties"]["BucketName"]
        assert isinstance(bucket_name_value, TaggedScalar), (
            f"{bucket_name} BucketName should be a tagged scalar (!Ref)"
        )
        assert bucket_name_value.tag.value == "!Ref", (
            f"{bucket_name} BucketName tag should be '!Ref', got '{bucket_name_value.tag.value}'"
        )

    @pytest.mark.parametrize("bucket_name,param_name", BUCKET_CONFIGS, ids=[b[0] for b in BUCKET_CONFIGS])
    def test_bucket_name_references_correct_parameter(self, template, bucket_name, param_name):
        """BucketName !Ref must point to the correct parameter."""
        bucket_name_value = template["Resources"][bucket_name]["Properties"]["BucketName"]
        assert bucket_name_value.value == param_name, (
            f"{bucket_name} BucketName should reference '{param_name}', got '{bucket_name_value.value}'"
        )


class TestS3PublicAccessBlock:
    """Verify PublicAccessBlockConfiguration blocks all public access for both buckets."""

    @pytest.mark.parametrize("bucket_name,_", BUCKET_CONFIGS, ids=[b[0] for b in BUCKET_CONFIGS])
    def test_public_access_block_exists(self, template, bucket_name, _):
        """PublicAccessBlockConfiguration must be present on each bucket."""
        props = template["Resources"][bucket_name]["Properties"]
        assert "PublicAccessBlockConfiguration" in props, (
            f"{bucket_name} must have a PublicAccessBlockConfiguration"
        )

    @pytest.mark.parametrize(
        "bucket_name,_,setting",
        [
            (b, p, s)
            for b, p in BUCKET_CONFIGS
            for s in PUBLIC_ACCESS_BLOCK_KEYS
        ],
        ids=[
            f"{b}-{s}"
            for b, _ in BUCKET_CONFIGS
            for s in PUBLIC_ACCESS_BLOCK_KEYS
        ],
    )
    def test_public_access_block_setting_is_true(self, template, bucket_name, _, setting):
        """Each PublicAccessBlockConfiguration setting must be true."""
        pac = template["Resources"][bucket_name]["Properties"]["PublicAccessBlockConfiguration"]
        assert setting in pac, (
            f"{bucket_name} PublicAccessBlockConfiguration must include '{setting}'"
        )
        assert pac[setting] is True, (
            f"{bucket_name} {setting} must be true, got {pac[setting]!r}"
        )
