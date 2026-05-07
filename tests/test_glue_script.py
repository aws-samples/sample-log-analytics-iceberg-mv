"""Tests for Glue script modifications — runtime WAREHOUSE resolution.

Validates: Requirements 3.1, 3.3
"""

from pathlib import Path

import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "sample-glue-job-iceberg-materializedview-builder.py"
)


@pytest.fixture
def script_content():
    """Read the Glue script file and return its content as a string."""
    return SCRIPT_PATH.read_text()


def test_no_hardcoded_warehouse_placeholder(script_content):
    """The script must NOT contain the old CloudFormation placeholder string.

    Validates: Requirements 3.1
    """
    assert 's3://${IcebergDataBucketName}/stream-analytics.db' not in script_content, (
        "Script still contains the hardcoded placeholder "
        "'s3://${IcebergDataBucketName}/stream-analytics.db'"
    )


def test_get_resolved_options_includes_iceberg_data_bucket(script_content):
    """The getResolvedOptions call must include 'iceberg-data-bucket'.

    Validates: Requirements 3.3
    """
    assert "iceberg-data-bucket" in script_content, (
        "Script does not contain 'iceberg-data-bucket' in getResolvedOptions"
    )


def test_warehouse_uses_fstring_with_resolved_bucket(script_content):
    """WAREHOUSE must be constructed via an f-string using the resolved bucket name.

    Validates: Requirements 3.1, 3.3
    """
    # WAREHOUSE is constructed dynamically using the bucket and DATABASE variables
    assert 'f"s3://{iceberg_data_bucket}/' in script_content or "f's3://{iceberg_data_bucket}/" in script_content, (
        "Script does not contain an f-string constructing WAREHOUSE with the resolved bucket name"
    )
