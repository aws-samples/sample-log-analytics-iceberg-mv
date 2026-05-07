"""Tests for the deploy.sh deployment script.

Validates: Requirements 5.1, 5.2, 5.3, 5.6, 5.7
"""

from pathlib import Path


DEPLOY_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "deploy.sh"


def test_deploy_script_exists():
    """The deploy script must exist at scripts/deploy.sh."""
    assert DEPLOY_SCRIPT_PATH.exists(), f"Deploy script not found at {DEPLOY_SCRIPT_PATH}"


def test_deploy_script_has_bash_shebang():
    """The deploy script must start with a bash shebang line."""
    content = DEPLOY_SCRIPT_PATH.read_text()
    first_line = content.splitlines()[0]
    assert first_line == "#!/usr/bin/env bash", (
        f"Expected shebang '#!/usr/bin/env bash', got '{first_line}'"
    )


def test_deploy_script_uses_strict_error_handling():
    """The deploy script must use 'set -euo pipefail' for strict error handling."""
    content = DEPLOY_SCRIPT_PATH.read_text()
    assert "set -euo pipefail" in content, (
        "Deploy script must include 'set -euo pipefail'"
    )


def test_deploy_script_creates_s3_bucket():
    """The deploy script must create the S3 bucket if it doesn't exist."""
    content = DEPLOY_SCRIPT_PATH.read_text()
    assert "aws s3 mb" in content, (
        "Deploy script must contain 'aws s3 mb' to create the bucket"
    )


def test_deploy_script_uploads_glue_script():
    """The deploy script must upload the Glue ETL script to S3."""
    content = DEPLOY_SCRIPT_PATH.read_text()
    assert "aws s3 cp" in content, (
        "Deploy script must contain 'aws s3 cp' to upload the Glue script"
    )
