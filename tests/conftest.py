"""Shared pytest fixtures for CloudFormation template tests."""

from pathlib import Path

import pytest
from ruamel.yaml import YAML


TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "cloudformation" / "iceberg-pipeline-glue.yaml"
CONSOLIDATED_TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "cloudformation" / "iceberg-pipeline.yaml"


@pytest.fixture
def template():
    """Parse the CloudFormation template YAML and return it as a Python dict."""
    yaml = YAML()
    with open(TEMPLATE_PATH, "r") as f:
        data = yaml.load(f)
    return dict(data) if data else {}


@pytest.fixture
def consolidated_template():
    """Parse the consolidated CloudFormation template YAML and return it as a Python dict."""
    yaml = YAML()
    with open(CONSOLIDATED_TEMPLATE_PATH, "r") as f:
        data = yaml.load(f)
    return dict(data) if data else {}
