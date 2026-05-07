"""Tests for template integrity: reference resolution, circular dependencies, and YAML round-trip.

Validates: Requirements 10.1, 10.2, 10.3, 10.4
"""

import re
from io import StringIO
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.comments import TaggedScalar


TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "cloudformation" / "iceberg-pipeline-glue.yaml"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collect_tagged_scalars(node):
    """Recursively walk a parsed YAML tree and yield all TaggedScalar objects."""
    if isinstance(node, TaggedScalar):
        yield node
    elif isinstance(node, dict):
        for value in node.values():
            yield from _collect_tagged_scalars(value)
    elif isinstance(node, list):
        for item in node:
            yield from _collect_tagged_scalars(item)


def _extract_sub_param_names(sub_value):
    """Extract parameter names from a !Sub string like 'arn:aws:iam::${AccountId}:role/${RoleName}'."""
    return re.findall(r"\$\{(\w+)\}", sub_value)


def _build_resource_dependency_graph(resources):
    """Build a dependency graph from !Ref and !GetAtt references between resources.

    Returns a dict mapping resource name -> set of resource names it depends on.
    """
    resource_names = set(resources.keys())
    graph = {name: set() for name in resource_names}

    for res_name, res_body in resources.items():
        for tagged in _collect_tagged_scalars(res_body):
            tag = tagged.tag.value
            if tag == "!Ref" and tagged.value in resource_names:
                graph[res_name].add(tagged.value)
            elif tag == "!GetAtt":
                # !GetAtt values look like "ResourceName.Attribute"
                ref_resource = tagged.value.split(".")[0]
                if ref_resource in resource_names:
                    graph[res_name].add(ref_resource)
    return graph


def _has_cycle(graph):
    """Detect cycles in a directed graph using iterative DFS with three-color marking.

    Returns True if a cycle exists, False otherwise.
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {node: WHITE for node in graph}

    for start in graph:
        if color[start] != WHITE:
            continue
        stack = [(start, False)]
        while stack:
            node, processed = stack.pop()
            if processed:
                color[node] = BLACK
                continue
            if color[node] == GRAY:
                return True
            color[node] = GRAY
            stack.append((node, True))
            for neighbor in graph.get(node, set()):
                if color[neighbor] == GRAY:
                    return True
                if color[neighbor] == WHITE:
                    stack.append((neighbor, False))
    return False


def _deep_equal(a, b):
    """Recursively compare two parsed YAML structures for structural equality.

    Handles TaggedScalar objects by comparing their tag and value.
    """
    if isinstance(a, TaggedScalar) and isinstance(b, TaggedScalar):
        return a.tag.value == b.tag.value and a.value == b.value
    if isinstance(a, dict) and isinstance(b, dict):
        if set(a.keys()) != set(b.keys()):
            return False
        return all(_deep_equal(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        return all(_deep_equal(x, y) for x, y in zip(a, b))
    return a == b


# ---------------------------------------------------------------------------
# 1. Reference resolution  (Reqs 10.2, 10.3)
# ---------------------------------------------------------------------------

class TestParameterReferenceResolution:
    """Verify all !Ref and !Sub parameter references resolve to defined parameters."""

    def test_all_ref_tags_resolve_to_parameters_or_resources(self, template):
        """Every !Ref must point to a defined Parameter or Resource."""
        defined_params = set(template.get("Parameters", {}).keys())
        defined_resources = set(template.get("Resources", {}).keys())
        valid_targets = defined_params | defined_resources
        # Also include AWS pseudo-parameters
        pseudo_params = {
            "AWS::AccountId", "AWS::NotificationARNs", "AWS::NoValue",
            "AWS::Partition", "AWS::Region", "AWS::StackId",
            "AWS::StackName", "AWS::URLSuffix",
        }
        valid_targets |= pseudo_params

        unresolved = []
        for tagged in _collect_tagged_scalars(template):
            if tagged.tag.value == "!Ref":
                if tagged.value not in valid_targets:
                    unresolved.append(tagged.value)

        assert not unresolved, (
            f"Unresolved !Ref references: {unresolved}"
        )

    def test_all_sub_param_references_resolve(self, template):
        """Every ${ParamName} inside a !Sub string must resolve to a defined Parameter or Resource."""
        defined_params = set(template.get("Parameters", {}).keys())
        defined_resources = set(template.get("Resources", {}).keys())
        valid_targets = defined_params | defined_resources
        pseudo_params = {
            "AWS::AccountId", "AWS::NotificationARNs", "AWS::NoValue",
            "AWS::Partition", "AWS::Region", "AWS::StackId",
            "AWS::StackName", "AWS::URLSuffix",
        }
        valid_targets |= pseudo_params

        unresolved = []
        for tagged in _collect_tagged_scalars(template):
            if tagged.tag.value == "!Sub":
                param_names = _extract_sub_param_names(tagged.value)
                for pname in param_names:
                    if pname not in valid_targets:
                        unresolved.append((tagged.value, pname))

        assert not unresolved, (
            f"Unresolved !Sub parameter references: {unresolved}"
        )


# ---------------------------------------------------------------------------
# 2. Circular dependency detection  (Req 10.1)
# ---------------------------------------------------------------------------

class TestCircularDependencies:
    """Verify no circular dependencies exist in the resource graph."""

    def test_no_circular_dependencies(self, template):
        """The resource dependency graph must be acyclic."""
        resources = template.get("Resources", {})
        graph = _build_resource_dependency_graph(resources)
        assert not _has_cycle(graph), (
            f"Circular dependency detected in resource graph: {graph}"
        )


# ---------------------------------------------------------------------------
# 3. YAML round-trip  (Req 10.4)
# ---------------------------------------------------------------------------

class TestYamlRoundTrip:
    """Verify the template survives a parse → serialize → parse cycle without structural loss."""

    def test_round_trip_structural_equality(self):
        """Parse the YAML, re-serialize it, parse again, and assert structural equality."""
        yaml = YAML()

        # First parse
        with open(TEMPLATE_PATH, "r") as f:
            first_parse = yaml.load(f)

        # Serialize to string
        buf = StringIO()
        yaml.dump(first_parse, buf)
        serialized = buf.getvalue()

        # Second parse
        second_parse = yaml.load(serialized)

        assert _deep_equal(first_parse, second_parse), (
            "Template structure changed after YAML round-trip (parse → serialize → parse)"
        )
