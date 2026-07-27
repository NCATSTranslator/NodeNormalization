"""Tests for OpenAPI schema construction (node_normalizer/apidocs.py).

`construct_open_api_schema` regenerates `paths` from the route decorators and merges
back only the document-level metadata from resources/openapi.yml. These tests pin that
split, because the failure mode is silent: metadata that stops being copied just
disappears from the served schema, and endpoint documentation written in the wrong
place is never served at all.

Only reads the app singleton; does not mutate `app.state` (see tests/CLAUDE.md).
"""

import pytest

from node_normalizer.apidocs import construct_open_api_schema, get_app_info
from node_normalizer.server import app

# FastAPI's placeholder when a route declares no response description.
DEFAULT_RESPONSE_DESCRIPTION = "Successful Response"


@pytest.fixture(scope="module")
def schema():
    return app.openapi_schema


def test_yml_metadata_reaches_the_served_schema(schema):
    """Each of these is copied by an explicit branch in construct_open_api_schema.

    `license` in particular was parsed and then silently dropped for a long time.
    """
    info = schema["info"]
    assert info["license"]["name"] == "MIT"
    assert info["contact"]["email"]
    assert info["termsOfService"]
    assert info["x-translator"]["infores"] == "infores:sri-node-normalizer"
    assert info["x-trapi"]["version"]
    assert schema["servers"]


def test_paths_come_from_the_routes_not_the_yml(schema):
    """openapi.yml has no `paths` section; the routes are the only source."""
    assert "/get_normalized_nodes" in schema["paths"]
    assert "/llms.txt" in schema["paths"]

    from node_normalizer.config import RESOURCES_DIR
    import yaml

    with open(RESOURCES_DIR / "openapi.yml") as f:
        api_docs = yaml.safe_load(f)
    assert "paths" not in api_docs, (
        "openapi.yml grew a `paths` section again. It is not merged into the served "
        "schema, so anything documented there is invisible — put it on the route "
        "decorator in server.py instead."
    )


def test_every_operation_documents_itself(schema):
    """Guards against sliding back to FastAPI's placeholder descriptions."""
    for path, operations in schema["paths"].items():
        for method, operation in operations.items():
            where = f"{method.upper()} {path}"
            assert operation.get("description"), f"{where} has no description"
            assert operation.get("summary"), f"{where} has no summary"
            described = operation["responses"]["200"]["description"]
            assert described != DEFAULT_RESPONSE_DESCRIPTION, (
                f"{where} still uses FastAPI's default 200 description"
            )


def test_construct_open_api_schema_returns_the_cached_schema():
    """The early-out returned `app.openapi_schema()` — calling a dict, i.e. TypeError.

    Unreachable while server.py assigns the attribute directly, but it would fire the
    moment anything called app.openapi().
    """
    assert construct_open_api_schema(app) is app.openapi_schema


def test_get_app_info_reads_title_version_description():
    info = get_app_info()
    assert set(info) == {"title", "version", "description"}
    assert info["version"] == app.openapi_schema["info"]["version"]
