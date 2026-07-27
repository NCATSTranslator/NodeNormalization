"""Tests for the /llms.txt endpoint, which serves skills/nodenorm/SKILL.md to AI agents.

These only issue GETs against the shared app singleton and never touch `app.state`, so
they are safe to run alongside the other frontend tests (see tests/CLAUDE.md). A
TestClient that is not used as a context manager does not run the lifespan, so no Redis
connection is needed.
"""

from starlette.testclient import TestClient

from node_normalizer.config import SKILL_PATH
from node_normalizer.server import app

client = TestClient(app)


def test_skill_file_exists():
    """SKILL_PATH must resolve; if this fails, /llms.txt 404s everywhere."""
    assert SKILL_PATH.is_file(), f"No skill file at {SKILL_PATH}"


def test_llms_txt_is_served_as_plain_text():
    response = client.get("/llms.txt")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert len(response.text) > 500


def test_llms_txt_strips_yaml_frontmatter():
    """The frontmatter is Claude Code packaging metadata and should not be served."""
    assert SKILL_PATH.read_text(encoding="utf-8").startswith("---"), (
        "Expected SKILL.md to start with YAML frontmatter; if that changed on purpose, "
        "this test and the stripping in server.llms_txt() are both now pointless."
    )

    body = client.get("/llms.txt").text
    assert not body.startswith("---")
    assert "name: nodenorm" not in body
    assert body.startswith("# ")


def test_llms_txt_documents_the_main_endpoint():
    """A guide that never names the endpoint it is a guide to is not useful."""
    body = client.get("/llms.txt").text
    assert "get_normalized_nodes" in body
    # The GET/POST conflation-default divergence (#398) is the trap most likely to
    # silently give an agent wrong results, so it must survive any rewrite.
    assert "drug_chemical_conflate" in body
