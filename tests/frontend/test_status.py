"""Test the /status endpoint (issue #377: openapi_version + backend keys)."""
from node_normalizer.server import app
from fastapi.testclient import TestClient


class MockStatusRedis:
    async def dbsize(self):
        return 0

    async def used_memory_rss_human(self):
        return "0B"


def test_status_openapi_version_and_backend():
    for db in (
        "eq_id_to_id_db", "id_to_eqids_db", "id_to_type_db", "curie_to_bl_type_db",
        "info_content_db", "gene_protein_db", "chemical_drug_db",
    ):
        setattr(app.state, db, MockStatusRedis())

    body = TestClient(app).get("/status").json()

    assert body["backend"] == "redis"
    assert body["version"] == app.openapi_schema["info"]["version"]
