"""Test node_normalizer server.py"""
import json
from node_normalizer.server import app
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
from .helpers.redis_mocks import mock_get_equivalent_curies, mock_get_ic
from pathlib import Path
import os
from bmt import Toolkit


class MockRedis:
    def __init__(self, data):
        self.data = data

    async def mget(self, *args, **kwargs):
        return [self.data[x] if x in self.data else None for x in args]


# Id -> Canonical
app.state.eq_id_to_id_db = MockRedis(
    {"DOID:3812": "MONDO:0005002", "MONDO:0005002": "MONDO:0005002"}
)
# Canonical->Equiv
app.state.id_to_eqids_db = MockRedis(
    {"MONDO:0005002": json.dumps([{"i": "MONDO:0005002"}, {"i": "DOID:3812"}])}
)
app.state.id_to_type_db = MockRedis({"MONDO:0005002": "biolink:Disease"})
app.state.curie_to_bl_type_db = MockRedis({})
app.state.info_content_db = MockRedis({})
app.state.gene_protein_db = MockRedis({})
#app.state.ancestor_map = {"biolink:Disease": ["biolink:Disease", "biolink:NamedThing"]}
app.state.toolkit = Toolkit()
app.state.ancestor_map = {}


def test_not_found():
    client = TestClient(app)
    response = client.get("/get_normalized_nodes", params={"curie": ["UNKNOWN:000000"]})
    result = json.loads(response.text)
    assert result == {"UNKNOWN:000000": None}
    response = client.post("/get_normalized_nodes", json={"curies": ["UNKNOWN:000000"]})
    result = json.loads(response.text)
    assert result == {"UNKNOWN:000000": None}


def test_one_missing():
    client = TestClient(app)
    response = client.get(
        "/get_normalized_nodes", params={"curie": ["UNKNOWN:000000", "DOID:3812"]}
    )
    result = json.loads(response.text)
    assert len(result) == 2
    assert result["UNKNOWN:000000"] == None
    assert result["DOID:3812"]["id"]["identifier"] == "MONDO:0005002"


def test_all_missing():
    """
    /get_normalized_nodes previously returned {} if none of the provided CURIEs are resolvable.
    This test ensures that that bug has been fixed.

    Reported in https://github.com/TranslatorSRI/NodeNormalization/issues/113
    """
    client = TestClient(app)

    # Test GET
    response = client.get(
        "/get_normalized_nodes",
        params={"curie": ["NCBIGene:ABCD", "NCBIGene:GENE:1017"]},
    )
    result = json.loads(response.text)
    assert result == {"NCBIGene:ABCD": None, "NCBIGene:GENE:1017": None}

    # Test POST
    response = client.post(
        "/get_normalized_nodes",
        json={"curies": ["NCBIGene:ABCD", "NCBIGene:GENE:1017"]},
    )
    result = json.loads(response.text)
    assert result == {"NCBIGene:ABCD": None, "NCBIGene:GENE:1017": None}


def test_merge():
    client = TestClient(app)
    response = client.get(
        "/get_normalized_nodes", params={"curie": ["MONDO:0005002", "DOID:3812"]}
    )
    result = json.loads(response.text)
    assert len(result) == 2
    assert "MONDO:0005002" in result
    assert "DOID:3812" in result
    assert result["MONDO:0005002"]["type"][0] == "biolink:Disease"


def test_empty():
    client = TestClient(app)

    # GET
    response = client.get("/get_normalized_nodes", params={"curie": []})
    assert response.status_code == 422
    assert response.reason == "Unprocessable Entity"
    result = json.loads(response.text)
    assert result["detail"][0]["msg"] == "ensure this value has at least 1 items"
    assert result["detail"][0]["loc"] == ["query", "curie"]

    # POST
    response = client.post("/get_normalized_nodes", json={"curies": []})
    assert response.status_code == 422
    assert response.reason == "Unprocessable Entity"
    result = json.loads(response.text)
    assert result["detail"][0]["msg"] == "ensure this value has at least 1 items"
    assert result["detail"][0]["loc"] == ["body", "curies"]
