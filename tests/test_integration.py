"""
Integration tests for NodeNormalization.

These tests require a real Redis instance loaded with test data.
Run with:  pytest -v -m integration

Prerequisites:
  docker compose -f docker-compose-redis.yml up -d
  # load test data (see tests/conftest.py integration_client fixture)

Test CURIEs are sourced from tests/data/compendia/Gene.txt lines 1-2 and
tests/data/conflation/GeneProtein.txt lines 1-3.

  Gene line 1:  NCBIGene:65329674  — trnS-UGA, single-identifier clique
  Gene line 2:  NCBIGene:106478148 — canonical; ENSEMBL:LOC106478148 is equivalent
  GeneProtein:  NCBIGene:100123973 + UniProtKB:A0A7M7GCA5, UniProtKB:A0A7M7H3Y8
"""

import pytest

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Canonical gene CURIE with one identifier in its clique
SINGLE_ID_GENE = "NCBIGene:65329674"

# Canonical gene CURIE with two identifiers in its clique
CANONICAL_GENE = "NCBIGene:106478148"
NON_CANONICAL_GENE = "ENSEMBL:LOC106478148"  # equivalent to CANONICAL_GENE

# Gene present in GeneProtein conflation
CONFLATION_GENE = "NCBIGene:100123973"
CONFLATION_UNIPROT_1 = "UniProtKB:A0A7M7GCA5"
CONFLATION_UNIPROT_2 = "UniProtKB:A0A7M7H3Y8"

UNKNOWN_CURIE = "UNKNOWN:000000"
ANOTHER_UNKNOWN = "FAKE:999999"


# ===========================================================================
class TestStatusEndpoint:

    def test_status_returns_running(self, integration_client):
        response = integration_client.get("/status")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "running"

    def test_status_contains_all_database_names(self, integration_client):
        response = integration_client.get("/status")
        assert response.status_code == 200
        databases = response.json()["databases"]
        expected = {
            "eq_id_to_id_db",
            "id_to_eqids_db",
            "id_to_type_db",
            "curie_to_bl_type_db",
            "info_content_db",
            "gene_protein_db",
            "chemical_drug_db",
        }
        assert set(databases.keys()) == expected

    def test_status_core_databases_have_nonzero_counts(self, integration_client):
        """Canary: confirms that data loading ran before these tests."""
        response = integration_client.get("/status")
        assert response.status_code == 200
        databases = response.json()["databases"]
        for db_name in ("eq_id_to_id_db", "id_to_eqids_db", "id_to_type_db"):
            count = databases[db_name]["count"]
            assert count > 0, f"{db_name} has count=0; was test data loaded into Redis?"


# ===========================================================================
class TestGetNormalizedNodes:

    def test_get_known_canonical_gene_curie(self, integration_client):
        response = integration_client.get(
            "/get_normalized_nodes", params={"curie": [CANONICAL_GENE]}
        )
        assert response.status_code == 200
        result = response.json()
        assert CANONICAL_GENE in result
        node = result[CANONICAL_GENE]
        assert node is not None
        assert node["id"]["identifier"] == CANONICAL_GENE
        assert "biolink:Gene" in node["type"]

    def test_get_non_canonical_resolves_to_canonical(self, integration_client):
        """An equivalent (non-canonical) CURIE should resolve to the canonical."""
        response = integration_client.get(
            "/get_normalized_nodes", params={"curie": [NON_CANONICAL_GENE]}
        )
        assert response.status_code == 200
        result = response.json()
        assert NON_CANONICAL_GENE in result
        node = result[NON_CANONICAL_GENE]
        assert node is not None
        assert node["id"]["identifier"] == CANONICAL_GENE

    def test_get_unknown_curie_returns_null(self, integration_client):
        """Unknown CURIE should return None (not an error)."""
        response = integration_client.get(
            "/get_normalized_nodes", params={"curie": [UNKNOWN_CURIE]}
        )
        assert response.status_code == 200
        result = response.json()
        assert result == {UNKNOWN_CURIE: None}

    def test_get_mix_known_and_unknown(self, integration_client):
        response = integration_client.get(
            "/get_normalized_nodes",
            params={"curie": [UNKNOWN_CURIE, CANONICAL_GENE]},
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result) == 2
        assert result[UNKNOWN_CURIE] is None
        assert result[CANONICAL_GENE] is not None
        assert result[CANONICAL_GENE]["id"]["identifier"] == CANONICAL_GENE

    def test_get_all_unknown_returns_dict_of_nulls(self, integration_client):
        """
        Regression for https://github.com/NCATSTranslator/NodeNormalization/issues/113
        Previously returned {} instead of {curie: None, ...}.
        """
        response = integration_client.get(
            "/get_normalized_nodes",
            params={"curie": [UNKNOWN_CURIE, ANOTHER_UNKNOWN]},
        )
        assert response.status_code == 200
        result = response.json()
        assert result == {UNKNOWN_CURIE: None, ANOTHER_UNKNOWN: None}

    def test_post_known_canonical_gene_curie(self, integration_client):
        response = integration_client.post(
            "/get_normalized_nodes", json={"curies": [CANONICAL_GENE]}
        )
        assert response.status_code == 200
        result = response.json()
        assert CANONICAL_GENE in result
        assert result[CANONICAL_GENE]["id"]["identifier"] == CANONICAL_GENE

    def test_post_unknown_curie_returns_null(self, integration_client):
        response = integration_client.post(
            "/get_normalized_nodes", json={"curies": [UNKNOWN_CURIE]}
        )
        assert response.status_code == 200
        result = response.json()
        assert result == {UNKNOWN_CURIE: None}

    def test_post_all_unknown_returns_dict_of_nulls(self, integration_client):
        response = integration_client.post(
            "/get_normalized_nodes",
            json={"curies": [UNKNOWN_CURIE, ANOTHER_UNKNOWN]},
        )
        assert response.status_code == 200
        result = response.json()
        assert result == {UNKNOWN_CURIE: None, ANOTHER_UNKNOWN: None}

    def test_get_empty_list_returns_422(self, integration_client):
        response = integration_client.get(
            "/get_normalized_nodes", params={"curie": []}
        )
        assert response.status_code == 422

    def test_post_empty_list_returns_422(self, integration_client):
        response = integration_client.post(
            "/get_normalized_nodes", json={"curies": []}
        )
        assert response.status_code == 422

    def test_conflate_true_includes_uniprot_identifiers(self, integration_client):
        """With conflate=True, a gene in GeneProtein.txt should have UniProtKB equivalents."""
        response = integration_client.get(
            "/get_normalized_nodes",
            params={"curie": [CONFLATION_GENE], "conflate": True},
        )
        assert response.status_code == 200
        result = response.json()
        node = result.get(CONFLATION_GENE)
        assert node is not None
        equiv_ids = [eq["identifier"] for eq in node.get("equivalent_identifiers", [])]
        assert any(eid.startswith("UniProtKB:") for eid in equiv_ids), (
            f"Expected UniProtKB identifiers in equivalents with conflate=True, got: {equiv_ids}"
        )

    def test_conflate_false_excludes_uniprot_identifiers(self, integration_client):
        """With conflate=False, UniProtKB identifiers from conflation should not appear."""
        response = integration_client.get(
            "/get_normalized_nodes",
            params={"curie": [CONFLATION_GENE], "conflate": False},
        )
        assert response.status_code == 200
        result = response.json()
        node = result.get(CONFLATION_GENE)
        assert node is not None
        equiv_ids = [eq["identifier"] for eq in node.get("equivalent_identifiers", [])]
        assert not any(eid.startswith("UniProtKB:") for eid in equiv_ids), (
            f"Expected no UniProtKB identifiers with conflate=False, got: {equiv_ids}"
        )


# ===========================================================================
class TestGetSemanticTypes:

    def test_semantic_types_nonempty(self, integration_client):
        response = integration_client.get("/get_semantic_types")
        assert response.status_code == 200
        data = response.json()
        types = data["semantic_types"]["types"]
        assert len(types) > 0

    def test_semantic_types_includes_gene_and_protein(self, integration_client):
        response = integration_client.get("/get_semantic_types")
        assert response.status_code == 200
        types = response.json()["semantic_types"]["types"]
        assert "biolink:Gene" in types
        assert "biolink:Protein" in types


# ===========================================================================
class TestGetCuriePrefixes:

    def test_get_all_prefixes_nonempty(self, integration_client):
        response = integration_client.get("/get_curie_prefixes")
        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0

    def test_get_gene_prefixes_contains_ncbigene(self, integration_client):
        response = integration_client.get(
            "/get_curie_prefixes",
            params={"semantic_type": ["biolink:Gene"]},
        )
        assert response.status_code == 200
        data = response.json()
        assert "biolink:Gene" in data
        prefixes = data["biolink:Gene"]["curie_prefix"]
        assert "NCBIGene" in prefixes

    def test_post_gene_prefixes_contains_ncbigene(self, integration_client):
        response = integration_client.post(
            "/get_curie_prefixes",
            json={"semantic_types": ["biolink:Gene"]},
        )
        assert response.status_code == 200
        data = response.json()
        assert "biolink:Gene" in data
        prefixes = data["biolink:Gene"]["curie_prefix"]
        assert "NCBIGene" in prefixes

    def test_unknown_semantic_type_returns_empty_dict(self, integration_client):
        response = integration_client.get(
            "/get_curie_prefixes",
            params={"semantic_type": ["biolink:NonExistentType"]},
        )
        assert response.status_code == 200
        assert response.json() == {}


# ===========================================================================
class TestGetSetId:

    def test_setid_is_deterministic(self, integration_client):
        """Calling get_setid twice with the same CURIEs returns the same hash."""
        params = {"curie": [CANONICAL_GENE, SINGLE_ID_GENE]}
        r1 = integration_client.get("/get_setid", params=params)
        r2 = integration_client.get("/get_setid", params=params)
        assert r1.status_code == 200
        assert r2.status_code == 200
        assert r1.json()["setid"] == r2.json()["setid"]

    def test_setid_normalizes_before_hashing(self, integration_client):
        """
        ENSEMBL:LOC106478148 and NCBIGene:106478148 are equivalent.
        Their setid should match the setid for just NCBIGene:106478148 since
        normalization collapses them to the same canonical CURIE.
        """
        r_canonical = integration_client.get(
            "/get_setid", params={"curie": [CANONICAL_GENE]}
        )
        r_non_canonical = integration_client.get(
            "/get_setid", params={"curie": [NON_CANONICAL_GENE]}
        )
        assert r_canonical.status_code == 200
        assert r_non_canonical.status_code == 200
        assert r_canonical.json()["setid"] == r_non_canonical.json()["setid"]

    def test_post_setid_multi_set(self, integration_client):
        """POST /get_setid accepts a list of sets and returns a list of results."""
        payload = [
            {"curies": [CANONICAL_GENE]},
            {"curies": [SINGLE_ID_GENE, CANONICAL_GENE]},
        ]
        response = integration_client.post("/get_setid", json=payload)
        assert response.status_code == 200
        results = response.json()
        assert isinstance(results, list)
        assert len(results) == 2
        # The two sets are different, so their setids should differ
        assert results[0]["setid"] != results[1]["setid"]

    def test_get_setid_no_params_returns_422(self, integration_client):
        response = integration_client.get("/get_setid", params={"curie": []})
        assert response.status_code == 422


# ===========================================================================
class TestQueryEndpoint:

    def test_query_with_known_gene_node(self, integration_client):
        """Minimal TRAPI query containing a known gene CURIE should normalize it."""
        payload = {
            "message": {
                "query_graph": {
                    "nodes": {"n0": {"ids": [CANONICAL_GENE]}},
                    "edges": {},
                },
                "knowledge_graph": {
                    "nodes": {
                        CANONICAL_GENE: {
                            "categories": ["biolink:Gene"],
                            "name": "LOC106478148",
                        }
                    },
                    "edges": {},
                },
                "results": [],
            }
        }
        response = integration_client.post("/query", json=payload)
        assert response.status_code == 200

    def test_query_with_empty_knowledge_graph(self, integration_client):
        """TRAPI with empty knowledge graph should return 200."""
        payload = {
            "message": {
                "query_graph": {"nodes": {}, "edges": {}},
                "knowledge_graph": {"nodes": {}, "edges": {}},
                "results": [],
            }
        }
        response = integration_client.post("/query", json=payload)
        assert response.status_code == 200


# ===========================================================================
class TestGetAllowedConflations:

    def test_conflations_list_contains_expected_types(self, integration_client):
        response = integration_client.get("/get_allowed_conflations")
        assert response.status_code == 200
        conflations = response.json()["conflations"]
        assert "GeneProtein" in conflations
        assert "DrugChemical" in conflations
