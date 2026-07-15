"""
Integration test for the data loader: spins up a real Redis with testcontainers,
runs a full load through the loader, and asserts that keys land in the correct
logical databases — in particular that the conflation database does not collide
with eq_id_to_id_db (the db-index bug fixed in this branch).

Requires Docker. Marked `integration` so it can be excluded with
`pytest -m "not integration"` where Docker is unavailable.
"""
import json
from pathlib import Path

import pytest
import redis
from testcontainers.redis import RedisContainer

import node_normalizer.config as config_mod
import node_normalizer.loader.loader as loader_mod

pytestmark = pytest.mark.integration

# Production runs Bitnami Redis 23.1.1, i.e. Redis 8.2.1 on Debian 12. The loader
# only uses SET/GET/pipeline, so it is Redis-version-agnostic, but we track the
# production version and base via the official image (bookworm == Debian 12),
# which avoids Bitnami's auth/paywall and whose readiness probe testcontainers
# supports out of the box. Bump this as production Redis moves.
REDIS_IMAGE = "redis:8.2.1-bookworm"

RESOURCES = Path(__file__).parent / "resources"

# canonical logical-db -> index layout (matches redis_config.yaml)
DB_INDEX = {
    "eq_id_to_id_db": 0,
    "id_to_eqids_db": 1,
    "id_to_type_db": 2,
    "curie_to_bl_type_db": 3,
    "gene_protein_db": 4,
    "info_content_db": 5,
    "chemical_drug_db": 6,
}


@pytest.fixture
def loaded_redis(tmp_path, monkeypatch):
    with RedisContainer(REDIS_IMAGE) as container:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(6379))

        # A redis_config.yaml pointing every logical db at this container, each
        # at a distinct db index.
        redis_config = {
            name: {
                "ssl_enabled": False,
                "is_cluster": False,
                "db": index,
                "hosts": [{"host_name": host, "port": str(port)}],
                "password": "",
            }
            for name, index in DB_INDEX.items()
        }
        redis_config_path = tmp_path / "redis_config.yaml"
        import yaml

        redis_config_path.write_text(yaml.safe_dump(redis_config))

        # A one-line conflation file, and a config.json that loads Cell.txt plus
        # that conflation into chemical_drug_db.
        conflation_members = ["CHEBI:15377", "DRUGBANK:DB00898"]
        (tmp_path / "DrugChemical.txt").write_text(json.dumps(conflation_members) + "\n")

        config = {
            "compendium_directory": str(RESOURCES),
            "conflation_directory": str(tmp_path),
            "biolink_version": "v4.4.3",
            "test_mode": 0,
            "data_files": ["Cell.txt", "Disease.txt", "PhenotypicFeature.txt"],
            "conflations": [
                {
                    "types": ["biolink:ChemicalEntity", "biolink:Drug"],
                    "file": "DrugChemical.txt",
                    "redis_db": "chemical_drug_db",
                }
            ],
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))

        # Point the loader at the temp config/redis_config.
        monkeypatch.setattr(config_mod, "CONFIG_PATH", config_path)
        monkeypatch.setattr(loader_mod, "REDIS_CONFIG_PATH", redis_config_path)
        loader_mod.redis_connect.cache_clear()

        loader_mod.load_all(block_size=100)

        yield host, port, conflation_members

        loader_mod.redis_connect.cache_clear()


def _client(host, port, db):
    return redis.Redis(host=host, port=port, db=db, decode_responses=True)


def test_load_populates_correct_databases(loaded_redis):
    host, port, conflation_members = loaded_redis

    eq_id_to_id = _client(host, port, DB_INDEX["eq_id_to_id_db"])
    id_to_eqids = _client(host, port, DB_INDEX["id_to_eqids_db"])
    id_to_type = _client(host, port, DB_INDEX["id_to_type_db"])
    info_content = _client(host, port, DB_INDEX["info_content_db"])
    chemical_drug = _client(host, port, DB_INDEX["chemical_drug_db"])

    # The compendium populated the id databases.
    assert eq_id_to_id.dbsize() > 0
    assert id_to_eqids.dbsize() > 0
    assert id_to_type.dbsize() > 0

    # eq_id_to_id keys are upper-cased; a known Cell.txt id resolves.
    assert eq_id_to_id.get("UMLS:C0229659".upper()) is not None

    # info_content_db is now a clique-property store keyed by canonical id: its value
    # is a JSON dict {"preferred_name", "ic"}, not a bare float. Cell.txt has no
    # preferred_name, so it loads as "" alongside the ic.
    #
    # ic here is the *string* "100", not 100.0: Babel encodes ic as a JSON string in
    # Cell.txt but as a JSON number in Disease.txt (below), and the loader stores
    # instance["ic"] verbatim without coercing, so db 5 preserves whichever type came
    # in. _clique_props() tolerates both.
    props = json.loads(info_content.get("UMLS:C0229659"))
    assert props == {"preferred_name": "", "ic": "100"}

    # A clique that carries both a preferred_name and an IC (Disease.txt) round-trips
    # both through db 5, keyed by its canonical id.
    disease_props = json.loads(info_content.get("UMLS:C4288892"))
    assert disease_props == {"preferred_name": "Infant Acute Undifferentiated Leukemia", "ic": 100.0}

    # Taxa are stored per-identifier in the "t" field of the id_to_eqids_db blob (not
    # in db 5), so they round-trip alongside labels and descriptions. PhenotypicFeature.txt
    # has a human-taxon clique; its canonical id's identifiers carry the taxon.
    pheno_ids = json.loads(id_to_eqids.get("HP:0009278"))
    assert pheno_ids[0] == {
        "i": "HP:0009278",
        "l": "Ulnar deviation of the 4th finger",
        "d": ["Displacement of the 4th finger towards the ulnar side (i.e., towards the 5th finger)."],
        "t": ["NCBITaxon:9606"],
    }

    # The conflation landed in chemical_drug_db (db 6)...
    for member in conflation_members:
        assert chemical_drug.get(member) is not None

    # ...and NOT in eq_id_to_id_db. This is the db-collision regression guard:
    # before the fix chemical_drug_db was db 0, so these would leak into it.
    for member in conflation_members:
        assert eq_id_to_id.get(member) is None
