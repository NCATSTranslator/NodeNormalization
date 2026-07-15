from pathlib import Path
from unittest.mock import patch

import pytest

import node_normalizer.loader.loader as loader_mod
from node_normalizer.loader import get_compendia, load_compendium, validate_compendium


good_json = Path(__file__).parent / "resources" / "datafile.json"
bad_json = Path(__file__).parent / "resources" / "datafile_with_errors.json"


def test_nn_load():
    # test_mode=1 buffers pipeline commands but never executes them, so no
    # running Redis is required.
    source_prefixes = load_compendium(good_json, 5, test_mode=1)
    assert source_prefixes


def test_nn_record_validation():
    assert validate_compendium(good_json)
    assert not validate_compendium(bad_json)


def test_get_compendia_raises_on_missing_file(tmp_path):
    # Missing files must fail fast rather than being returned and later crashing
    # load_all with a FileNotFoundError deep in validate_compendium.
    with pytest.raises(FileNotFoundError):
        get_compendia(tmp_path, ["does_not_exist.txt"])


class _FakePipeline:
    def __init__(self, parent):
        self.parent = parent

    def set(self, key, value):
        self.parent.sets.append((key, value))

    def execute(self):
        return []


class _FakeRedis:
    def __init__(self):
        self.sets = []

    def pipeline(self):
        return _FakePipeline(self)


def test_one_set_per_line():
    """
    Guards against reintroducing the per-ancestor write bug: the id -> value
    databases must get exactly one SET per input line, not one per Biolink
    ancestor.
    """
    fakes = {}

    def fake_connect(db_name):
        return fakes.setdefault(db_name, _FakeRedis())

    with patch.object(loader_mod, "redis_connect", fake_connect):
        load_compendium(good_json, block_size=5, test_mode=0)

    num_lines = sum(1 for line in open(good_json) if line.strip())
    assert len(fakes["id_to_eqids_db"].sets) == num_lines
    assert len(fakes["id_to_type_db"].sets) == num_lines
