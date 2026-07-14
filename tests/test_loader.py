from pathlib import Path

from node_normalizer.loader import load_compendium, validate_compendium


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
