"""
The NodeNorm loader: reads flat Babel compendium and conflation files and
populates the backend Redis databases that the frontend queries.

This is a batch script — read a file, fill a pipeline, execute, repeat — so it
is written synchronously against redis-py. It is invoked one file at a time by
the loader Helm chart (which generates a single-file config.json per Kubernetes
Job and runs `python load.py`); see documentation/Development.md.
"""
import json
from functools import lru_cache
from itertools import islice
from pathlib import Path

import jsonschema
import redis
import yaml
from bmt import Toolkit
from bmt.utils import format_element as bmt_format

from ..config import get_config, REDIS_CONFIG_PATH, RESOURCES_DIR
from ..util import LoggingUtil

logger = LoggingUtil.init_logging()


_toolkit = None


def _get_toolkit() -> Toolkit:
    """Build the Biolink toolkit lazily so importing the loader stays cheap."""
    global _toolkit
    if _toolkit is None:
        _toolkit = Toolkit()
    return _toolkit


@lru_cache(maxsize=None)
def get_ancestors(input_type: str) -> list:
    """Return the Biolink type and all its ancestors, formatted as CURIEs."""
    ancestors = [bmt_format(a) for a in _get_toolkit().get_ancestors(input_type)]
    if input_type not in ancestors:
        ancestors = [input_type] + ancestors
    return ancestors


@lru_cache(maxsize=None)
def _load_schema() -> dict:
    with open(RESOURCES_DIR / "valid_data_format.json") as schema_file:
        return json.load(schema_file)


@lru_cache(maxsize=None)
def redis_connect(db_name: str) -> redis.Redis:
    """
    Return a synchronous Redis client for the named database in
    redis_config.yaml. Cluster mode is no longer supported (see
    documentation/Redis.md).
    """
    with open(REDIS_CONFIG_PATH) as config_file:
        config = yaml.load(config_file, yaml.FullLoader)[db_name]

    if config.get("is_cluster"):
        raise ValueError(
            f"Redis cluster mode is no longer supported (is_cluster: true for {db_name}). "
            "See documentation/Redis.md."
        )

    host = config["hosts"][0]
    return redis.Redis(
        host=host["host_name"],
        port=int(host["port"]),
        db=config["db"],
        password=config["password"] or None,
        ssl=config.get("ssl_enabled", False),
        decode_responses=True,
    )


def validate_compendium(in_file) -> bool:
    """Validate the first few lines of a compendium against the data schema."""
    schema = _load_schema()
    with open(in_file, "r") as compendium:
        logger.info(f"Validating {in_file}...")
        for line in islice(compendium, 5):
            try:
                instance = json.loads(line)
                jsonschema.validate(instance=instance, schema=schema)
            except Exception as e:
                logger.error(f"Exception thrown in validate_compendium({in_file}): {e}")
                return False
    return True


def get_compendia(compendium_directory: Path, data_files: list) -> list:
    """Return the list of compendium file paths to load, warning on any missing."""
    file_list = [compendium_directory / file_name for file_name in data_files]
    for file in file_list:
        if not file.exists():
            # This should probably raise an exception
            logger.warning(f"file not found: {file.name}")
    return file_list


def load_compendium(compendium_filename, block_size: int, test_mode: int = 0) -> dict:
    """
    Load a single compendium into Redis. Writes:
      eq_id_to_id_db:   UPPER(equivalent id) -> canonical id
      id_to_eqids_db:   canonical id -> equivalent identifiers (JSON)
      id_to_type_db:    canonical id -> biolink type
      info_content_db:  canonical id -> clique properties JSON {"preferred_name", "ic"}
    (info_content_db is being grown into a general clique-property store; see #306.
    It formerly held a bare information-content float, so readers must tolerate both.)
    Returns the per-type source-prefix counts accumulated from this file.
    """
    source_prefixes: dict = {}

    term2id_redis = redis_connect("eq_id_to_id_db")
    id2eqids_redis = redis_connect("id_to_eqids_db")
    id2type_redis = redis_connect("id_to_type_db")
    info_content_redis = redis_connect("info_content_db")

    term2id_pipeline = term2id_redis.pipeline()
    id2eqids_pipeline = id2eqids_redis.pipeline()
    id2type_pipeline = id2type_redis.pipeline()
    info_content_pipeline = info_content_redis.pipeline()

    line_counter = 0
    with open(compendium_filename, "r", encoding="utf-8") as compendium:
        logger.info(f"Processing {compendium_filename}...")

        for line in compendium:
            line_counter += 1
            instance = json.loads(line)

            # "The" identifier is the first one in the presorted identifiers list.
            identifier = instance["identifiers"][0]["i"]

            # We only keep the leaf type in the file (and redis), but we accumulate
            # prefix statistics for each implied (ancestor) type as well.
            semantic_types = get_ancestors(instance["type"])

            # Accumulate prefix statistics for the leaf type and every ancestor.
            for semantic_type in semantic_types:
                if source_prefixes.get(semantic_type) is None:
                    source_prefixes[semantic_type] = {}
                for equivalent_id in instance["identifiers"]:
                    source_prefix = equivalent_id["i"].split(":")[0]
                    if source_prefixes[semantic_type].get(source_prefix) is None:
                        source_prefixes[semantic_type][source_prefix] = 1
                    else:
                        source_prefixes[semantic_type][source_prefix] += 1

            # The Redis writes are independent of the semantic type, so do them
            # once per line rather than once per ancestor.
            for equivalent_id in instance["identifiers"]:
                term2id_pipeline.set(equivalent_id["i"].upper(), identifier)
            id2eqids_pipeline.set(identifier, json.dumps(instance["identifiers"]))
            id2type_pipeline.set(identifier, instance["type"])
            # Clique-level properties, keyed by canonical id. Every clique gets one
            # (unlike the old IC-only write, which skipped cliques without an "ic").
            props = {"preferred_name": instance.get("preferred_name", "")}
            if instance.get("ic") is not None:
                props["ic"] = instance["ic"]
            info_content_pipeline.set(identifier, json.dumps(props))

            if test_mode != 1 and line_counter % block_size == 0:
                term2id_pipeline.execute()
                id2eqids_pipeline.execute()
                id2type_pipeline.execute()
                info_content_pipeline.execute()

                term2id_pipeline = term2id_redis.pipeline()
                id2eqids_pipeline = id2eqids_redis.pipeline()
                id2type_pipeline = id2type_redis.pipeline()
                info_content_pipeline = info_content_redis.pipeline()

                logger.info(f"{line_counter} {compendium_filename} lines processed")

        if test_mode != 1:
            term2id_pipeline.execute()
            id2eqids_pipeline.execute()
            id2type_pipeline.execute()
            info_content_pipeline.execute()
            logger.info(f"{line_counter} {compendium_filename} total lines processed")

        if line_counter == 0:
            raise RuntimeError(f"Compendium file {compendium_filename} is empty.")

        print(f"Done loading {compendium_filename}...")

    return source_prefixes


def load_conflation(conflation: dict, conflation_directory: Path, block_size: int, test_mode: int = 0) -> None:
    """Load a conflation file: each identifier in a clique -> the whole clique line."""
    conflation_file = conflation["file"]
    conflation_redis = redis_connect(conflation["redis_db"])
    conflation_pipeline = conflation_redis.pipeline()

    line_counter = 0
    with open(f"{conflation_directory}/{conflation_file}", "r", encoding="utf-8") as cfile:
        logger.info(f"Processing {conflation_file}...")

        for line in cfile:
            line_counter += 1
            instance = json.loads(line)

            for identifier in instance:
                conflation_pipeline.set(identifier, line)

            if test_mode != 1 and line_counter % block_size == 0:
                conflation_pipeline.execute()
                conflation_pipeline = conflation_redis.pipeline()
                logger.info(f"{line_counter} {conflation_file} lines processed")

        if test_mode != 1:
            conflation_pipeline.execute()
            logger.info(f"{line_counter} {conflation_file} total lines processed")

        if line_counter == 0:
            raise RuntimeError(f"Conflation file {conflation_file} is empty.")

        print(f"Done loading {conflation_file}...")


def merge_semantic_meta_data(test_mode: int = 0) -> None:
    """
    Sum the per-file source-prefix counts written during compendium loading into
    the aggregate `semantic_types` list and per-type prefix-count keys that the
    frontend serves from curie_to_bl_type_db.
    """
    types_prefixes_redis = redis_connect("curie_to_bl_type_db")

    meta_data_keys = [key for key in types_prefixes_redis.keys("file-*") if key != "semantic_types"]

    pipeline = types_prefixes_redis.pipeline()
    for meta_data_key in meta_data_keys:
        pipeline.get(meta_data_key)
    meta_data = pipeline.execute()

    all_meta_data = {}
    for meta_data_key, meta_datum in zip(meta_data_keys, meta_data):
        if meta_datum:
            all_meta_data[meta_data_key] = json.loads(meta_datum)

    sources_prefix: dict = {}
    for data in all_meta_data.values():
        for bl_type, curie_counts in data["source_prefixes"].items():
            sources_prefix.setdefault(bl_type, {})
            for curie_prefix, count in curie_counts.items():
                sources_prefix[bl_type][curie_prefix] = sources_prefix[bl_type].get(curie_prefix, 0) + count

    pipeline = types_prefixes_redis.pipeline()
    if sources_prefix:
        pipeline.lpush("semantic_types", *list(sources_prefix.keys()))
    for bl_type, counts in sources_prefix.items():
        pipeline.set(bl_type, json.dumps(counts))

    if test_mode != 1:
        pipeline.execute()


def load_all(block_size: int = 100_000) -> bool:
    """
    Load every compendium and conflation named in config.json into Redis.
    Returns True on success.
    """
    config = get_config()
    compendium_directory = Path(config["compendium_directory"])
    conflation_directory = Path(config["conflation_directory"])
    test_mode = config["test_mode"]
    data_files = config["data_files"]
    conflations = config["conflations"]

    if test_mode == 1:
        logger.debug("Test mode enabled. No data will be produced.")

    compendia = get_compendia(compendium_directory, data_files)
    if len(compendia) != len(data_files):
        logger.error("Error: 1 or more data files were incorrect")
        return False

    types_prefixes_redis = redis_connect("curie_to_bl_type_db")
    for comp in compendia:
        if not validate_compendium(comp):
            logger.warning(f"Compendia file {comp} is invalid.")
            continue

        source_prefixes = load_compendium(comp, block_size, test_mode)

        pipeline = types_prefixes_redis.pipeline()
        # @TODO add meta data about files eg. checksum to this object
        pipeline.set(f"file-{comp}", json.dumps({"source_prefixes": source_prefixes}))
        if test_mode != 1:
            pipeline.execute()

    for conf in conflations:
        load_conflation(conf, conflation_directory, block_size, test_mode)

    merge_semantic_meta_data(test_mode)
    return True
