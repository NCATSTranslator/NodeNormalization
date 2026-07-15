"""
The NodeNorm loader: reads flat Babel compendium and conflation files and
populates the backend Redis databases that the frontend queries.

This is a batch script — read a file, fill a pipeline, execute, repeat — so it
is written synchronously against redis-py. It is invoked one file at a time by
the loader Helm chart (which generates a single-file config.json per Kubernetes
Job and runs `python load.py`); see documentation/Loader.md.
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
    """
    Build the Biolink toolkit lazily so importing the loader stays cheap.

    The Biolink Model version is pinned by the `biolink_version` key in
    config.json (a tag, branch, or commit in the biolink-model repo) so that a
    load computes ancestors against the same model Babel built the data with,
    rather than whatever version bmt happens to default to.
    """
    global _toolkit
    if _toolkit is None:
        biolink_version = get_config()["biolink_version"]
        url = f"https://raw.githubusercontent.com/biolink/biolink-model/{biolink_version}/biolink-model.yaml"
        logger.info(f"Initializing Biolink Model Toolkit from {url}")
        _toolkit = Toolkit(url)
    return _toolkit


@lru_cache(maxsize=None)
def get_ancestors(input_type: str) -> list:
    """Return the Biolink type and all its ancestors, formatted as CURIEs."""
    ancestors = [bmt_format(a) for a in _get_toolkit().get_ancestors(input_type)]
    if input_type not in ancestors:
        ancestors = [input_type] + ancestors
    return ancestors


def _accumulate_source_prefixes(source_prefixes: dict, identifiers: list, semantic_types: list) -> None:
    """
    Fold one compendium line's CURIE-prefix counts into every implied semantic
    type. The prefixes are counted once for the line and then added to each
    ancestor bucket, rather than re-splitting every identifier once per type.
    """
    line_prefix_counts: dict = {}
    for equivalent_id in identifiers:
        source_prefix = equivalent_id["i"].split(":", 1)[0]
        line_prefix_counts[source_prefix] = line_prefix_counts.get(source_prefix, 0) + 1

    for semantic_type in semantic_types:
        type_counts = source_prefixes.setdefault(semantic_type, {})
        for source_prefix, count in line_prefix_counts.items():
            type_counts[source_prefix] = type_counts.get(source_prefix, 0) + count


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
        config = yaml.safe_load(config_file)[db_name]

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
        password=config.get("password") or None,
        ssl=config.get("ssl_enabled", False),
        decode_responses=True,
    )


def disable_periodic_save() -> None:
    """
    Turn off automatic RDB snapshots (`CONFIG SET save ""`) on every backend
    Redis for the duration of the load.

    The NodeNorm backend databases are write-once: they are created empty,
    populated by a load, then persisted with a single *manual* BGSAVE (per the
    loader SOP) and copied elsewhere to seed future `mode: restore` loads. The
    default `save 300 1000000` therefore just forks a multi-GB BGSAVE every few
    minutes during the write storm for no benefit — on the 150 GB+ databases the
    copy-on-write churn is a real drag. A crashed load leaves the database empty
    rather than partial, which is fine because loads are re-runnable.

    Best-effort: a server that refuses CONFIG SET (e.g. a locked-down managed
    Redis) is logged and skipped rather than failing the load. CONFIG SET save
    is server-wide, so this hits each backend instance once.
    """
    with open(REDIS_CONFIG_PATH) as config_file:
        db_names = list(yaml.safe_load(config_file).keys())

    for db_name in db_names:
        try:
            redis_connect(db_name).config_set("save", "")
            logger.info(f"Disabled periodic RDB save on {db_name} for the load.")
        except Exception as e:
            logger.warning(f"Could not disable periodic save on {db_name}: {e}")


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
    """Return the list of compendium file paths to load, raising if any are missing."""
    file_list = [compendium_directory / file_name for file_name in data_files]
    missing = [str(file) for file in file_list if not file.exists()]
    if missing:
        raise FileNotFoundError(f"Compendium file(s) not found: {', '.join(missing)}")
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

    # transaction=False: this is a bulk load, not an atomic update, so skip the
    # MULTI/EXEC framing around every flushed block.
    term2id_pipeline = term2id_redis.pipeline(transaction=False)
    id2eqids_pipeline = id2eqids_redis.pipeline(transaction=False)
    id2type_pipeline = id2type_redis.pipeline(transaction=False)
    info_content_pipeline = info_content_redis.pipeline(transaction=False)

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
            _accumulate_source_prefixes(source_prefixes, instance["identifiers"], semantic_types)

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

                term2id_pipeline = term2id_redis.pipeline(transaction=False)
                id2eqids_pipeline = id2eqids_redis.pipeline(transaction=False)
                id2type_pipeline = id2type_redis.pipeline(transaction=False)
                info_content_pipeline = info_content_redis.pipeline(transaction=False)

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
    conflation_pipeline = conflation_redis.pipeline(transaction=False)

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
                conflation_pipeline = conflation_redis.pipeline(transaction=False)
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

    meta_data_keys = types_prefixes_redis.keys("file-*")

    pipeline = types_prefixes_redis.pipeline(transaction=False)
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

    pipeline = types_prefixes_redis.pipeline(transaction=False)
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
    else:
        disable_periodic_save()

    # Raises FileNotFoundError if any named compendium is missing.
    compendia = get_compendia(compendium_directory, data_files)

    types_prefixes_redis = redis_connect("curie_to_bl_type_db")
    for comp in compendia:
        if not validate_compendium(comp):
            logger.warning(f"Compendia file {comp} is invalid.")
            continue

        source_prefixes = load_compendium(comp, block_size, test_mode)

        pipeline = types_prefixes_redis.pipeline(transaction=False)
        # @TODO add meta data about files eg. checksum to this object
        pipeline.set(f"file-{comp}", json.dumps({"source_prefixes": source_prefixes}))
        if test_mode != 1:
            pipeline.execute()

    for conf in conflations:
        load_conflation(conf, conflation_directory, block_size, test_mode)

    merge_semantic_meta_data(test_mode)
    return True
