import asyncio
import os
from pathlib import Path
import json
from typing import Dict

import bmt as bmt
import asyncclick as click
import redis_adapter
from util import validate_compendium, LoggingUtil

logger = LoggingUtil.init_logging()


@click.command(no_args_is_help=True)
@click.option("--compendium-file", "-c", help="Compendia File", multiple=True)
@click.option("--block-size", "-b", help="Block Size", default=1000)
@click.option("--dry-run", "-d", help="Dry Run", default=False)
@click.option("--redis-config", "-r", help="Redis Config File", type=click.Path(), default=Path(__file__).parent.parent / "redis_config.yaml")
async def load(compendium_file, block_size, dry_run, redis_config) -> bool:
    """
    Given a compendia directory, load every file there into a running
    redis instance so that it can be read by R3
    """
    # The new style compendia files look like:
    # {"type": "biolink:Disease", "identifiers": [{"i": "UMLS:C4331330", "l": "Stage III Oropharyngeal (p16-Negative) Carcinoma AJCC v8"}, {"i": "NCIT:C132998", "l": "Stage III Oropharyngeal (p16-Negative) Carcinoma AJCC v8"}]}
    # {"type": "biolink:Disease", "identifiers": [{"i": "UMLS:C1274244", "l": "Dermatosis in a child"}, {"i": "SNOMEDCT:402803008"}]}

    # Update 11/4/2021: The files now look like:
    # {"type": "biolink:Disease", "ic": "100", "identifiers": [{"i": "UMLS:C4331330", "l": "Stage III Oropharyngeal (p16-Negative) Carcinoma AJCC v8"}, {"i": "NCIT:C132998", "l": "Stage III Oropharyngeal (p16-Negative) Carcinoma AJCC v8"}]}
    # {"type": "biolink:Disease", "identifiers": [{"i": "UMLS:C1274244", "l": "Dermatosis in a child"}, {"i": "SNOMEDCT:402803008"}]}

    # Update 11/4/2021: a new key of 'ic' (information content) is now incorporated for enhanced filtering of results.
    # Type is now a single biolink type so that we can save space rather than the gigantic array
    # identifiers replaces equivalent identifiers, and the keys are "i" and "l" rather than 'identifier" and "label".

    # the identifiers are ordered, such that the first identifier is the best identifier.
    # We are going to put these different parts into a few different redis tables, and reassemble and nicify on
    # output.  This will be a touch slower, but it will save a lot of space, and make conflation easier as well.

    # We will have the following redis databases:
    # 0: contains identifier.upper() -> canonical_id
    # 1: canonical_id -> equivalent_identifiers
    # 2: canonical_id -> biolink type
    # 3: types -> prefix counts
    # Update 11/4/2021: 4: info_content -> filtering value
    # 5-X: conflation databases consisting of canonical_id -> (list of conflated canonical_ids)
    #      Each of these databases corresponds to a particular conflation e.g. gene/protein or chemical/drug

    # for each file validate and process
    # check the validity of the files
    for comp in compendium_file:
        if not validate_compendium(comp, logger):
            logger.warning(f"Compendia file {comp} is invalid.")
            return False

    try:

        connection_factory: redis_adapter.RedisConnectionFactory = await redis_adapter.RedisConnectionFactory.create_connection_pool(redis_config)

        BIOLINK_VERSION = os.getenv("BIOLINK_VERSION", "2.1.0")
        toolkit = bmt.Toolkit(f"https://raw.githubusercontent.com/biolink/biolink-model/{BIOLINK_VERSION}/biolink-model.yaml")

        # get the list of files in the directory
        types_prefixes_redis: redis_adapter.RedisConnection = connection_factory.get_connection("curie_to_bl_type_db")

        for comp in compendium_file:
            # try to load the file
            loaded = await load_compendium(comp, block_size, connection_factory, toolkit, dry_run)
            semantic_types_redis_pipeline = types_prefixes_redis.pipeline()
            # @TODO add meta data about files eg. checksum to this object
            semantic_types_redis_pipeline.set(f"file-{str(comp)}", json.dumps({"source_prefixes": source_prefixes}))
            if not dry_run:
                response = await redis_adapter.RedisConnection.execute_pipeline(semantic_types_redis_pipeline)
                if asyncio.coroutines.iscoroutine(response):
                    await response
            # self.source_prefixes = {}
            if not loaded:
                logger.warning(f"Compendia file {comp} did not load.")
                continue
        # merge all semantic counts from other files / loaders
        await merge_semantic_meta_data(connection_factory, dry_run)
    except Exception as e:
        logger.error(f"Exception thrown in load(): {e}")
        raise e

    # return to the caller
    return True


async def merge_semantic_meta_data(connection_factory: redis_adapter.RedisConnectionFactory, dry_run: bool):
    # get the connection and pipeline to the database

    types_prefixes_redis: redis_adapter.RedisConnection = connection_factory.get_connection("curie_to_bl_type_db")
    meta_data_keys = await types_prefixes_redis.keys("file-*")
    # recreate pipeline

    types_prefixes_pipeline = types_prefixes_redis.pipeline()
    # capture all keys except semenatic_types , as that would be the one that will contain the sum of all semantic types
    meta_data_keys = list(filter(lambda key: key != "semantic_types", meta_data_keys[0]))

    # get actual data
    for meta_data_key in meta_data_keys:
        types_prefixes_pipeline.get(meta_data_key)

    meta_data = types_prefixes_pipeline.execute()

    if asyncio.coroutines.iscoroutine(meta_data):
        meta_data = await meta_data

    all_meta_data = {}

    for meta_data_key, meta_datum in zip(meta_data_keys, meta_data):
        if meta_datum:
            all_meta_data[meta_data_key.decode("utf-8")] = json.loads(meta_datum.decode("utf-8"))

    sources_prefix = {}

    for meta_data_key, data in all_meta_data.items():
        prefix_counts = data["source_prefixes"]
        for bl_type, curie_counts in prefix_counts.items():
            # if
            sources_prefix[bl_type] = sources_prefix.get(bl_type, {})
            for curie_prefix, count in curie_counts.items():
                # get count of this curie prefix
                sources_prefix[bl_type][curie_prefix] = sources_prefix[bl_type].get(curie_prefix, 0)
                # add up the new count
                sources_prefix[bl_type][curie_prefix] += count

    types_prefixes_pipeline = types_prefixes_redis.pipeline()

    if len(sources_prefix.keys()) > 0:
        # add all the semantic types
        types_prefixes_pipeline.lpush("semantic_types", *list(sources_prefix.keys()))

    # for each semantic type insert the list of source prefixes
    for item in sources_prefix:
        types_prefixes_pipeline.set(item, json.dumps(sources_prefix[item]))

    if not dry_run:
        # add the data to redis
        response = await redis_adapter.RedisConnection.execute_pipeline(types_prefixes_pipeline)
        if asyncio.coroutines.iscoroutine(response):
            await response




async def load_compendium(compendium_filename: str, block_size: int, connection_factory: redis_adapter.RedisConnectionFactory, toolkit: bmt.Toolkit, dry_run: bool) -> bool:
    """
    Given the full path to a compendium, load it into redis so that it can
    be read by R3.  We also load extra keys, which are the upper-cased
    identifiers, for ease of use
    """
    ancestor_map = {}
    source_prefixes: Dict = {}

    def get_ancestors(input_type):
        if input_type in ancestor_map:
            return ancestor_map[input_type]
        a = toolkit.get_ancestors(input_type)
        ancs = [toolkit.get_element(ai)["class_uri"] for ai in a]
        if input_type not in ancs:
            ancs = [input_type] + ancs
        ancestor_map[input_type] = ancs
        return ancs

    # init a line counter
    line_counter: int = 0
    try:

        term2id_redis: redis_adapter.RedisConnection = connection_factory.get_connection("eq_id_to_id_db")
        id2eqids_redis: redis_adapter.RedisConnection = connection_factory.get_connection("id_to_eqids_db")
        id2type_redis: redis_adapter.RedisConnection = connection_factory.get_connection("id_to_type_db")
        info_content_redis: redis_adapter.RedisConnection = connection_factory.get_connection("info_content_db")

        term2id_pipeline = term2id_redis.pipeline()
        id2eqids_pipeline = id2eqids_redis.pipeline()
        id2type_pipeline = id2type_redis.pipeline()
        info_content_pipeline = info_content_redis.pipeline()

        with open(compendium_filename, "r", encoding="utf-8") as compendium:
            logger.info(f"Processing {compendium_filename}...")

            # for each line in the file
            for line in compendium:
                line_counter = line_counter + 1

                # example line:
                # {"type": "biolink:Cell", "ic": "100", "identifiers": [{"i": "UMLS:C0229659", "l": "Myelomonocyte"}, {"i": "NCIT:C37041",
                # "l": "Neoplastic Monocyte"}, {"i": "SNOMEDCT:41621006"}]}

                # load the line into memory
                instance: dict = json.loads(line)

                # save the identifier
                # "The" identifier is the first one in the presorted identifiers list
                identifier: str = instance["identifiers"][0]["i"]

                # We want to accumulate statistics for each implied type as well, though we are only keeping the
                # leaf type in the file (and redis).  so now is the time to expand.  We'll regenerate the same
                # list on output.
                semantic_types = get_ancestors(instance["type"])

                # for each semantic type in the list
                for semantic_type in semantic_types:
                    # save the semantic type in a set to avoid duplicates
                    semantic_types.add(semantic_type)

                    #  create a source prefix if it has not been encountered
                    if source_prefixes.get(semantic_type) is None:
                        source_prefixes[semantic_type] = {}

                    # go through each equivalent identifier in the data row
                    # each will be assigned the semantic type information
                    for equivalent_id in instance["identifiers"]:
                        # split the identifier to just get the data source out of the curie
                        source_prefix: str = equivalent_id["i"].split(":")[0]

                        # save the source prefix if no already there
                        if source_prefixes[semantic_type].get(source_prefix) is None:
                            source_prefixes[semantic_type][source_prefix] = 1
                        # else just increment the count for the semantic type/source
                        else:
                            source_prefixes[semantic_type][source_prefix] += 1

                        # equivalent_id might be an array, where the first element is
                        # the identifier, or it might just be a string. not worrying about that case yet.
                        equivalent_id = equivalent_id["i"]
                        term2id_pipeline.set(equivalent_id.upper(), identifier)
                        # term2id_pipeline.set(equivalent_id, identifier)

                    id2eqids_pipeline.set(identifier, json.dumps(instance["identifiers"]))
                    id2type_pipeline.set(identifier, instance["type"])

                    # if there is information content add it to the cache
                    if "ic" in instance:
                        info_content_pipeline.set(identifier, instance["ic"])

                if not dry_run and line_counter % block_size == 0:
                    await redis_adapter.RedisConnection.execute_pipeline(term2id_pipeline)
                    await redis_adapter.RedisConnection.execute_pipeline(id2eqids_pipeline)
                    await redis_adapter.RedisConnection.execute_pipeline(id2type_pipeline)
                    await redis_adapter.RedisConnection.execute_pipeline(info_content_pipeline)

                    # Pipeline executed create a new one error
                    term2id_pipeline = term2id_redis.pipeline()
                    id2eqids_pipeline = id2eqids_redis.pipeline()
                    id2type_pipeline = id2type_redis.pipeline()
                    info_content_pipeline = info_content_redis.pipeline()

                    logger.info(f"{line_counter} {compendium_filename} lines processed")

            if not dry_run:
                await redis_adapter.RedisConnection.execute_pipeline(term2id_pipeline)
                await redis_adapter.RedisConnection.execute_pipeline(id2eqids_pipeline)
                await redis_adapter.RedisConnection.execute_pipeline(id2type_pipeline)
                await redis_adapter.RedisConnection.execute_pipeline(info_content_pipeline)

                logger.info(f"{line_counter} {compendium_filename} total lines processed")

            print(f"Done loading {compendium_filename}...")
    except Exception as e:
        logger.error(f"Exception thrown in load_compendium({compendium_filename}), line {line_counter}: {e}")
        return False

    # return to the caller
    return True


if __name__ == '__main__':
    load()
