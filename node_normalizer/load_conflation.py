import asyncio
import logging
from pathlib import Path
from itertools import islice
from datetime import datetime
from typing import Dict, Any
import json
import hashlib
from itertools import combinations
import jsonschema
import os
from .redis_adapter import RedisConnectionFactory, RedisConnection
from bmt import Toolkit
from bmt.util import format as bmt_format
from .util import LoggingUtil

logger = LoggingUtil.init_logging()


class NodeLoader:
    """
    Class that gets all node definitions from a series of flat files
    and produces Translator compliant nodes which are then loaded into
    a redis database.
    """

    def __init__(self):
        self._config = self.get_config()

        self._compendium_directory: Path = Path(self._config["compendium_directory"])
        self._conflation_directory: Path = Path(self._config["conflation_directory"])
        self._test_mode: int = self._config["test_mode"]
        self._data_files: list = self._config["data_files"]
        self._conflations: list = self._config["conflations"]

        json_schema = Path(__file__).parent / "resources" / "valid_data_format.json"

        with open(json_schema) as json_file:
            self._validate_with = json.load(json_file)

        # Initialize storage instance vars for the semantic types and source prefixes
        self.semantic_types: set = set()
        self.source_prefixes: Dict = {}

        self.toolkit = Toolkit()
        self.ancestor_map = {}

    def get_ancestors(self, input_type):
        if input_type in self.ancestor_map:
            return self.ancestor_map[input_type]
        a = self.toolkit.get_ancestors(input_type)
        ancs = [bmt_format(ai) for ai in a]
        if input_type not in ancs:
            ancs = [input_type] + ancs
        self.ancestor_map[input_type] = ancs
        return ancs

    async def merge_semantic_meta_data(self):

        # get the connection and pipeline to the database

        types_prefixes_redis: RedisConnection = await self.get_redis("curie_to_bl_type_db")
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

        if self._test_mode != 1:
            # add the data to redis
            response = await RedisConnection.execute_pipeline(types_prefixes_pipeline)
            if asyncio.coroutines.iscoroutine(response):
                await response

    def validate_compendia(self, in_file):
        # open the file to validate
        with open(in_file, "r") as compendium:
            logger.info(f"Validating {in_file}...")

            # sample the file
            for line in islice(compendium, 5):
                try:
                    instance: dict = json.loads(line)

                    # validate the incoming json against the spec
                    jsonschema.validate(instance=instance, schema=self._validate_with)
                # catch any exceptions
                except Exception as e:
                    logger.error(f"Exception thrown in validate_compendia({in_file}): {e}")
                    return False

        return True

    # TODO: this strikes me as backwards.  Caller has to know and look up by index.  So the info about what index
    # does what is scattered.  Instead this should look up by what kind of redis you want and map to dbid for you.
    @staticmethod
    async def get_redis(db_name):
        """
        Return a redis instance
        """
        redis_config_path = Path(__file__).parent.parent / "redis_config.yaml"
        connection_factory: RedisConnectionFactory = await RedisConnectionFactory.create_connection_pool(redis_config_path)
        connection = connection_factory.get_connection(db_name)
        return connection

    async def load_conflation(self, conflation: dict, block_size: int) -> bool:
        """
        Given a conflation, load it into a redis so that it can
        be read by R3.
        """

        conflation_file = conflation["file"]
        conflation_redis_connection_name = conflation["redis_db"]
        # init a line counter
        line_counter: int = 0
        conflation_redis: RedisConnection = await self.get_redis(conflation_redis_connection_name)
        conflation_pipeline = conflation_redis.pipeline()

        with open(f"{self._conflation_directory}/{conflation_file}", "r", encoding="utf-8") as cfile:
            logger.info(f"Processing {conflation_file}...")

            # for each line in the file
            for line in cfile:
                line_counter = line_counter + 1

                # load the line into memory
                instance: dict = json.loads(line)

                for identifier in instance:
                    # We need to include the identifier in the list of identifiers so that we know its position
                    conflation_pipeline.set(identifier, line)

                if self._test_mode != 1 and line_counter % block_size == 0:
                    await RedisConnection.execute_pipeline(conflation_pipeline)
                    # Pipeline executed create a new one error
                    conflation_pipeline = conflation_redis.pipeline()
                    logger.info(f"{line_counter} {conflation_file} lines processed")

            if self._test_mode != 1:
                await RedisConnection.execute_pipeline(conflation_pipeline)
                logger.info(f"{line_counter} {conflation_file} total lines processed")

            print(f"Done loading {conflation_file}...")

        # return to the caller
        return True

