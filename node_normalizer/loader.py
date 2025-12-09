import asyncio
import logging
import traceback
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
from bmt.utils import format_element as bmt_format

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

    @staticmethod
    def get_config() -> Dict[str, Any]:
        """get configuration file"""
        cname = Path(__file__).parents[1] / "config.json"

        with open(cname, "r") as json_file:
            data = json.load(json_file)

        return data

    def convert_to_kgx(self, outfile_name) -> bool:
        """
        Given a compendia directory, create a KGX node file
        """

        # init the return value
        ret_val = True

        line_counter: int = 0

        try:
            # get the list of files in the directory
            compendia: list = self.get_compendia()

            nodes: list = []
            edges: list = []
            pass_nodes: list = []

            # did we get all the files
            if len(compendia) == len(self._data_files):
                # open the output file and start loading it
                with open(os.path.join(self._compendium_directory, outfile_name + "_nodes.jsonl"), "w", encoding="utf-8") as node_file, open(
                    os.path.join(self._compendium_directory, outfile_name + "_edges.jsonl"), "w", encoding="utf-8"
                ) as edge_file:

                    # set the flag for suppressing the first ",\n" in the written data
                    first = True

                    # for each file validate and process
                    for comp in compendia:
                        # get the true path to the file
                        comp = os.path.join(self._compendium_directory, comp)

                        # check the validity of the file
                        if self.validate_compendia(comp):
                            with open(comp, "r", encoding="utf-8") as compendium:
                                logger.info(f"Processing {comp}...")

                                # get the name of the source
                                # source = os.path.split(comp)[-1]

                                # for each line in the file
                                for line in compendium:
                                    # increment the record counter
                                    line_counter += 1

                                    # clear storage for this pass
                                    pass_nodes.clear()

                                    # load the line into memory
                                    instance: dict = json.loads(line)

                                    # all ids (even the root one) are in the equivalent identifiers
                                    if len(instance["identifiers"]) > 0:
                                        # loop through each identifier and create a node
                                        for equiv_id in instance["identifiers"]:
                                            # check to see if there is a label. if there is use it
                                            if "l" in equiv_id:
                                                name = equiv_id["l"]
                                            else:
                                                name = ""

                                            # add the node to the ones in this pass
                                            pass_nodes.append(
                                                {
                                                    "id": equiv_id["i"],
                                                    "name": name,
                                                    "category": instance["type"],
                                                    "equivalent_identifiers": list(x["i"] for x in instance["identifiers"]),
                                                }
                                            )

                                        # get the combinations of the nodes in this pass
                                        combos = combinations(pass_nodes, 2)

                                        # for all the node combinations create an edge between them
                                        for c in combos:
                                            # create a unique id
                                            record_id: str = c[0]["id"] + c[1]["id"] + f"{comp}"

                                            # save the edge
                                            edges.append(
                                                {
                                                    "id": f'{hashlib.md5(record_id.encode("utf-8")).hexdigest()}',
                                                    "subject": c[0]["id"],
                                                    "predicate": "biolink:same_as",
                                                    "object": c[1]["id"],
                                                }
                                            )

                                    # save the nodes in this pass to the big list
                                    nodes.extend(pass_nodes)

                                    # did we reach the write threshold
                                    if line_counter == 10000:
                                        # first time in doesnt get a leading comma
                                        if first:
                                            prefix = ""
                                        else:
                                            prefix = "\n"

                                        # reset the first record flag
                                        first = False

                                        # reset the line counter for the next group
                                        line_counter = 0

                                        # get all the nodes in a string and write them out
                                        nodes_to_write = prefix + "\n".join([json.dumps(node) for node in nodes])
                                        node_file.write(nodes_to_write)

                                        # are there any edges to output
                                        if len(edges) > 0:
                                            # get all the edges in a string and write them out
                                            edges_to_write = prefix + "\n".join([json.dumps(edge) for edge in edges])
                                            edge_file.write(edges_to_write)

                                        # reset for the next group
                                        nodes.clear()
                                        edges.clear()

                                # pick up any remainders in the file
                                if len(nodes) > 0:
                                    nodes_to_write = "\n" + "\n".join([json.dumps(node) for node in nodes])
                                    node_file.write(nodes_to_write)

                                if len(edges) > 0:
                                    edges_to_write = "\n" + "\n".join([json.dumps(edge) for edge in edges])
                                    edge_file.write(edges_to_write)
                        else:
                            logger.warning(f"Compendia file {comp} is invalid.")
                            continue

        except Exception as e:
            logger.error(f"Exception thrown in convert_to_KGX(): {''.join(traceback.format_exc())}")
            ret_val = False

        # return to the caller
        return ret_val

    async def load(self, block_size) -> bool:
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

        # init the return value
        ret_val = True

        if self._test_mode == 1:
            logger.debug(f"Test mode enabled. No data will be produced.")

        # get the list of files in the directory
        compendia: list = self.get_compendia()
        types_prefixes_redis: RedisConnection = await self.get_redis("curie_to_bl_type_db")
        # did we get all the files
        if len(compendia) == len(self._data_files):
            # for each file validate and process
            for comp in compendia:
                # check the validity of the file
                if self.validate_compendia(comp):
                    # try to load the file
                    loaded = await self.load_compendium(comp, block_size)
                    semantic_types_redis_pipeline = types_prefixes_redis.pipeline()
                    # @TODO add meta data about files eg. checksum to this object
                    semantic_types_redis_pipeline.set(f"file-{str(comp)}", json.dumps({"source_prefixes": self.source_prefixes}))
                    if self._test_mode != 1:
                        response = await RedisConnection.execute_pipeline(semantic_types_redis_pipeline)
                        if asyncio.coroutines.iscoroutine(response):
                            await response
                    self.source_prefixes = {}
                    if not loaded:
                        logger.warning(f"Compendia file {comp} did not load.")
                        continue
                else:
                    logger.warning(f"Compendia file {comp} is invalid.")
                    continue
            for conf in self._conflations:
                loaded = await self.load_conflation(conf, block_size)
                if not loaded:
                    logger.warning(f"Conflation file {conf} did not load.")
                    continue
            # merge all semantic counts from other files / loaders
            await self.merge_semantic_meta_data()
        else:
            logger.error(f"Error: 1 or more data files were incorrect")
            ret_val = False

        # return to the caller
        return ret_val

    async def merge_semantic_meta_data(self):

        # get the connection and pipeline to the database

        types_prefixes_redis: RedisConnection = await self.get_redis("curie_to_bl_type_db")
        meta_data_keys = await types_prefixes_redis.keys("file-*")
        # recreate pipeline

        types_prefixes_pipeline = types_prefixes_redis.pipeline()
        # capture all keys except semenatic_types , as that would be the one that will contain the sum of all semantic types
        meta_data_keys = list(filter(lambda key: key != "semantic_types", meta_data_keys))

        # get actual data
        for meta_data_key in meta_data_keys:
            types_prefixes_pipeline.get(meta_data_key)
        meta_data = types_prefixes_pipeline.execute()
        if asyncio.coroutines.iscoroutine(meta_data):
            meta_data = await meta_data
        all_meta_data = {}
        for meta_data_key, meta_datum in zip(meta_data_keys, meta_data):
            if meta_datum:
                all_meta_data[meta_data_key] = json.loads(meta_datum.decode("utf-8"))
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

    def get_compendia(self):
        """
        Return the list of compendium files to load
        """
        file_list = [self._compendium_directory / file_name for file_name in self._data_files]

        for file in file_list:
            if not file.exists():
                # This should probably raise an exception
                logger.warning(f"file not found: {file.name}")

        return file_list

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

            # Fail if the file was empty.
            if line_counter == 0:
                raise RuntimeError(f"Conflation file {conflation_file} is empty.")

            print(f"Done loading {conflation_file}...")

        # return to the caller
        return True

    async def load_compendium(self, compendium_filename: str, block_size: int) -> bool:
        """
        Given the full path to a compendium, load it into redis so that it can
        be read by R3.  We also load extra keys, which are the upper-cased
        identifiers, for ease of use
        """

        # init a line counter
        line_counter: int = 0
        term2id_redis: RedisConnection = await self.get_redis("eq_id_to_id_db")
        id2eqids_redis: RedisConnection = await self.get_redis("id_to_eqids_db")
        id2type_redis: RedisConnection = await self.get_redis("id_to_type_db")
        info_content_redis: RedisConnection = await self.get_redis("info_content_db")

        term2id_pipeline = term2id_redis.pipeline()
        id2eqids_pipeline = id2eqids_redis.pipeline()
        id2type_pipeline = id2type_redis.pipeline()
        info_content_pipeline = info_content_redis.pipeline()

        with open(compendium_filename, "r", encoding="utf-8") as compendium:
            logger.info(f"Processing {compendium_filename}...")

            # for each line in the file
            for line in compendium:
                line_counter = line_counter + 1

                # load the line into memory
                instance: dict = json.loads(line)

                # save the identifier
                # "The" identifier is the first one in the presorted identifiers list
                identifier: str = instance["identifiers"][0]["i"]

                # We want to accumulate statistics for each implied type as well, though we are only keeping the
                # leaf type in the file (and redis).  so now is the time to expand.  We'll regenerate the same
                # list on output.
                semantic_types = self.get_ancestors(instance["type"])

                # for each semantic type in the list
                for semantic_type in semantic_types:
                    # save the semantic type in a set to avoid duplicates
                    self.semantic_types.add(semantic_type)

                    #  create a source prefix if it has not been encountered
                    if self.source_prefixes.get(semantic_type) is None:
                        self.source_prefixes[semantic_type] = {}

                    # go through each equivalent identifier in the data row
                    # each will be assigned the semantic type information
                    for equivalent_id in instance["identifiers"]:
                        # split the identifier to just get the data source out of the curie
                        source_prefix: str = equivalent_id["i"].split(":")[0]

                        # save the source prefix if no already there
                        if self.source_prefixes[semantic_type].get(source_prefix) is None:
                            self.source_prefixes[semantic_type][source_prefix] = 1
                        # else just increment the count for the semantic type/source
                        else:
                            self.source_prefixes[semantic_type][source_prefix] += 1

                        # equivalent_id might be an array, where the first element is
                        # the identifier, or it might just be a string. not worrying about that case yet.
                        equivalent_id = equivalent_id["i"]
                        term2id_pipeline.set(equivalent_id.upper(), identifier)
                        # term2id_pipeline.set(equivalent_id, identifier)

                    id2eqids_pipeline.set(identifier, json.dumps(instance["identifiers"]))
                    id2type_pipeline.set(identifier, instance["type"])

                    # if there is information content add it to the cache
                    if "ic" in instance and instance["ic"] is not None:
                        info_content_pipeline.set(identifier, instance["ic"])

                if self._test_mode != 1 and line_counter % block_size == 0:
                    await RedisConnection.execute_pipeline(term2id_pipeline)
                    await RedisConnection.execute_pipeline(id2eqids_pipeline)
                    await RedisConnection.execute_pipeline(id2type_pipeline)
                    await RedisConnection.execute_pipeline(info_content_pipeline)

                    # Pipeline executed create a new one error
                    term2id_pipeline = term2id_redis.pipeline()
                    id2eqids_pipeline = id2eqids_redis.pipeline()
                    id2type_pipeline = id2type_redis.pipeline()
                    info_content_pipeline = info_content_redis.pipeline()

                    logger.info(f"{line_counter} {compendium_filename} lines processed")

            if self._test_mode != 1:
                await RedisConnection.execute_pipeline(term2id_pipeline)
                await RedisConnection.execute_pipeline(id2eqids_pipeline)
                await RedisConnection.execute_pipeline(id2type_pipeline)
                await RedisConnection.execute_pipeline(info_content_pipeline)

                logger.info(f"{line_counter} {compendium_filename} total lines processed")

            if line_counter == 0:
                raise RuntimeError(f"Compendium file {compendium_filename} is empty.")

            print(f"Done loading {compendium_filename}...")

        # return to the caller
        return True
