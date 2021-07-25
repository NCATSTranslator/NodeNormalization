import asyncio
from pathlib import Path
from itertools import islice
from datetime import datetime
from typing import Dict, Any
import json
import hashlib
from itertools import combinations
import jsonschema
import os
from node_normalizer.redis_adapter import RedisConnectionFactory, RedisConnection


class NodeLoader:
    """
    Class that gets all node definitions from a series of flat files
    and produces Translator compliant nodes which are then loaded into
    a redis database.
    """

    def __init__(self):
        self._config = self.get_config()

        self._compendium_directory: Path = Path(self._config['compendium_directory'])
        self._test_mode: int = self._config['test_mode']
        self._data_files: list = self._config['data_files'].split(',')

        json_schema = Path(__file__).parent / 'resources' / 'valid_data_format.json'

        with open(json_schema) as json_file:
            self._validate_with = json.load(json_file)

        # Initialize storage instance vars for the semantic types and source prefixes
        self.semantic_types: set = set()
        self.source_prefixes: Dict = {}

    @staticmethod
    def get_config() -> Dict[str, Any]:
        """ get configuration file """
        cname = Path(__file__).parents[1] / 'config.json'

        with open(cname, 'r') as json_file:
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
                with open(os.path.join(self._compendium_directory, outfile_name + '_nodes.jsonl'), 'w', encoding="utf-8") as node_file, \
                     open(os.path.join(self._compendium_directory, outfile_name + '_edges.jsonl'), 'w', encoding="utf-8") as edge_file:

                    # set the flag for suppressing the first ",\n" in the written data
                    first = True

                    # for each file validate and process
                    for comp in compendia:
                        # check the validity of the file
                        if self.validate_compendia(comp):
                            with open(comp, 'r', encoding="utf-8") as compendium:
                                self.print_debug_msg(f'Processing {comp}...', True)

                                # get the name of the source
                                source = comp.parts[-1]

                                # for each line in the file
                                for line in compendium:
                                    # increment the record counter
                                    line_counter += 1

                                    # clear storage for this pass
                                    pass_nodes.clear()

                                    # load the line into memory
                                    instance: dict = json.loads(line)

                                    # all ids (even the root one) are in the equivalent identifiers
                                    if len(instance['equivalent_identifiers']) > 0:
                                        # loop through each identifier and create a node
                                        for equiv_id in instance['equivalent_identifiers']:
                                            # check to see if there is a label. if there is use it
                                            if 'label' in equiv_id:
                                                name = equiv_id['label']
                                            else:
                                                name = ''

                                            # add the node to the ones in this pass
                                            pass_nodes.append({'id': equiv_id['identifier'], 'name': name, 'category': instance['type'], 'equivalent_identifiers': [x['identifier'] for x in instance['equivalent_identifiers']]})

                                        # get the combinations of the nodes in this pass
                                        combos = combinations(pass_nodes, 2)

                                        # for all the node combinations create an edge between them
                                        for c in combos:
                                            # create a unique id
                                            record_id: str = c[0]['id'] + 'biolink:same_as' + c[1]['id'] + f'NodeNormalizer:{comp}'

                                            # save the edge
                                            edges.append({'id': f'{hashlib.md5(record_id.encode("utf-8")).hexdigest()}', 'subject': c[0]['id'], 'predicate': 'biolink:same_as', 'object': c[1]['id'], 'source_database': f'NodeNormalizer:{source}'})

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
                            self.print_debug_msg(f'Compendia file {comp} is invalid.', True)
                            continue

        except Exception as e:
            self.print_debug_msg(f'Exception thrown in convert_to_KGX(): {e}', True)
            ret_val = False

        # return to the caller
        return ret_val

    async def load(self, block_size) -> bool:
        """
        Given a compendia directory, load every file there into a running
        redis instance so that it can be read by R3
        """

        # init the return value
        ret_val = True

        if self._test_mode == 1:
            self.print_debug_msg(f'Test mode enabled. No data will be produced.', True)

        try:
            # get the list of files in the directory
            compendia: list = self.get_compendia()

            # did we get all the files
            if len(compendia) == len(self._data_files):
                # for each file validate and process
                for comp in compendia:
                    # check the validity of the file
                    if self.validate_compendia(comp):
                        # try to load the file
                        loaded = await self.load_compendium(comp, block_size)
                        if not loaded:
                            self.print_debug_msg(f'Compendia file {comp} did not load.', True)
                            continue
                    else:
                        self.print_debug_msg(f'Compendia file {comp} is invalid.', True)
                        continue

                # get the connection and pipeline to the database
                types_prefixes_redis: RedisConnection = await self.get_redis(2)
                types_prefixes_pipeline = types_prefixes_redis.pipeline()

                # create a command to get the current semantic types
                types_prefixes_pipeline.lrange('semantic_types', 0, -1)

                # get the current list of semantic types
                vals = types_prefixes_pipeline.execute()
                if asyncio.coroutines.iscoroutine(vals):
                    vals = await vals
                types_prefixes_pipeline = types_prefixes_redis.pipeline()
                # get the values and insure they are strings
                current_types: set = set(x.decode("utf-8") if not isinstance(x,str) else x for x in vals[0])

                # remove any dupes
                self.semantic_types = self.semantic_types.difference(current_types)

                if len(self.semantic_types) > 0:
                    # add all the semantic types
                    types_prefixes_pipeline.lpush('semantic_types', *self.semantic_types)

                # for each semantic type insert the list of source prefixes
                for item in self.source_prefixes:
                    types_prefixes_pipeline.set(item, json.dumps(self.source_prefixes[item]))

                if self._test_mode != 1:
                    # add the data to redis
                    response = RedisConnection.execute_pipeline(types_prefixes_pipeline)
                    if asyncio.coroutines.iscoroutine(response):
                        await response
            else:
                self.print_debug_msg(f'Error: 1 or more data files were incorrect', True)
                ret_val = False
        except Exception as e:
            raise e
            self.print_debug_msg(f'Exception thrown in load(): {e}', True)
            ret_val = False

        # return to the caller
        return ret_val

    def validate_compendia(self, in_file):
        # open the file to validate
        with open(in_file, 'r') as compendium:
            self.print_debug_msg(f'Validating {in_file}...', True)

            # sample the file
            for line in islice(compendium, 5):
                try:
                    instance: dict = json.loads(line)

                    # validate the incoming json against the spec
                    jsonschema.validate(instance=instance, schema=self._validate_with)
                # catch any exceptions
                except Exception as e:
                    self.print_debug_msg(f'Exception thrown in validate_compendia({in_file}): {e}', True)
                    return False

        return True

    def get_compendia(self):
        """
        Return the list of compendium files to load
        """
        file_list = [self._compendium_directory / file_name
                     for file_name in self._data_files]

        for file in file_list:
            if not file.exists():
                # This should probably raise an exception
                self.print_debug_msg(f'DEBUG: file not found: {file.name}')

        return file_list

    async def get_redis(self, dbid):
        """
        Return a redis instance
        """
        db_id_mapping = {
            0: RedisConnectionFactory.ID_TO_ID_DB_CONNECTION_NAME,
            1: RedisConnectionFactory.ID_TO_NODE_DATA_DB_CONNECTION_NAME,
            2: RedisConnectionFactory.CURIE_PREFIX_TO_BL_TYPE_DB_CONNECTION_NAME
        }
        redis_config_path = Path(__file__).parent.parent / 'redis_config.yaml'
        connection_factory: RedisConnectionFactory = await RedisConnectionFactory.create_connection_pool(redis_config_path)
        connection = connection_factory.get_connection(db_id_mapping[dbid])
        return connection

    async def load_compendium(self, compendium_filename: str, block_size: int) -> bool:
        """
        Given the full path to a compendium, load it into redis so that it can
        be read by R3.  We also load extra keys, which are the upper-cased
        identifiers, for ease of use
        """

        # init a line counter
        line_counter: int = 0
        try:
            term2id_redis: RedisConnection = await self.get_redis(0)
            id2instance_redis: RedisConnection = await self.get_redis(1)

            term2id_pipeline = term2id_redis.pipeline()
            id2instance_pipeline = id2instance_redis.pipeline()

            with open(compendium_filename, 'r', encoding="utf-8") as compendium:
                self.print_debug_msg(f'Processing {compendium_filename}...', True)

                # for each line in the file
                for line in compendium:
                    line_counter = line_counter + 1

                    # load the line into memory
                    instance: dict = json.loads(line)

                    # save the identifier
                    identifier: str = instance['id']['identifier']

                    # for each semantic type in the list
                    for semantic_type in instance['type']:
                        # save the semantic type in a set to avoid duplicates
                        self.semantic_types.add(semantic_type)

                        #  create a source prefix if it has not been encountered
                        if self.source_prefixes.get(semantic_type) is None:
                            self.source_prefixes[semantic_type] = {}

                        # go through each equivalent identifier in the data row
                        # each will be assigned the semantic type information
                        for equivalent_id in instance['equivalent_identifiers']:
                            # split the identifier to just get the data source out of the curie
                            source_prefix: str = equivalent_id['identifier'].split(':')[0]

                            # save the source prefix if no already there
                            if self.source_prefixes[semantic_type].get(source_prefix) is None:
                                self.source_prefixes[semantic_type][source_prefix] = 1
                            # else just increment the count for the semantic type/source
                            else:
                                self.source_prefixes[semantic_type][source_prefix] += 1

                            # equivalent_id might be an array, where the first element is
                            # the identifier, or it might just be a string. not worrying about that case yet.
                            equivalent_id = equivalent_id['identifier']
                            term2id_pipeline.set(equivalent_id, identifier)
                            term2id_pipeline.set(equivalent_id.upper(), identifier)

                        id2instance_pipeline.set(identifier, line)

                    if self._test_mode != 1 and line_counter % block_size == 0:
                        RedisConnection.execute_pipeline(term2id_pipeline)
                        RedisConnection.execute_pipeline(id2instance_pipeline)
                        self.print_debug_msg(f'{line_counter} {compendium_filename} lines processed.', True)

                if self._test_mode != 1:
                    RedisConnection.execute_pipeline(term2id_pipeline)
                    RedisConnection.execute_pipeline(id2instance_pipeline)
                    self.print_debug_msg(f'{line_counter} {compendium_filename} total lines processed.', True)

                print(f'Done loading {compendium_filename}...')
        except Exception as e:
            self.print_debug_msg(f'Exception thrown in load_compendium({compendium_filename}), line {line_counter}: {e}', True)
            return False

        # return to the caller
        return True

    def print_debug_msg(self, msg: str, force: bool = False):
        """
        Prints a debug message if enabled in the config file
        """
        if self._config['debug_messages'] == 1 or force:
            now: datetime = datetime.now()

            print(f'{now.strftime("%Y/%m/%d %H:%M:%S")} - {msg}')
