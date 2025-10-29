import collections
import itertools
from pathlib import Path

import json as builtin_json
import time

import orjson as json
import logging
import os
import uuid
import traceback
from typing import List, Dict, Optional, Any, Set, Tuple, Union
from uuid import UUID
from bmt.utils import format_element as bmt_format

from fastapi import FastAPI
from reasoner_pydantic import KnowledgeGraph, Message, QueryGraph, Result, CURIE, Attribute

from .util import (
    LoggingUtil,
    uniquify_list,
    BIOLINK_NAMED_THING,
    get_numerical_curie_suffix,
)

# logger = LoggingUtil.init_logging(__name__, level=logging.INFO, format='medium', logFilePath=os.path.dirname(__file__), logFileLevel=logging.INFO)
logger = LoggingUtil.init_logging()

# Load configuration from config.json.
with open(Path(__file__).parents[1] / "config.json", "r") as configf:
    config = builtin_json.load(configf)


def sort_identifiers_with_boosted_prefixes(identifiers, prefixes):
    """
    Given a list of identifiers (with `identifier` and `label` keys), sort them using
    the following rules:
    - Any identifier that has a prefix in prefixes is sorted based on its order in prefixes.
    - Any identifier that does not have a prefix in prefixes is left in place.

    Copied from https://github.com/TranslatorSRI/Babel/blob/0c3f3aed1bb1647f1ca101ba905dc241797fdfc9/src/babel_utils.py#L315-L333

    :param identifiers: A list of identifiers to sort. This is a list of dictionaries
        containing `identifier` and `label` keys, and possible others that we ignore.
    :param prefixes: A list of prefixes, in the order in which they should be boosted.
        We assume that CURIEs match these prefixes if they are in the form `{prefix}:...`.
    :return: The list of identifiers sorted as described above.
    """

    # Thanks to JetBrains AI.
    return sorted(
        identifiers,
        key=lambda identifier: prefixes.index(identifier['i'].split(':', 1)[0]) if identifier['i'].split(':', 1)[0] in prefixes else len(prefixes)
    )


def get_ancestors(app, input_type):
    if input_type in app.state.ancestor_map:
        return app.state.ancestor_map[input_type]
    a = app.state.toolkit.get_ancestors(input_type)
    ancs = [bmt_format(ai) for ai in a]
    #if input_type is in ancs, remove it
    if input_type in ancs:
        ancs.remove(input_type)
    ancs = [input_type] + ancs
    app.state.ancestor_map[input_type] = ancs
    return ancs


async def normalize_message(app: FastAPI, message: Message) -> Message:
    """
    Given a TRAPI message, updates the message to include a
    normalized qgraph, kgraph, and results
    """
    ret = Message()

    logger.debug(f"message.query_graph is None: {message.query_graph is None}")
    if message.query_graph is not None:
        merged_qgraph = await normalize_qgraph(app, message.query_graph)
        ret.query_graph = merged_qgraph
    logger.debug(f"Merged Qgraph: {merged_qgraph}")

    logger.debug(f"message.knowledge_graph is None: {message.knowledge_graph is None}")
    if message.knowledge_graph is not None:
        merged_kgraph, node_id_map, edge_id_map = await normalize_kgraph(app, message.knowledge_graph)
        ret.knowledge_graph = merged_kgraph
    logger.debug(f"Merged Kgraph: {merged_kgraph}")
    logger.debug(f"node_id_map: {node_id_map}")
    logger.debug(f"edge_id_map: {edge_id_map}")

    logger.debug(f"message.results is None: {message.results is None}")
    if message.results is not None:
        merged_results = await normalize_results(app, message.results, node_id_map, edge_id_map)
        ret.results = merged_results
    logger.debug(f"Merged Results: {merged_results}")

    return ret


async def normalize_results(app,
                            results: List[Result],
                            node_id_map: Dict[str, str],
                            edge_id_map: Dict[str, str]
                            ) -> List[Result]:
    """
    Given a TRAPI result creates a normalized result object
    """

    merged_results: List[Result] = []
    result_seen = set()

    for result in results:
        merged_result = {
            'node_bindings': {},
            'analyses': []
        }

        node_binding_seen = set()

        for node_code, node_bindings in result.node_bindings.items():
            merged_node_bindings = []
            for n_bind in node_bindings:
                merged_binding = n_bind.dict()
                # merged_binding['id'] = node_id_map[n_bind.id.__root__]
                merged_binding['id'] = node_id_map[n_bind.id]

                # get the information content value
                ic_attrib = await get_info_content_attribute(app, merged_binding['id'])

                # did we get a good attribute dict
                if ic_attrib:
                    if 'attributes' in merged_binding and merged_binding['attributes'] is not None:
                        merged_binding['attributes'].append(ic_attrib)
                    else:
                        merged_binding['attributes'] = [ic_attrib]

                node_binding_information = [
                    "atts" if k == 'attributes'
                    else (k, tuple(v)) if isinstance(v, list)
                    else (k, v)
                    for k, v in merged_binding.items()
                ]

                # if there are attributes in the node binding
                if 'attributes' in merged_binding:
                    # storage for the pydantic Attributes
                    attribs = []

                    # the items in list of attributes must be of type Attribute
                    # in order to reuse hash method
                    if merged_binding['attributes'] is not None:
                        for attrib in merged_binding['attributes']:
                            new_attrib = Attribute.parse_obj(attrib)

                            # add the new Attribute to the list
                            attribs.append(new_attrib)

                        # call to get the hash
                        atty_hash = _hash_attributes(attribs)
                        node_binding_information.append(atty_hash)
                node_binding_hash = frozenset(node_binding_information)

                if node_binding_hash in node_binding_seen:
                    continue
                else:
                    node_binding_seen.add(node_binding_hash)
                    merged_node_bindings.append(merged_binding)

            merged_result['node_bindings'][node_code] = merged_node_bindings

        edge_binding_seen = set()
        for analysis in result.analyses:
            for edge_code, edge_bindings in analysis.edge_bindings.items():
                merged_edge_bindings = []
                for e_bind in edge_bindings:
                    merged_binding = e_bind.dict()
                    merged_binding['id'] = edge_id_map[e_bind.id]

                    edge_binding_hash = frozenset([
                        (k, freeze(v))
                        for k, v in merged_binding.items()
                    ])

                    if edge_binding_hash in edge_binding_seen:
                        continue
                    else:
                        edge_binding_seen.add(edge_binding_hash)
                        merged_edge_bindings.append(merged_binding)

                analysis.edge_bindings[edge_code] = merged_edge_bindings
                merged_result['analyses'].append(analysis.dict())

        try:
            # This used to have some list comprehension based on types.  But in TRAPI 1.1 the list/dicts get pretty deep.
            # This is simpler, and the sort_keys argument makes sure we get a constant result.
            #THis is for regular json
            #hashed_result = json.dumps(merged_result, sort_keys=True)
            #This is for orjson
            hashed_result = json.dumps(merged_result, option=json.OPT_SORT_KEYS)

        except Exception as e:  # TODO determine exception(s) to catch
            exception_str = "".join(traceback.format_exc())
            logger.error(f'Exception: {exception_str}')
            hashed_result = False

        if hashed_result is not False:
            if hashed_result in result_seen:
                continue
            else:
                result_seen.add(hashed_result)

        merged_results.append(Result.parse_obj(merged_result))

    return merged_results


def freeze(d):
    if isinstance(d, dict):
        return frozenset((key, freeze(value)) for key, value in d.items())
    elif isinstance(d, list):
        return tuple(freeze(value) for value in d)
    return d


async def normalize_qgraph(app: FastAPI, qgraph: QueryGraph) -> QueryGraph:
    """
    Given a TRAPI query graph creates a normalized query graph
    with primary curies replacing nonprimary ones
    """
    merged_nodes = {}

    node_code_map: Dict[str, Union[str, List]] = {}

    for node_code, node in qgraph.nodes.items():
        try:
            merged_nodes[node_code] = node.dict()

            # as of TRAPI 1.1, node.id must be none or a list.
            # node.id can be none, a string, or a list
            if not node.ids:
                # do nothing
                continue
            else:
                if not isinstance(node.ids.__root__, list):
                    raise Exception("node.ids must be a list")
                primary_ids = set()
                for nr in node.ids.__root__:
                    equivalent_curies = await get_equivalent_curies(app, nr)
                    if equivalent_curies[nr]:
                        primary_ids.add(equivalent_curies[nr]['id']['identifier'])
                    else:
                        primary_ids.add(nr)
                merged_nodes[node_code]['ids'] = list(primary_ids)
                node_code_map[node_code] = list(primary_ids)
        except Exception as e:
            exception_str = "".join(traceback.format_exc())
            logger.error(f'Exception: {exception_str}')

    return QueryGraph.parse_obj({
        'nodes': merged_nodes,
        'edges': qgraph.edges
    })


async def normalize_kgraph(
        app: FastAPI,
        kgraph: KnowledgeGraph
) -> Tuple[KnowledgeGraph, Dict[str, str], Dict[str, str]]:
    """
    Given a TRAPI knowledge graph creates a merged graph
    by iterating over each node, getting the primary id,
    and merging nodes and edges (as needed)

    Returns a tuple with the first element being the merged
    knowledge graph, and the second element being a map
    of the original node id to the updated node id, and the third
    being an edge id map
    """

    merged_kgraph: Dict = {
        'nodes': {},
        'edges': {}
    }

    node_id_map: Dict[str, str] = {}
    edge_id_map: Dict[str, str] = {}

    # Map for each node id (curie) and its primary id
    node_id_map: Dict[str, str] = {}

    # Map for each edge id and its primary id
    edge_id_map: Dict[str, str] = {}

    # Map for each edge to its s,p,r,o signature
    primary_edges: Dict[Tuple[str, str, Optional[str], str, Union[UUID, int]], str] = {}

    # cache for primary node ids
    primary_nodes_seen = set()

    # Count of times a node has been merged for attribute merging
    node_merge_count: Dict[str, int] = {}

    # cache for nodes
    nodes_seen = set()

    # cache for subject, predicate, relation, object, attribute hash tuples
    edges_seen: Set[Tuple[str, str, str, str, Union[UUID, int]]] = set()

    for node_id, node in kgraph.nodes.items():
        if node_id in nodes_seen:
            continue

        nodes_seen.add(node_id)
        node_id_map[node_id] = node_id  # expected to overridden by primary id

        merged_node = node.dict()

        equivalent_curies = await get_equivalent_curies(app, node_id)

        if equivalent_curies[node_id]:
            primary_id = equivalent_curies[node_id]['id']['identifier']
            node_id_map[node_id] = primary_id

            if primary_id in primary_nodes_seen:
                merged_node = _merge_node_attributes(
                    node_a=merged_kgraph['nodes'][primary_id],
                    node_b=node.dict(),
                    merged_count=node_merge_count[primary_id]
                )
                merged_kgraph['nodes'][primary_id] = merged_node
                node_merge_count[primary_id] += 1
                continue
            else:
                node_merge_count[primary_id] = 0

            primary_nodes_seen.add(primary_id)

            if 'label' in equivalent_curies[node_id]['id']:
                primary_label = equivalent_curies[node_id]['id']['label']
            elif 'name' in merged_node:
                primary_label = merged_node['name']
            else:
                primary_label = ''

            merged_node['name'] = primary_label

            # Even if there's already a same_as attribute we add another
            # since it is coming from a new source
            if 'equivalent_identifiers' in equivalent_curies[node_id]:
                same_as_attribute = {
                    'attribute_type_id': 'biolink:same_as',
                    'value': [
                        node['identifier']
                        for node in equivalent_curies[node_id]['equivalent_identifiers']
                    ],
                    'original_attribute_name': 'equivalent_identifiers',
                    "value_type_id": "EDAM:data_0006",

                    # TODO, should we add the app version as the source
                    # or perhaps the babel/redis cache version
                    # This will make unit testing a little more tricky
                    # see https://stackoverflow.com/q/57624731

                    # 'source': f'{app.title} {app.version}',
                }
                if 'attributes' in merged_node and merged_node['attributes']:
                    merged_node['attributes'].append(same_as_attribute)
                else:
                    merged_node['attributes'] = [same_as_attribute]

            if 'type' in equivalent_curies[node_id]:
                if type(equivalent_curies[node_id]['type']) is list:
                    merged_node['categories'] = equivalent_curies[node_id]['type']
                else:
                    merged_node['categories'] = [equivalent_curies[node_id]['type']]

            # get the information content value
            ic_attrib = await get_info_content_attribute(app, node_id)

            # did we get a good attribute dict
            if ic_attrib:
                # add the attribute to the node
                merged_node['attributes'].append(ic_attrib)

            merged_kgraph['nodes'][primary_id] = merged_node
        else:
            merged_kgraph['nodes'][node_id] = merged_node

    for edge_id, edge in kgraph.edges.items():
        # Accessing __root__ directly seems wrong,
        # https://github.com/samuelcolvin/pydantic/issues/730
        # could also do str(edge.subject)
        if edge.subject in node_id_map:
            primary_subject = node_id_map[edge.subject]
        else:
            # should we throw a validation error here?
            primary_subject = edge.subject

        if edge.object in node_id_map:
            primary_object = node_id_map[edge.object]
        else:
            primary_object = edge.object

        hashed_attributes = _hash_attributes(edge.attributes)

        if hashed_attributes is False:
            # we couldn't hash the attribute so assume unique
            hashed_attributes = uuid.uuid4()

        triple = (
            primary_subject,
            edge.predicate,
            primary_object,
            hashed_attributes
        )

        if triple in edges_seen:
            edge_id_map[edge_id] = primary_edges[triple]
            continue
        else:
            primary_edges[triple] = edge_id
            edge_id_map[edge_id] = edge_id

        edges_seen.add(triple)
        merged_edge = edge.dict()

        merged_edge['subject'] = primary_subject
        merged_edge['object'] = primary_object
        merged_kgraph['edges'][edge_id] = merged_edge

    return KnowledgeGraph.parse_obj(merged_kgraph), node_id_map, edge_id_map


async def get_equivalent_curies(
        app: FastAPI,
        curie: Union[str, CURIE]
) -> Dict:
    """
    Get primary id and equivalent curies using redis GET

    Returns a dict with the structure
    {
      ${curie}:
        {'identifier': 'foo', 'label': bar},
        'equivalent_identifiers': [{'identifier': 'foo', 'label': bar}, ...],
        'type': ['named_thing']
    }
    """

    # set default return in case curie not found
    default_return = {curie: None}

    try:

        # Get the equivalent list primary key identifier
        value = await get_normalized_nodes(app, [curie], True, True)

        # did we get a valid response
        if value is None:
            # no, so return the default
            return default_return

    except Exception as e:
        exception_str = "".join(traceback.format_exc())
        logger.error(f'Exception: {exception_str}')
        return default_return

    # return the curie normalization data
    return value


async def get_info_content(
        app: FastAPI,
        canonical_nonan: List) -> dict:
    """
    Gets the information content value for the node id

    :param app:
    :param canonical_nonan:
    :return:
    """
    # Check to see if canonical_nonan is not empty.
    if not canonical_nonan:
        return {}

    # call redis and get the value
    info_contents = await app.state.info_content_db.mget(*canonical_nonan, encoding='utf8')

    # get this into a list
    info_contents = [round(float(ic_ids), 1) if ic_ids is not None else None for ic_ids in info_contents]

    # zip into an array of dicts
    info_contents = dict(zip(canonical_nonan, info_contents))

    # return the value to the caller
    return info_contents


async def get_eqids_and_types(
        app: FastAPI,
        canonical_nonan: List) -> Tuple[List, List]:
    if len(canonical_nonan) == 0:
        return [], []
    batch_size = int(os.environ.get("EQ_BATCH_SIZE", 2500))
    eqids = []
    for i in range(0, len(canonical_nonan), batch_size):
        eqids += await app.state.id_to_eqids_db.mget(*canonical_nonan[i:i + batch_size], encoding='utf-8')
    eqids = [json.loads(value) if value is not None else [None] for value in eqids]
    types = await app.state.id_to_type_db.mget(*canonical_nonan, encoding='utf-8')
    types_with_ancestors = []
    for index, typ in enumerate(types):
        if not typ:
            logging.error(f"No type information found for '{canonical_nonan[index]}' with eqids: {eqids[index]}, "
                          f"replacing with {BIOLINK_NAMED_THING}")
            types_with_ancestors.append([BIOLINK_NAMED_THING])
        else:
            types_with_ancestors.append(get_ancestors(app, typ))

        # Every equivalent identifier here has the same type.
        for eqid in eqids[index]:
            eqid.update({'types': [typ]})

    return eqids, types_with_ancestors


async def get_normalized_nodes(
        app: FastAPI,
        curies: List[Union[CURIE, str]],
        conflate_gene_protein: bool,
        conflate_chemical_drug: bool,
        include_descriptions: bool = False,
        include_individual_types: bool = True,
        include_taxa: bool = True,
) -> Dict[str, Optional[str]]:
    """
    Get value(s) for key(s) using redis MGET
    """

    # Time how long this query takes.
    start_time = time.time_ns()

    # malkovich malkovich
    curies = [
        curie.__root__ if isinstance(curie, CURIE) else curie
        for curie in curies
    ]
    normal_nodes = {}

    # TODO: Add an option that lets one choose which conflations to do, and get the details of those conflations from the configs.
    # conflation_types = {"biolink:Gene", "biolink:Protein"}
    # conflation_redis = 5

    upper_curies = [c.upper() for c in curies]
    canonical_ids = await app.state.eq_id_to_id_db.mget(*upper_curies, encoding='utf-8')
    canonical_nonan = [canonical_id for canonical_id in canonical_ids if canonical_id is not None]
    info_contents = {}

    # did we get some canonical ids
    if canonical_nonan:
        # get the information content values
        info_contents = await get_info_content(app, canonical_nonan)

        # Get the equivalent_ids and types
        eqids, types = await get_eqids_and_types(app, canonical_nonan)

        # are we looking for conflated values
        if conflate_gene_protein or conflate_chemical_drug:
            other_ids = []

            if conflate_gene_protein:
                other_ids.extend(await app.state.gene_protein_db.mget(*canonical_nonan, encoding='utf8'))

            # logger.error(f"After conflate_gene_protein: {other_ids}")

            if conflate_chemical_drug:
                other_ids.extend(await app.state.chemical_drug_db.mget(*canonical_nonan, encoding='utf8'))

            # logger.error(f"After conflate_chemical_drug: {other_ids}")

            # if there are other ids, then we want to rebuild eqids and types.  That's because even though we have them,
            # they're not necessarily first.  For instance if what came in and got canonicalized was a protein id
            # and we want gene first, then we're relying on the order of the other_ids to put it back in the right place.
            other_ids = [json.loads(oids) if oids else [] for oids in other_ids]

            # Until we added conflate_chemical_drug, canonical_nonan and other_ids would always have the same
            # length, so we could figure out mappings from one to the other just by doing:
            #   dereference_others = dict(zip(canonical_nonan, other_ids))
            # Now that we have (potentially multiple) results to associate with each identifier, we need
            # something a bit more sophisticated.
            # - We use a defaultdict with set so that we can deduplicate identifiers here.
            # - We use itertools.cycle() because len(canonical_nonan) will be <= len(other_ids), but we can be sure
            #   that each conflation method will return a list of identifiers (e.g. if gene_conflation returns nothing
            #   for two queries, other_ids = [[], [], ...]. By cycling through canonical_nonan, we can assign each
            #   result to the correct query for each conflation method.
            dereference_others = collections.defaultdict(list)
            for canon, oids in zip(itertools.cycle(canonical_nonan), other_ids):
                dereference_others[canon].extend(oids)

            all_other_ids = sum(other_ids, [])
            eqids2, types2 = await get_eqids_and_types(app, all_other_ids)

            # logger.error(f"other_ids = {other_ids}")
            # logger.error(f"dereference_others = {dereference_others}")
            # logger.error(f"all_other_ids = {all_other_ids}")

            final_eqids = []
            final_types = []

            deref_others_eqs = dict(zip(all_other_ids, eqids2))
            deref_others_typ = dict(zip(all_other_ids, types2))

            zipped = zip(canonical_nonan, eqids, types)

            for canonical_id, e, t in zipped:
                # here's where we replace the eqids, types
                if len(dereference_others[canonical_id]) > 0:
                    e = []
                    t = []

                for other in dereference_others[canonical_id]:
                    # logging.debug(f"e = {e}, other = {other}, deref_others_eqs = {deref_others_eqs}")
                    e += deref_others_eqs[other]
                    t += deref_others_typ[other]

                final_eqids.append(e)
                final_types.append(uniquify_list(t))

            dereference_ids = dict(zip(canonical_nonan, final_eqids))
            dereference_types = dict(zip(canonical_nonan, final_types))
        else:
            dereference_ids = dict(zip(canonical_nonan, eqids))
            dereference_types = dict(zip(canonical_nonan, types))
    else:
        dereference_ids = dict()
        dereference_types = dict()

    # output the final result
    normal_nodes = {
        input_curie: await create_node(app, canonical_id, dereference_ids, dereference_types, info_contents,
                                       include_descriptions=include_descriptions,
                                       include_individual_types=include_individual_types,
                                       include_taxa=include_taxa,
                                       conflations={
                                           'GeneProtein': conflate_gene_protein,
                                           'DrugChemical': conflate_chemical_drug,
                                       })
        for input_curie, canonical_id in zip(curies, canonical_ids)
    }

    end_time = time.time_ns()
    logger.info(f"Normalized {len(curies)} nodes in {(end_time - start_time)/1_000_000:.2f} ms with arguments " +
                f"(curies={curies}, conflate_gene_protein={conflate_gene_protein}, conflate_chemical_drug={conflate_chemical_drug}, " +
                f"include_descriptions={include_descriptions}, include_individual_types={include_individual_types})")

    return normal_nodes


async def get_info_content_attribute(app, canonical_nonan) -> dict:
    """
    gets the information content value from the redis cache

    :param app:
    :param canonical_nonan:
    :return:
    """
    # get the information content value
    ic_val = await app.state.info_content_db.get(canonical_nonan, encoding='utf8')

    # did we get a good value
    if ic_val is not None:
        # load up a dict with the attribute data and create a trapi attribute object
        new_attrib = dict(attribute_type_id="biolink:has_numeric_value", original_attribute_name="information_content", value_type_id="EDAM:data_0006",
                          value=round(float(ic_val), 1))
    else:
        # else return nothing
        new_attrib = None

    # return to the caller
    return new_attrib


async def create_node(app, canonical_id, equivalent_ids, types, info_contents, include_descriptions=True,
                      include_individual_types=False, include_taxa=False, conflations=None):
    """Construct the output format given the compressed redis data"""
    # It's possible that we didn't find a canonical_id
    if canonical_id is None:
        return None

    # If no conflation information was provided, assume it's empty.
    if conflations is None:
        conflations = {}

    # If we have 'None' in the equivalent IDs, skip it so we don't confuse things further down the line.
    if None in equivalent_ids[canonical_id]:
        logging.warning(f"Skipping None in canonical ID {canonical_id} among eqids: {equivalent_ids}")
        equivalent_ids[canonical_id] = [x for x in equivalent_ids[canonical_id] if x is not None]
        if not equivalent_ids[canonical_id]:
            logging.warning(f"No non-None values found for ID {canonical_id} among filtered eqids: {equivalent_ids}")
            return None

    # If we have 'None' in the canonical types, something went horribly wrong (specifically: we couldn't
    # find the type information for all the eqids for this clique). Return None.
    if None in types[canonical_id]:
        logging.error(f"No types found for canonical ID {canonical_id} among types: {types}")
        return None

    # OK, now we should have id's in the format [ {"i": "MONDO:12312", "l": "Scrofula"}, {},...]
    eids = equivalent_ids[canonical_id]

    # As per https://github.com/TranslatorSRI/Babel/issues/158, we select the first label from any
    # identifier _except_ where one of the types is in preferred_name_boost_prefixes, in which case
    # we prefer the prefixes listed there.
    #
    # This should perfectly replicate NameRes labels for non-conflated cliques, but it WON'T perfectly
    # match conflated cliques. To do that, we need to run the preferred label algorithm on ONLY the labels
    # for the FIRST clique of the conflated cliques with labels.
    any_conflation = any(conflations.values())
    if not any_conflation:
        # No conflation. We just use the identifiers we've been given.
        identifiers_with_labels = eids
    else:
        # We have a conflation going on! To replicate Babel's behavior, we need to run the algorithem
        # on the list of labels corresponding to the first
        # So we need to run the algorithm on the first set of identifiers that have any
        # label whatsoever.
        identifiers_with_labels = []
        curies_already_checked = set()
        for identifier in eids:
            curie = identifier.get('i', '')
            if curie in curies_already_checked:
                continue
            results, _ = await get_eqids_and_types(app, [curie])

            identifiers_with_labels = results[0]
            labels = map(lambda ident: ident.get('l', ''), identifiers_with_labels)
            if any(map(lambda l: l != '', labels)):
                break

            # Since we didn't get any matches here, add it to the list of CURIEs already checked so
            # we don't make redundant queries to the database.
            curies_already_checked.update(set(map(lambda x: x.get('i', ''), identifiers_with_labels)))

        # We might get here without any labels, which is fine. At least we tried.

    # At this point:
    #   - eids will be the full list of all identifiers and labels in this clique.
    #   - identifiers_with_labels is the list of identifiers and labels for the first subclique that has at least
    #     one label.

    # Note that types[canonical_id] goes from most specific to least specific, so we
    # need to reverse it in order to apply preferred_name_boost_prefixes for the most
    # specific type.
    possible_labels = []
    for typ in types[canonical_id][::-1]:
        if typ in config['preferred_name_boost_prefixes']:
            # This is the most specific matching type, so we use this and then break.
            possible_labels = list(map(lambda ident: ident.get('l', ''),
                                  sort_identifiers_with_boosted_prefixes(
                                      identifiers_with_labels,
                                      config['preferred_name_boost_prefixes'][typ]
                                  )))

            # Add in all the other labels -- we'd still like to consider them, but at a lower priority.
            for eid in identifiers_with_labels:
                label = eid.get('l', '')
                if label not in possible_labels:
                    possible_labels.append(label)

            # Since this is the most specific matching type, we shouldn't do other (presumably higher-level)
            # categories: so let's break here.
            break

    # Step 1.2. If we didn't have a preferred_name_boost_prefixes, just use the identifiers in their
    # Biolink prefix order.
    if not possible_labels:
        possible_labels = map(lambda eid: eid.get('l', ''), identifiers_with_labels)

    # Step 2. Filter out any suspicious labels.
    filtered_possible_labels = [l for l in possible_labels if
        l and                               # Ignore blank or empty names.
        not l.startswith('CHEMBL')          # Some CHEMBL names are just the identifier again.
        ]

    # Step 3. Filter out labels longer than config['demote_labels_longer_than'], but only if there is at
    # least one label shorter than this limit.
    labels_shorter_than_limit = [l for l in filtered_possible_labels if l and len(l) <= config['demote_labels_longer_than']]
    if labels_shorter_than_limit:
        filtered_possible_labels = labels_shorter_than_limit

    # Note that the id will be from the equivalent ids, not the canonical_id.  This is to handle conflation
    if len(filtered_possible_labels) > 0:
        node = {"id": {"identifier": eids[0]['i'], "label": filtered_possible_labels[0]}}
    else:
        # Sometimes, nothing has a label :(
        node = {"id": {"identifier": eids[0]['i']}}

    # Now that we've determined a label for this clique, we should never use identifiers_with_labels, possible_labels,
    # or filtered_possible_labels after this point.

    # now need to reformat the identifier keys.  It could be cleaner but we have to worry about if there is a label
    first_description = None
    node_taxa = set()
    node["equivalent_identifiers"] = []
    for eqid in eids:
        eq_item = {"identifier": eqid["i"]}
        if "l" in eqid and eqid["l"]:
            eq_item["label"] = eqid["l"]
        # if descriptions is enabled and exist add them to each eq_id entry
        if include_descriptions and "d" in eqid and len(eqid["d"]) > 0:
            eq_item["description"] = eqid["d"][0]
            if not first_description:
                first_description = eq_item["description"]
        # if include_taxa is enabled and we have taxa on this node, add them to every eq_id entry
        if include_taxa and "t" in eqid and eqid["t"]:
            eq_item["taxa"] = eqid["t"]
            node_taxa.update(eqid["t"])
        # if individual types have been requested, add them too.
        if include_individual_types and 'types' in eqid:
            eq_item["type"] = eqid['types'][-1]
        node["equivalent_identifiers"].append(eq_item)

    if include_descriptions:
        node["description"] = first_description

    if include_taxa:
        node["taxa"] = sorted(node_taxa, key=get_numerical_curie_suffix)

    # We need to remove `biolink:Entity` from the types returned.
    # (See explanation at https://github.com/TranslatorSRI/NodeNormalization/issues/173)
    if 'biolink:Entity' in types[canonical_id]:
        types[canonical_id].remove('biolink:Entity')

    node['type'] = types[canonical_id]

    # add the info content to the node if we got one
    if info_contents[canonical_id] is not None:
        node['information_content'] = info_contents[canonical_id]

    return node


async def get_curie_prefixes(
        app: FastAPI,
        semantic_types: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Get pivot table of semantic type x curie prefix
    """
    ret_val: dict = {}  # storage for the returned data

    # was an arg passed in
    if semantic_types:
        for item in semantic_types:
            # get the curies for this type
            curies = await app.state.curie_to_bl_type_db.get(item, encoding='utf-8')

            # did we get any data
            if not curies:
                curies = '{' + f'"{item}"' + ': "Not found"}'

            curies = json.loads(curies)

            # set the return data
            ret_val[item] = {'curie_prefix': curies}
    else:
        types = await app.state.curie_to_bl_type_db.lrange('semantic_types', 0, -1, encoding='utf-8')

        for item in types:
            # get the curies for this type
            curies = await app.state.curie_to_bl_type_db.get(item, encoding='utf-8')

            # did we get any data
            if not curies:
                curies = '{' + f'"{item}"' + ': "Not found"}'

            curies = json.loads(curies)

            # set the return data
            ret_val[item] = {'curie_prefix': curies}

    return ret_val


def _merge_node_attributes(node_a: Dict, node_b, merged_count: int) -> Dict:
    """
    :param node_a: the primary node
    :param node_b: the node to be merged
    :param merged_count: the number of nodes merged into node_a **upon entering this fx**
    """

    if not ('attributes' in node_b and node_b['attributes']):
        return node_a

    if merged_count == 0:
        if 'attributes' in node_a and node_a['attributes']:
            new_attribute_list = []
            for attribute in node_a['attributes']:
                new_dict = {}
                for k, v in attribute.items():
                    new_dict[f"{k}.1"] = v
                new_attribute_list.append(new_dict)

            node_a['attributes'] = new_attribute_list

    # Need to DRY this off
    b_attr_id = merged_count + 2
    if 'attributes' in node_b and node_b['attributes']:
        new_attribute_list = []
        for attribute in node_b['attributes']:
            new_dict = {}
            for k, v in attribute.items():
                new_dict[f"{k}.{b_attr_id}"] = v
            new_attribute_list.append(new_dict)

        node_a['attributes'] = node_a['attributes'] + new_attribute_list

    return node_a


def _hash_attributes(attributes: List[Attribute] = None) -> Union[int, bool]:
    """
    Attempt to make an attribute list hashable by converting it to a
    tuple of tuples

    Using the python builtin hash https://docs.python.org/3.5/library/functions.html#hash
    Which can technically return zero, so downstream code should explicitly check
    for a False value instead of falsy values

    The tricky thing here is that attribute.value is an Any type, so we do some type
    checking to see if it's hashable
    """
    new_attributes = []

    try:
        if not attributes:
            return hash(attributes)

        for attribute in attributes:
            hashed_value = attribute.value
            if attribute.value:
                if isinstance(attribute.value, list):
                    # TODO list of lists?
                    hashed_value = tuple(attribute.value)
                elif isinstance(attribute.value, dict):
                    hashed_value = tuple(
                        (k, tuple(v))
                        if isinstance(v, list)
                        else (k, v)
                        for k, v in attribute.value.items())

            new_attribute = (
                attribute.attribute_type_id,
                hashed_value,
                attribute.original_attribute_name,
                attribute.value_url,
                attribute.attribute_source,
                attribute.value_type_id if attribute.value_type_id is not None else '',
                attribute.attribute_source
            )
            new_attributes.append(new_attribute)

        return hash(frozenset(new_attributes))
    except Exception as e:
        exception_str = "".join(traceback.format_exc())
        logger.error(f'Exception: {exception_str}')
        return False
