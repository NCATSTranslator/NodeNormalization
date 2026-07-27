"""FastAPI server."""
import asyncio
import os
import re
import logging, warnings

from pathlib import Path
from typing import List, Optional, Dict, Annotated

from fastapi.middleware.cors import CORSMiddleware
import requests
from requests.adapters import HTTPAdapter, Retry
import fastapi
from fastapi import FastAPI, HTTPException, Body, Query
import reasoner_pydantic
import yaml
from pydantic import BaseModel
from bmt import Toolkit
from starlette.responses import JSONResponse, PlainTextResponse

from .apidocs import get_app_info, construct_open_api_schema
from .config import SKILL_PATH
from .model import (
    SemanticTypes,
    CuriePivot,
    CurieList,
    SemanticTypesInput,
    ConflationList,
    SetIDResponse,
    SetIDQuery,
    NormalizedNode,
)
from .normalizer import get_normalized_nodes, get_curie_prefixes, normalize_message
from .set_id import generate_setid
from .redis_adapter import RedisConnectionFactory
from .util import LoggingUtil
from .examples import EXAMPLE_QUERY_DRUG_TREATS_ESSENTIAL_HYPERTENSION, EXAMPLE_NORMALIZED_NODES

logger = LoggingUtil.init_logging()

# Some metadata not implemented see
# https://github.com/tiangolo/fastapi/pull/1812
app = FastAPI(**get_app_info())

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BIOLINK_MODEL_TAG = os.environ.get("BIOLINK_MODEL_TAG", "master") # Note that this should be a tag from the Biolink Model repo, e.g. "master" or "v4.3.6".
BIOLINK_MODEL_URL = f"https://raw.githubusercontent.com/biolink/biolink-model/{BIOLINK_MODEL_TAG}/biolink-model.yaml"

async_query_tasks = set()


@app.on_event("startup")
async def startup_event():
    """
    Start up Redis connection
    """
    redis_config_file = Path(__file__).parent.parent / "redis_config.yaml"
    connection_factory = await RedisConnectionFactory.create_connection_pool(redis_config_file)
    app.state.eq_id_to_id_db = connection_factory.get_connection(connection_id="eq_id_to_id_db")
    app.state.id_to_eqids_db = connection_factory.get_connection(connection_id="id_to_eqids_db")
    app.state.id_to_type_db = connection_factory.get_connection(connection_id="id_to_type_db")
    app.state.curie_to_bl_type_db = connection_factory.get_connection(connection_id="curie_to_bl_type_db")
    app.state.info_content_db = connection_factory.get_connection(connection_id="info_content_db")
    app.state.gene_protein_db = connection_factory.get_connection(connection_id="gene_protein_db")
    app.state.chemical_drug_db = connection_factory.get_connection(connection_id="chemical_drug_db")
    app.state.toolkit = Toolkit(BIOLINK_MODEL_URL)
    logger.info(f"Initialized Biolink Model Toolkit ({app.state.toolkit}) from {BIOLINK_MODEL_URL} (based on tag: {BIOLINK_MODEL_TAG}).")
    app.state.ancestor_map = {}


@app.on_event("shutdown")
async def shutdown_event():
    """
    Shut down Redis connection
    """
    app.state.eq_id_to_id_db.close()
    await app.state.eq_id_to_id_db.wait_closed()
    app.state.id_to_eqids_db.close()
    await app.state.id_to_eqids_db.wait_closed()
    app.state.id_to_type_db.close()
    await app.state.id_to_type_db.wait_closed()
    app.state.curie_to_bl_type_db.close()
    await app.state.curie_to_bl_type_db.wait_closed()
    app.state.info_content_db.close()
    await app.state.info_content_db.wait_closed()
    app.state.gene_protein_db.close()
    await app.state.gene_protein_db.wait_closed()
    app.state.chemical_drug_db.close()
    await app.state.chemical_drug_db.wait_closed()


@app.get(
    "/status",
    summary="Status information on this NodeNorm instance",
    description="Returns information about this NodeNorm instance and the databases it is connected to, including "
                "the version of <a href=\"https://github.com/NCATSTranslator/Babel\">Babel</a> whose output is loaded "
                "into those databases (<code>babel_version</code>) and the Biolink Model version used to expand "
                "semantic types (<code>biolink_model</code>). The backend databases are written once and never "
                "updated in place, so <code>babel_version</code> identifies the data this instance will return until "
                "an operator loads a newer build. You can read more about this endpoint in the "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/API.md#status\">"
                "NodeNorm API documentation</a>.",
    response_description="Information about this NodeNorm instance and the databases it is connected to.",
)
async def status_get() -> Dict:
    """ Return status information about this NodeNorm instance as well as its databases. """
    return await status()


async def status() -> Dict:
    """ Return status information about this NodeNorm instance as well as its databases. """
    redis_config_file = Path(__file__).parent.parent / "redis_config.yaml"
    with open(redis_config_file, 'r') as rcfile:
        # Load rcfile as a YAML file using safe loading.
        redis_config = yaml.safe_load(rcfile)

    # Do we know the Babel version and version URL? It will be stored in an environmental variable if we do.
    babel_version = os.environ.get("BABEL_VERSION", "unknown")
    babel_version_url = os.environ.get("BABEL_VERSION_URL", "")

    # Can we figure out the NodeNorm version?
    nodenorm_version = "unknown"
    if "version" in app.openapi_schema["info"]:
        nodenorm_version = app.openapi_schema["info"]["version"]

    return {
        "status": "running",
        "version": nodenorm_version,
        "backend": "redis",
        "babel_version": babel_version,
        "babel_version_url": babel_version_url,
        "biolink_model": {
            "tag": BIOLINK_MODEL_TAG,
            "url": f"https://github.com/biolink/biolink-model/tree/{BIOLINK_MODEL_TAG}",
            "download_url": BIOLINK_MODEL_URL,
        },
        "databases": {
            "eq_id_to_id_db": {
                "dbname": "id-id",
                "count": await app.state.eq_id_to_id_db.dbsize(),
                "used_memory_rss_human": await app.state.eq_id_to_id_db.used_memory_rss_human(),
                "is_cluster": redis_config['eq_id_to_id_db'].get('is_cluster', 'false')
            },
            "id_to_eqids_db": {
                "dbname": "id-eq-id",
                "count": await app.state.id_to_eqids_db.dbsize(),
                "used_memory_rss_human": await app.state.id_to_eqids_db.used_memory_rss_human(),
                "is_cluster": redis_config['id_to_eqids_db'].get('is_cluster', 'false')
            },
            "id_to_type_db": {
                "dbname": "id-categories",
                "count": await app.state.id_to_type_db.dbsize(),
                "used_memory_rss_human": await app.state.id_to_type_db.used_memory_rss_human(),
                "is_cluster": redis_config['id_to_type_db'].get('is_cluster', 'false')
            },
            "curie_to_bl_type_db": {
                "dbname": "semantic-count",
                "count": await app.state.curie_to_bl_type_db.dbsize(),
                "used_memory_rss_human": await app.state.curie_to_bl_type_db.used_memory_rss_human(),
                "is_cluster": redis_config['curie_to_bl_type_db'].get('is_cluster', 'false')
            },
            "info_content_db": {
                "dbname": "info-content",
                "count": await app.state.info_content_db.dbsize(),
                "used_memory_rss_human": await app.state.info_content_db.used_memory_rss_human(),
                "is_cluster": redis_config['info_content_db'].get('is_cluster', 'false')
            },
            "gene_protein_db": {
                "dbname": "conflation-db",
                "count": await app.state.gene_protein_db.dbsize(),
                "used_memory_rss_human": await app.state.gene_protein_db.used_memory_rss_human(),
                "is_cluster": redis_config['gene_protein_db'].get('is_cluster', 'false')
            },
            "chemical_drug_db": {
                "dbname": "chemical-drug-db",
                "count": await app.state.chemical_drug_db.dbsize(),
                "used_memory_rss_human": await app.state.chemical_drug_db.used_memory_rss_human(),
                "is_cluster": redis_config['chemical_drug_db'].get('is_cluster', 'false')
            }
        },
    }


#: Matches the YAML frontmatter block at the very top of a SKILL.md.
_FRONTMATTER = re.compile(r"\A---\n.*?\n---\n", re.DOTALL)


@app.get(
    "/llms.txt",
    summary="Instructions for using this API from an AI agent or LLM",
    description="Returns a Markdown document explaining how to use this service to normalize "
                "identifiers: which endpoint to call, how to read the response, how to batch, and "
                "which conflation settings to choose. Intended to be fetched by an agent that has "
                "been pointed at this instance and needs to work out how to use it. The same "
                "document is maintained as a "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/tree/master/skills/nodenorm\">"
                "skill</a> in the NodeNorm repository.",
    response_class=PlainTextResponse,
    response_description="Markdown instructions for using this API.",
)
async def llms_txt() -> PlainTextResponse:
    """Serve the agent instructions from skills/nodenorm/SKILL.md."""
    try:
        skill = SKILL_PATH.read_text(encoding="utf-8")
    except OSError:
        # Most likely an image built without `COPY ./skills`; say so rather than 500ing.
        logger.warning(f"Could not read agent instructions from {SKILL_PATH}.")
        raise HTTPException(
            status_code=404,
            detail="Agent instructions are not available on this instance. They can be read at "
                   "https://github.com/NCATSTranslator/NodeNormalization/tree/master/skills/nodenorm",
        )

    # The frontmatter is Claude Code packaging metadata; it is noise in an llms.txt.
    return PlainTextResponse(_FRONTMATTER.sub("", skill, count=1).lstrip("\n"))


@app.post(
    "/query",
    summary="Normalizes every identifier in a TRAPI message",
    description="Normalizes the identifiers in the knowledge graph, query graph and results of a TRAPI message, "
                "returning the message with a merged knowledge graph and updated bindings. Each normalized node also "
                "gains the clique's information content as an attribute "
                "(<code>biolink:has_numeric_value</code> / <code>information_content</code>). "
                "<strong>Deprecated</strong>: this endpoint is no longer actively maintained and will be removed once "
                "the Workflow Runner stops using it "
                "(<a href=\"https://github.com/NCATSTranslator/NodeNormalization/pull/323\">PR #323</a>). New callers "
                "should extract the CURIEs they care about and use /get_normalized_nodes instead.",
    response_description="The submitted TRAPI message with all identifiers normalized.",
    response_model=reasoner_pydantic.Query,
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
    deprecated=True,
)
async def query(query: Annotated[reasoner_pydantic.Query, Body(openapi_examples={"Drugs that treat essential hypertension": {
    "summary": "A result from a query for drugs that treat essential hypertension.",
    "value": EXAMPLE_QUERY_DRUG_TREATS_ESSENTIAL_HYPERTENSION,
}})]) -> reasoner_pydantic.Query:
    """
    Normalizes a TRAPI compliant knowledge graph
    """
    query.message = await normalize_message(app, query.message)
    return query


@app.post(
    "/asyncquery",
    summary="Normalizes every identifier in a TRAPI message, returning the result to a callback URL",
    description="Identical to /query, except that it returns as soon as the work has been queued and POSTs the "
                "normalized TRAPI message to the <code>callback</code> URL given in the request body when it is "
                "ready (retrying a few times if the callback fails). "
                "<strong>Deprecated</strong>: this endpoint is no longer actively maintained and will be removed once "
                "the Workflow Runner stops using it "
                "(<a href=\"https://github.com/NCATSTranslator/NodeNormalization/pull/323\">PR #323</a>). New callers "
                "should extract the CURIEs they care about and use /get_normalized_nodes instead.",
    response_description="Confirmation that the query has been queued; the normalized message is sent to the callback URL.",
    deprecated=True,
)
async def async_query(async_query: reasoner_pydantic.AsyncQuery):
    """
    Normalizes a TRAPI compliant knowledge graph
    """
    # need a strong reference to task such that GC doesn't remove it mid execution...https://docs.python.org/3/library/asyncio-task.html#creating-tasks
    task = asyncio.create_task(async_query_task(async_query))
    async_query_tasks.add(task)
    task.add_done_callback(async_query_tasks.discard)

    return JSONResponse(content={"description": f"Query commenced. Will send result to {async_query.callback}"}, status_code=200)


async def async_query_task(async_query: reasoner_pydantic.AsyncQuery):
    async_query.message = await normalize_message(app, async_query.message)
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=[
            "HEAD",
            "GET",
            "PUT",
            "DELETE",
            "OPTIONS",
            "TRACE",
            "POST",
        ],
    )
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    logger.info(f"sending callback to: {async_query.callback}")

    post_response = session.post(
        url=async_query.callback,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data=async_query.json(),
    )
    logger.info(f"async_query post status code: {post_response.status_code}")


@app.get(
    "/get_allowed_conflations",
    summary="Return a list of named conflations.",
    description="Returns a list of allowed conflation options. Conflation allows cliques to be combined on-the-fly "
                "on the basis of two different criteria:"
                "<ol>"
                "<li><code>GeneProtein</code> conflation merges protein-coding genes with the proteins they encode. "
                "The gene(s) always appear first in the combined clique."
                "<li><code>DrugChemical</code> conflation merges chemicals based on their active ingredient. We "
                "attempt to ensure that the active ingredient appears before any formulations in the combined clique."
                "</ol>"
                "The returned strings can be used with the <code>conflation</code> parameter of /get_setid; "
                "/get_normalized_nodes has a separate boolean flag for each. You can read "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/Babel.md#conflation\">"
                "more about conflation</a> or "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/API.md#get_allowed_conflations\">"
                "more about this endpoint</a>.",
    response_description="The list of conflations supported by this NodeNorm instance.",
)
async def get_conflations() -> ConflationList:
    """
    Get implemented conflations
    """
    conflations = ConflationList(conflations=["GeneProtein", "DrugChemical"])

    return conflations


@app.get(
    "/get_normalized_nodes",
    summary="Get the equivalent identifiers and semantic types for the CURIEs entered.",
    description="Returns the equivalent identifiers and semantic types for the CURIEs entered. "
                "A CURIE that cannot be normalized is returned as a key with a <code>null</code> value rather than "
                "being left out of the response. "
                "You can optionally "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/Babel.md#conflation\">"
                "conflate identifiers</a> if needed: <code>conflate</code> merges genes with the proteins they encode "
                "(the gene comes first), and <code>drug_chemical_conflate</code> merges drugs with their active "
                "ingredient (the ingredient comes before any formulations). Conflated cliques are returned as a single "
                "flat list of equivalent identifiers, so use <code>individual_types</code> if you need to tell the "
                "members apart. "
                "You can read more about this endpoint in the "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/API.md#get_normalized_nodes\">NodeNorm API documentation</a>, "
                "and about where the identifiers, labels and information content values come from in "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/Babel.md\">"
                "Where NodeNorm's data comes from</a>.",
    responses={200: {
        "model": Dict[str, Optional[NormalizedNode]],
        "description": "A mapping from each CURIE queried to its normalized clique, or to null if it could not be normalized.",
        "content": {"application/json": {"example": EXAMPLE_NORMALIZED_NODES}},
    }},
)
async def get_normalized_node_handler(
    curie: List[str] = fastapi.Query(
        [],
        description="List of curies to normalize",
        example=["MESH:D014867", "NCIT:C34373", "NCBIGene:1756"],
        min_items=1,
    ),
    conflate: bool = fastapi.Query(True, description="Whether to apply gene/protein conflation"),
    drug_chemical_conflate: bool = fastapi.Query(True, description="Whether to apply drug/chemical conflation"),
    description: bool = fastapi.Query(False, description="Whether to return curie descriptions when possible"),
    individual_types: bool = fastapi.Query(False, description="Whether to return individual types for equivalent identifiers"),
    include_taxa: bool = fastapi.Query(True, description="Whether to return taxa for equivalent identifiers"),
):
    """
    Get value(s) for key(s) using redis MGET
    """
    # no_conflate = request.args.get('dontconflate',['GeneProtein'])
    normalized_nodes = await get_normalized_nodes(app, curie, conflate, drug_chemical_conflate,
                                                  include_descriptions=description,
                                                  include_individual_types=individual_types,
                                                  include_taxa=include_taxa,
                                                  )

    # If curie contains at least one entry, then the only way normalized_nodes could be blank
    # would be if an error occurred during processing.
    if not normalized_nodes:
        raise HTTPException(detail="Error occurred during processing.", status_code=500)

    return normalized_nodes


@app.post(
    "/get_normalized_nodes",
    summary="Get the equivalent identifiers and semantic types for the CURIEs entered.",
    description="Returns the equivalent identifiers and semantic types for the CURIEs entered. Identical to the GET "
                "method of this endpoint, but takes a <code>curies</code> list in a JSON body instead of repeated "
                "<code>curie</code> query parameters. "
                "<strong>Note that <code>drug_chemical_conflate</code> currently defaults to <code>false</code> here "
                "but <code>true</code> on the GET method</strong> "
                "(<a href=\"https://github.com/NCATSTranslator/NodeNormalization/issues/398\">#398</a>); set it "
                "explicitly if you care which conflations are applied. "
                "You can read more about this endpoint in the "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/API.md#get_normalized_nodes\">NodeNorm API documentation</a>.",
    responses={200: {
        "model": Dict[str, Optional[NormalizedNode]],
        "description": "A mapping from each CURIE queried to its normalized clique, or to null if it could not be normalized.",
        "content": {"application/json": {"example": EXAMPLE_NORMALIZED_NODES}},
    }},
)
async def get_normalized_node_handler_post(curies: CurieList):
    """
    Get value(s) for key(s) using redis MGET
    """
    normalized_nodes = await get_normalized_nodes(app, curies.curies, curies.conflate, curies.drug_chemical_conflate,
                                                  curies.description, include_individual_types=curies.individual_types,
                                                  include_taxa=curies.include_taxa,
                                                  )

    # If curies.curies contains at least one entry, then the only way normalized_nodes could be blank
    # would be if an error occurred during processing.
    if not normalized_nodes:
        raise HTTPException(detail="Error occurred during processing.", status_code=500)

    return normalized_nodes


@app.get(
    "/get_setid",
    response_model=SetIDResponse,
    summary="Normalize and deduplicate a set of identifiers and return a single hash that represents this set.",
    description="Returns the set ID for a given set of CURIEs. CURIEs that can be normalized are normalized (using "
                "the conflations provided); those that cannot are kept as-is. Duplicates are then removed, the "
                "remaining CURIEs are sorted, and a hash is generated from them. "
                "A set ID is an identifier that can be used to identify this set of CURIEs going forward. It is "
                "currently impossible to recreate a set of CURIEs from a set ID, but the same set of CURIEs given to "
                "the same version of Node Normalization will always return the same set ID. Note that a different "
                "Babel build may normalize the same CURIEs differently and so produce a different set ID — see "
                "<code>babel_version</code> in /status. "
                "You can read more about this endpoint in the "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/API.md#get_setid\">NodeNorm API documentation</a>.",
    response_description="The normalized CURIEs and the set ID calculated from them.",
)
async def get_setid(
    curie: List[str] = fastapi.Query(
        [],
        description="Set of curies to normalize",
        example=["MESH:D014867", "NCIT:C34373", "UNII:63M8RYN44N", "RUBBISH:1234"],
        min_items=1,
    ),
    conflation: List[str] = fastapi.Query(
        [],
        description="Set of conflations to apply",
        example=["GeneProtein", "DrugChemical"],
    )
) -> SetIDResponse:
    return await generate_setid(app, curie, conflation)


@app.post(
    "/get_setid",
    response_model=List[SetIDResponse],
    summary="Normalize and deduplicate a set of identifiers and return a single hash that represents this set.",
    description="Identical to the GET method of this endpoint, but calculates a set ID for several sets at once. "
                "Takes a list of sets, each with its own <code>curies</code> and optional <code>conflations</code>, "
                "and returns a list of results in the same order. "
                "You can read more about this endpoint in the "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/API.md#get_setid\">NodeNorm API documentation</a>.",
    response_description="One result per set submitted, in the order submitted.",
)
async def get_setid(
    sets: List[SetIDQuery] = fastapi.Body([],
                                                description="Set of identifiers to normalize",
                                                example=[
                                                    {
                                                        "curies": ["MESH:D014867", "NCIT:C34373"],
                                                    },
                                                    {
                                                        "curies": ["NCIT:C34373", "MESH:D014867", "UNII:63M8RYN44N", "RUBBISH:1234" ],
                                                        "conflations": ["GeneProtein", "DrugChemical"]
                                                    }
                                                ])
) -> List[SetIDResponse]:
    # I'm guessing there's some way of doing this so that the generate_setid()s run in parallel, but I don't know how.
    # I'll figure it out if needed.
    return [await generate_setid(app, q.curies, q.conflations) for q in sets]


@app.get(
    "/get_semantic_types",
    response_model=SemanticTypes,
    summary="Return a list of BioLink semantic types for which normalization has been attempted.",
    description="Returns a distinct, unordered set of the Biolink semantic types found in the "
                "<a href=\"https://github.com/NCATSTranslator/Babel\">Babel</a> compendia loaded into this instance. "
                "Returns 404 if no semantic types could be found, which usually means the databases have not been "
                "loaded. You can read more about this endpoint in the "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/API.md#get_semantic_types\">NodeNorm API documentation</a>.",
    response_description="The distinct Biolink semantic types present in this instance.",
)
async def get_semantic_types_handler() -> SemanticTypes:
    # look for all biolink semantic types
    types = await app.state.curie_to_bl_type_db.lrange("semantic_types", 0, -1, encoding="utf-8")

    # did we get any data
    if not types:
        raise HTTPException(detail="No semantic types discovered.", status_code=404)

    # get the distinct list of Biolink model types in the correct format
    # https://github.com/NCATSTranslator/NodeNormalization/issues/29
    ret_val = SemanticTypes(semantic_types={"types": list(set(types))})

    # return the data to the caller
    return ret_val


@app.get(
    "/get_curie_prefixes",
    response_model=Dict[str, CuriePivot],
    summary="Return the number of times each CURIE prefix appears in an equivalent identifier for a semantic type",
    description="Returns the CURIE prefixes and their hit counts for one or more semantic types. Omit "
                "<code>semantic_type</code> to get every semantic type. Counts are returned as strings. "
                "These counts are tallied when the "
                "<a href=\"https://github.com/NCATSTranslator/Babel\">Babel</a> compendia are loaded into this "
                "instance and are approximate — the load aggregates them concurrently without locking "
                "(<a href=\"https://github.com/NCATSTranslator/NodeNormalization/issues/380\">#380</a>), so prefer "
                "Babel's own reports if you need exact figures. You can read more about this endpoint in the "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/API.md#get_curie_prefixes\">NodeNorm API documentation</a>.",
    response_description="A mapping from each semantic type requested to its CURIE prefix counts.",
)
async def get_curie_prefixes_handler(
    semantic_type: Optional[List[str]] = fastapi.Query([], description="The semantic types to report on, e.g. "
                                                                       "biolink:ChemicalEntity, "
                                                                       "biolink:AnatomicalEntity. Omit for all types.")
) -> Dict[str, CuriePivot]:
    return await get_curie_prefixes(app, semantic_type)


@app.post(
    "/get_curie_prefixes",
    response_model=Dict[str, CuriePivot],
    summary="Return the number of times each CURIE prefix appears in an equivalent identifier for a semantic type",
    description="Identical to the GET method of this endpoint, but takes the list of semantic types in a JSON body. "
                "You can read more about this endpoint in the "
                "<a href=\"https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/API.md#get_curie_prefixes\">NodeNorm API documentation</a>.",
    response_description="A mapping from each semantic type requested to its CURIE prefix counts.",
)
async def get_curie_prefixes_handler(
    semantic_types: SemanticTypesInput,
) -> Dict[str, CuriePivot]:
    return await get_curie_prefixes(app, semantic_types.semantic_types)


# Override open api schema with custom schema
app.openapi_schema = construct_open_api_schema(app)

# Set up opentelemetry if enabled.
if os.environ.get('OTEL_ENABLED', 'false') == 'true':
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    # from opentelemetry.sdk.trace.export import ConsoleSpanExporter

    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    # httpx connections need to be open a little longer by the otel decorators
    # but some libs display warnings of resource being unclosed.
    # these supresses such warnings.
    logging.captureWarnings(capture=True)
    warnings.filterwarnings("ignore", category=ResourceWarning)

    otel_service_name = os.environ.get('SERVER_NAME', 'infores:sri-node-normalizer')
    assert otel_service_name and isinstance(otel_service_name, str)

    otlp_host = os.environ.get("JAEGER_HOST", "http://localhost/").rstrip('/')
    otlp_port = os.environ.get("JAEGER_PORT", "4317")
    otlp_endpoint = f'{otlp_host}:{otlp_port}'
    otlp_exporter = OTLPSpanExporter(endpoint=f'{otlp_endpoint}')
    processor = BatchSpanProcessor(otlp_exporter)
    # processor = BatchSpanProcessor(ConsoleSpanExporter())

    resource = Resource.create(attributes={
        SERVICE_NAME: os.environ.get("JAEGER_SERVICE_NAME", otel_service_name),
    })
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider, excluded_urls="docs,openapi.json")
    HTTPXClientInstrumentor().instrument()
