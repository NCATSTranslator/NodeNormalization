# NodeNorm API

The NodeNorm API includes many API endpoints that cover normalization of identifiers, TRAPI messages
and identifier sets, as well as endpoints to retrieve allowed conflations, semantic types and CURIE
prefixes. The [NodeNorm FastAPI Documentation](https://nodenormalization-sri.renci.org/docs) includes
information about the parameters for calling each endpoint, but this document will describe the
intended function of each endpoint, suggestions for use and descriptions of the JSON documents returned.

The identifiers, preferred names, types and information content values these endpoints return are all
computed upstream by [Babel](https://github.com/NCATSTranslator/Babel) and loaded into NodeNorm as a
fixed snapshot. If you are trying to work out *why* a particular clique looks the way it does, start
with [Where NodeNorm's data comes from](Babel.md).

## Identifier/Node Normalization

### `/get_normalized_nodes`

* Method: [GET](https://nodenormalization-sri.renci.org/docs#/default/get_normalized_node_handler_get_normalized_nodes_get)
  * Parameters:
    * `curie` (e.g. `curie=MESH:D014867&curie=NCIT:C34373`): The identifiers to normalize.
    * `conflate` (default `true`): Whether to apply GeneProtein conflation.
    * `drug_chemical_conflate` (default `true`): Whether to apply DrugChemical conflation.
    * `description` (default `false`): Whether to include descriptions for nodes that have descriptions.
    * `individual_types` (default `false`): When returning a conflated result, should Biolink types be
      returned for each individual identifier.
    * `include_taxa` (default `true`): Whether to include the taxa associated with each equivalent identifier.
* Method: [POST](https://nodenormalization-sri.renci.org/docs#/default/get_normalized_node_handler_post_get_normalized_nodes_post)
  * POST Body: A JSON object with the same parameters as the GET method, with a `curies` list instead of individual
    `curie` entries.
  * **Note that `drug_chemical_conflate` defaults to `false` on POST but `true` on GET**
    ([#398](https://github.com/NCATSTranslator/NodeNormalization/issues/398)). Until that is resolved,
    set it explicitly if you use the POST method and care which conflations are applied.

Conflation changes which identifier is preferred and merges several cliques into one flat list of
equivalent identifiers; see [Where NodeNorm's data comes from](Babel.md#conflation) for what each
conflation guarantees about ordering.

Example output:

```json
{
  "MESH:D014867": {
    "id": {
      "identifier": "CHEBI:15377",
      "label": "Water",
      "description": "An oxygen hydride consisting of an oxygen atom that is covalently bonded to two hydrogen atoms"
    },
    "equivalent_identifiers": [
      {
        "identifier": "CHEBI:15377",
        "label": "water",
        "description": "An oxygen hydride consisting of an oxygen atom that is covalently bonded to two hydrogen atoms",
        "type": "biolink:SmallMolecule"
      },
      {
        "identifier": "UNII:059QF0KO0R",
        "label": "WATER",
        "type": "biolink:SmallMolecule"
      },
      {
        "identifier": "PUBCHEM.COMPOUND:962",
        "label": "Water",
        "type": "biolink:SmallMolecule"
      },
      [...]
    ],
    "descriptions": [
      "An oxygen hydride consisting of an oxygen atom that is covalently bonded to two hydrogen atoms"
    ],
    "type": [
      "biolink:SmallMolecule",
      "biolink:MolecularEntity",
      "biolink:ChemicalEntity",
      "biolink:PhysicalEssence",
      "biolink:ChemicalOrDrugOrTreatment",
      "biolink:ChemicalEntityOrGeneOrGeneProduct",
      "biolink:ChemicalEntityOrProteinOrPolypeptide",
      "biolink:NamedThing",
      "biolink:PhysicalEssenceOrOccurrent"
    ],
    "information_content": 47.7
  }
}
```

* Output values: the output is a dictionary with queried CURIEs as the keys and with JSON objects
  as the values. **A CURIE that could not be normalized is present as a key with a `null` value**, rather than
  being left out — check for `null` values rather than for missing keys. Each non-null value contains the
  following keys:
  * `id`: A JSON object that provides the preferred identifier and labels for this clique.
    * `identifier`: The preferred CURIE for this clique. Every Biolink class includes a list of
      preferred prefixes (e.g.
      [valid ID prefixes for SmallMolecule](https://biolink.github.io/biolink-model/SmallMolecule/#valid-id-prefixes)),
      and this is used to choose the preferred CURIE for this clique.
    * `label`: The preferred label for this clique, as computed by Babel (its `preferred_name`). Note that this is
      not necessarily the label associated with the preferred CURIE: for some classes (such as chemicals), the best
      label is chosen in a different prefix order than the Biolink Model preferred prefix order, based on which
      sources tend to have the best labels. See
      [Where NodeNorm's data comes from](Babel.md#cliques-preferred-identifiers-and-labels).
      (Older database loads that predate Babel supplying `preferred_name` fall back to a legacy label-selection
      algorithm inside NodeNorm, which does not reproduce Babel's choice for conflated cliques.)
    * `description`: One of the descriptions for the identifiers within this clique.
  * `equivalent_identifiers`: a list of identifiers that are part of this clique given the conflation options.
    Each identifier includes an `identifier` (a CURIE), a `label` (which corresponds to the label of the CURIE as per
    its authoritative source), a `description` (currently only taken from UberGraph), and (if `individual_types` is set)
    the Biolink type of each identifier. This list is ordered in the Biolink Model's preferred prefix order for this class.
  * `descriptions`: a list of unique descriptions for the identifiers within this clique. This list is ordered in the same
    order as `equivalent_identifiers`. Only returned when `description` is set.
  * `taxa`: every taxon associated with any identifier in this clique, as NCBITaxon CURIEs. Only returned when
    `include_taxa` is set (the default), and omitted when no identifier in the clique has a known taxon. Individual
    taxa are also returned on each entry in `equivalent_identifiers`.
  * `type`: The list of Biolink classes for this clique, starting with the most specific type (in this example,
    `biolink:SmallMolecule`), and ending with any mixins that include this class.
  * `information_content`: the information content value between 0 and 100. This is calculated by Babel, which retrieves the
    [normalized information content value](https://github.com/INCATools/ubergraph/?tab=readme-ov-file#graph-organization)
    for each identifier that is present in UberGraph, and then stores the lowest information content value of
    any identifier in this clique for which UberGraph has a value. NodeNorm returns that stored value unchanged.
    According to UberGraph's documentation,
    the normalized information content value is "Precomputed information content score for each ontology class, based
    on the count of terms related to a given term via rdfs:subClassOf or any existential relation. The scores are
    xsd:decimal values scaled from 0 to 100 (e.g., a very specific term with no subclasses)." A value of 0.0 therefore
    means a broad, high-level concept and 100.0 a very specific one. Not every clique has one, and the field is
    omitted when it is unknown. See [Where NodeNorm's data comes from](Babel.md#information-content) — including the
    caveat that information content from Babel builds before `2025sep1` is unreliable.
  * Internally, conflation is represented as sets of cliques that should be combined when that conflation is turned on.
    This means that a conflated clique will be represented by a single list of equivalent identifiers, starting with the
    equivalent identifiers from the first clique, followed by the equivalent identifiers from the second clique, and so
    on. There is currently no way to retrieve the clique leaders (although
    [this is a requested feature](https://github.com/NCATSTranslator/NodeNormalization/issues/320)), but you can use the
    `individual_types` parameter to get a Biolink type for each identifier.

## Sets

### `/get_setid`

This endpoint is used to calculate a `set ID` for a set of CURIEs. CURIEs that can be normalized will
be normalized (using the conflation settings provided), and those that can't be will be left as is.
Duplicate normalized CURIEs will be removed, even if two distinct CURIEs were passed to this endpoint
but were normalized to the same CURIE. CURIEs will then be sorted in alphabetical order and a hash
generated as a set ID for that set of CURIEs. A set ID is therefore unique to a set of normalized CURIEs for the curies
passed in.

* Method: [GET](https://nodenormalization-sri.renci.org/docs#/default/get_setid_get_setid_get)
  * Parameters:
    * `curie` (example: `curie=MESH:D014867&curie=NCIT:C34373`): The CURIEs to normalize as a set.
    * `conflation` (optional, example: `conflation=GeneProtein&conflation=DrugChemical`): The conflations to apply.
* Method: [POST](https://nodenormalization-sri.renci.org/docs#/default/get_setid_get_setid_post)
  * POST Body: a JSON string representing a list of sets, where each set consists of:
    * `curies` (e.g. `"MESH:D014867", "NCIT:C34373": A list of CURIEs to normalize as a set.
    * `conflations` (optional, e.g. `["GeneProtein", "DrugChemical"]): A list of conflations to apply.

Example output: note that the GET method will return a single object, while the POST method will
return a list that corresponds to the list of sets sent to this endpoint for normalization.

```json
[
  {
    "curies": [
      "NCIT:C34373",
      "MESH:D014867",
      "UNII:63M8RYN44N",
      "RUBBISH:1234"
    ],
    "conflations": [
      "GeneProtein",
      "DrugChemical"
    ],
    "error": null,
    "normalized_curies": [
      "CHEBI:15377",
      "MONDO:0004976",
      "RUBBISH:1234"
    ],
    "normalized_string": "CHEBI:15377||MONDO:0004976||RUBBISH:1234",
    "setid": "uuid:771d3c09-9a8c-5c46-8b85-97f481a90d40"
  }
]
```

Output values:
* `curies`: The list of CURIEs passed to this endpoint for normalization.
* `conflations`: The list of conflations to apply as passed to this endpoint.
* `error`: Any error that occurred when normalizing this string. Note that a CURIE that cannot be normalized
  does not count as an error.
* `normalized_curies`: The list of unique normalized queries used to construct the setid.
* `setid`: The setid calculated for this set.

## Status

### [/status](https://nodenormalization-sri.renci.org/docs#/default/status_get_status_get)

This endpoint can be used to find out about the NodeNorm service and the underlying Redis databases.
It can be useful to confirm whether the databases are fully loaded and how much memory is being used.

* Methods: GET only
* No parameters.

Example output:
```json
{
  "status": "running",
  "version": "2.5.1",
  "backend": "redis",
  "babel_version": "2025mar31",
  "babel_version_url": "https://github.com/NCATSTranslator/Babel/blob/main/releases/2025mar31.md",
  "biolink_model": {
    "tag": "v4.2.6-rc2",
    "url": "https://github.com/biolink/biolink-model/tree/v4.2.6-rc2",
    "download_url": "https://raw.githubusercontent.com/biolink/biolink-model/v4.2.6-rc2/biolink-model.yaml"
  },
  "databases": {
    "eq_id_to_id_db": {
      "dbname": "id-id",
      "count": 677731045,
      "used_memory_rss_human": "68.83G",
      "is_cluster": false
    },
    [...]
  }
}
```

Output values:

* `status` (example: `running`): Whether or not the service is running.
* `version` (example: `2.5.1`): The version of NodeNorm itself.
* `backend` (example: `redis`): The storage backend in use.
* `babel_version` (example: `2025mar31`): The version of [Babel](https://github.com/NCATSTranslator/Babel) used to generate
  the cliques being presented. These are usually date-based versions indicating approximately when the Babel build was
  completed. Since the backend databases are written once and never updated in place, this identifies the data
  this instance will return until an operator loads a newer build — record it if you need reproducible results.
* `babel_version_url` (example: https://github.com/NCATSTranslator/Babel/blob/main/releases/2025mar31.md): A URL you
  can use to learn more about this version of Babel, and how it differs from previous and future versions.
* `biolink_model`: The version of the [Biolink Model](https://github.com/biolink/biolink-model) this instance uses to
  expand a clique's most specific type into the full list of ancestors returned in `type`. Set with the
  `BIOLINK_MODEL_TAG` environment variable.
  * `tag`: The tag, branch or commit name (example: `v4.2.6-rc2`).
  * `url`: The biolink-model repository at that tag.
  * `download_url`: The `biolink-model.yaml` actually loaded.
  * Note that the loader pins its own Biolink version separately (`biolink_version` in `config.json`), so that
    ancestors are computed against the model Babel built the data with. The two can differ.
* `databases`: A dictionary of Redis key-value databases used by this NodeNorm instance (currently: 7). Each database
  uses the internal name of this database as its key, along with the following information:
  * `dbname`: A second name used for this database.
  * `count`: The number of keys in this database.
  * `used_memory_rss_human`: the `used_memory_rss_human` value returned by this Redis database, described
    [in the Redis documentation](https://redis.io/docs/latest/commands/info/) as "Human readable representation of
    [Number of bytes that Redis allocated as seen by the operating system (a.k.a resident set size). This is the number reported by tools such as top(1) and
    ps(1)]."
  * `is_cluster`: Whether this database is being used as part of a cluster or as a single node database.

## Informational endpoints

### `/get_allowed_conflations`

Returns a list of the supported conflations.

* Method: [GET](https://nodenormalization-sri.renci.org/docs#/default/get_conflations_get_allowed_conflations_get)
* No parameters.

Example output:

```json
{
  "conflations": [
    "GeneProtein",
    "DrugChemical"
  ]
}
```

### `/get_semantic_types`

Returns a list of all the Biolink types/classes that this instance of NodeNorm has at least one identifier for.

* Method: [GET](https://nodenormalization-sri.renci.org/docs#/default/get_semantic_types_handler_get_semantic_types_get)

Example output:

```json
{
  "semantic_types": {
    "types": [
      "biolink:NucleicAcidEntity",
      "biolink:ActivityAndBehavior",
      "biolink:PhysicalEssence",
      "biolink:StudyPopulation",
      "biolink:PhysicalEssenceOrOccurrent",
      "biolink:GenomicEntity",
      "biolink:Protein",
      "biolink:Event",
      [...]
    ]
  }
}
```

### `/get_curie_prefixes`

Returns a list of CURIE prefixes for zero or more Biolink types, as well as the number of identifiers for each prefix.

These are generated when the Babel compendia are loaded into NodeNorm, and I haven't verified if they are
accurate — I'm more confident about the Babel reports, but I haven't checked them against each other.

* Method: [GET](https://nodenormalization-sri.renci.org/docs#/default/get_curie_prefixes_handler_get_curie_prefixes_get)
  * Parameters:
    * `semantic_type` (optional, e.g. `semantic_type=biolink:ChemicalEntity&semantic_type=biolink:AnatomicalEntity`)
    * Without a `semantic_type`, every semantic type is returned.
* Method: [POST](https://nodenormalization-sri.renci.org/docs#/default/get_curie_prefixes_handler_get_curie_prefixes_post)
  * POST Body: `{"semantic_types": ["biolink:ChemicalEntity", "biolink:AnatomicalEntity"]}`

Example output:
```json
{
  "biolink:ChemicalEntity": {
    "curie_prefix": {
      "PUBCHEM.COMPOUND": "119397095",
      "INCHIKEY": "115661650",
      "CHEMBL.COMPOUND": "2496527",
      "CAS": "4029002",
      "CHEBI": "200507",
      "HMDB": "217920",
      "MESH": "258506",
      "UMLS": "668019",
      "KEGG.COMPOUND": "16035",
      "UNII": "134411",
      "DRUGBANK": "16108",
      "GTOPDB": "12953",
      "DrugCentral": "4995",
      "RXCUI": "124852"
    }
  },
  "biolink:AnatomicalEntity": {
    "curie_prefix": {
      "UMLS": "159496",
      "FMA": "98632",
      "MESH": "1992",
      "UBERON": "14513",
      "NCIT": "10223",
      "EMAPA": "968",
      "ZFA": "607",
      "FBbt": "117",
      "WBbt": "18",
      "CL": "2865",
      "SNOMEDCT": "1421",
      "GO": "4022"
    }
  }
}
```

## TRAPI Normalization (deprecated)

These two endpoints normalize an entire TRAPI message rather than a list of CURIEs. Both are
deprecated ([PR #323](https://github.com/NCATSTranslator/NodeNormalization/pull/323)): they are no
longer actively maintained, and will be removed once the Workflow Runner stops using them. New
callers should extract the CURIEs they care about and use `/get_normalized_nodes` instead.

Note that `/query` also attaches each clique's information content to the normalized node as a TRAPI
attribute (`attribute_type_id: biolink:has_numeric_value`, `original_attribute_name:
information_content`), rounded to one decimal place.

### `/query`

Normalizes all the identifiers in a [TRAPI](https://github.com/NCATSTranslator/ReasonerAPI) message.

* Method: [POST](https://nodenormalization-sri.renci.org/docs#/default/query_query_post)

### `/asyncquery`

Identical to `/query`, but returns a URL that the requester can use to poll for the response
rather than waiting for the request to complete.

* Method: [POST](https://nodenormalization-sri.renci.org/docs#/default/async_query_asyncquery_post)
