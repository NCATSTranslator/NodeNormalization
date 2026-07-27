"""
API Response Models not described in reasoner-pydantic

Note that the normalization models below (`EquivalentIdentifier`, `NormalizedNodeId`,
`NormalizedNode`) are used to *document* the response of /get_normalized_nodes, via the
`responses=` argument of the route decorators, rather than as a `response_model=`. They are
deliberately not enforced: `create_node()` builds these dictionaries by hand and omits keys that
don't apply, and making FastAPI filter its output through a model risks silently dropping a field
the model forgot. Keep them in sync with `create_node()` in `normalizer.py`.
"""

from pydantic import BaseModel, Field

from typing import Dict, List, Optional


class SemanticTypes(BaseModel):
    semantic_types: Dict[str, List]

    class Config:
        schema_extra = {
            "example": {
                "semantic_types": {
                    "types": [
                        "biolink:CellularComponent",
                        "biolink:NamedThing",
                        "etc."
                    ]
                }
            }
        }


class CuriePivot(BaseModel):
    curie_prefix: Dict[str, str] = Field(
        ...,
        description="A mapping from CURIE prefix to the number of times it appears in an equivalent identifier "
                    "for this semantic type. Note that the counts are strings, not numbers.",
    )

    class Config:
        schema_extra = {
            "example": {
                "curie_prefix": {
                    "PUBCHEM.COMPOUND": "119397095",
                    "INCHIKEY": "115661650",
                    "CHEBI": "200507",
                }
            }
        }


class ConflationList(BaseModel):
    conflations: List[str] = Field(
        ...,
        description="The conflations supported by this NodeNorm instance.",
    )

    class Config:
        schema_extra = {"example": {"conflations": ["GeneProtein", "DrugChemical"]}}


class EquivalentIdentifier(BaseModel):
    """A single identifier belonging to a normalized clique."""

    identifier: str = Field(..., description="The CURIE for this identifier.")
    label: Optional[str] = Field(
        None,
        description="The label for this identifier as given by its authoritative source. Omitted if that source "
                    "provides no label.",
    )
    description: Optional[str] = Field(
        None,
        description="A description of this identifier, currently always from UberGraph. Only returned when the "
                    "`description` parameter is set, and omitted when no description is known.",
    )
    taxa: Optional[List[str]] = Field(
        None,
        description="The taxa this identifier is associated with, as NCBITaxon CURIEs. Only returned when the "
                    "`include_taxa` parameter is set, and omitted when no taxa are known.",
    )
    type: Optional[str] = Field(
        None,
        description="The most specific Biolink type of this individual identifier. Only returned when the "
                    "`individual_types` parameter is set. Useful for telling apart the members of a conflated "
                    "clique, e.g. which identifiers are the gene and which the protein.",
    )


class NormalizedNodeId(BaseModel):
    """The preferred identifier and label for a normalized clique."""

    identifier: str = Field(
        ...,
        description="The preferred CURIE for this clique: the first identifier in the Biolink Model's preferred "
                    "prefix order for this clique's type. With a conflation applied, this is the preferred "
                    "identifier of the first clique being conflated (for GeneProtein, always the gene).",
    )
    label: Optional[str] = Field(
        None,
        description="The preferred label for this clique, as computed by Babel. Note that this is not necessarily "
                    "the label of the preferred identifier — for chemicals in particular, a label may be taken from "
                    "a different identifier. Omitted if nothing in the clique has a label.",
    )
    description: Optional[str] = Field(
        None,
        description="One of the descriptions for the identifiers in this clique. Only returned when the "
                    "`description` parameter is set.",
    )


class NormalizedNode(BaseModel):
    """A normalized clique: everything NodeNorm knows about one concept."""

    id: NormalizedNodeId = Field(..., description="The preferred identifier and label for this clique.")
    equivalent_identifiers: List[EquivalentIdentifier] = Field(
        ...,
        description="Every identifier in this clique, in the Biolink Model's preferred prefix order for this type. "
                    "When a conflation is applied this is a single flat list — all the identifiers of the first "
                    "clique, then those of the second, and so on — with no marker for where one clique ends and the "
                    "next begins.",
    )
    type: List[str] = Field(
        ...,
        description="The Biolink classes for this clique, starting with the most specific type and ending with any "
                    "mixins. Note that `biolink:Entity` is deliberately excluded.",
    )
    descriptions: Optional[List[str]] = Field(
        None,
        description="The unique descriptions of the identifiers in this clique, in the same order as "
                    "`equivalent_identifiers`. Only returned when the `description` parameter is set.",
    )
    taxa: Optional[List[str]] = Field(
        None,
        description="Every taxon associated with any identifier in this clique, as NCBITaxon CURIEs. Only returned "
                    "when the `include_taxa` parameter is set.",
    )
    information_content: Optional[float] = Field(
        None,
        description="How specific this concept is, from 0.0 (a broad, high-level term with many subclasses) to "
                    "100.0 (a very specific term with none). Taken from Ubergraph's normalizedInformationContent; "
                    "where several identifiers in the clique have a value, the lowest is reported. Omitted when no "
                    "identifier in the clique has one.",
    )


class SetIDResponse(BaseModel):
    curies: List[str] = Field(..., description="The CURIEs submitted for normalization, as submitted.")
    conflations: List[str] = Field(..., description="The conflations applied, as submitted.")
    error: Optional[str] = Field(
        None,
        description="Any error that occurred while normalizing this set. Note that a CURIE that simply cannot be "
                    "normalized is not an error.",
    )
    normalized_curies: Optional[List[str]] = Field(
        None,
        description="The unique, sorted CURIEs used to construct the set ID. CURIEs that could not be normalized "
                    "are included unchanged.",
    )
    normalized_string: Optional[str] = Field(
        None,
        description="The normalized CURIEs joined with `||`; this is the string the set ID is calculated from.",
    )
    setid: Optional[str] = Field(
        None,
        description="The set ID: a UUID (prefixed with `uuid:`) derived from `normalized_string`. The same set of "
                    "CURIEs given to the same version of NodeNorm always produces the same set ID, but a different "
                    "Babel build may normalize them differently and so produce a different one.",
    )
    # base64: Optional[str]
    # base64zlib: Optional[str]
    # sha224hash: Optional[str]