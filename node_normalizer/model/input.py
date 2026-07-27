"""
API Input Models not described in reasoner-pydantic
"""

from pydantic import BaseModel, Field

from typing import List


class CurieList(BaseModel):
    """Curie list input model"""

    curies: List[str] = Field(
        ...,  # Ellipsis means field is required
        description="The CURIEs to normalize. Any CURIE that cannot be normalized is returned with a null value "
                    "rather than being left out of the response.",
        min_items=1
    )

    conflate: bool = Field(
        True,
        description="Whether to apply GeneProtein conflation, which merges a gene with the protein it encodes. "
                    "The gene always comes first in the combined clique."
    )

    description: bool = Field(
        False,
        description="Whether to return CURIE descriptions when possible. Descriptions currently come only from "
                    "UberGraph, so most identifiers have none."
    )

    drug_chemical_conflate: bool = Field(
        False,
        description="Whether to apply DrugChemical conflation, which merges a drug with its active ingredient. "
                    "The active ingredient comes before any formulations in the combined clique. "
                    "Note that this defaults to false here but true on the GET method of this endpoint "
                    "(https://github.com/NCATSTranslator/NodeNormalization/issues/398)."
    )

    individual_types: bool = Field(
        False,
        description="Whether to return the Biolink type of each equivalent identifier. Useful for telling apart "
                    "the members of a conflated clique, which are otherwise returned as one flat list."
    )

    include_taxa: bool = Field(
        True,
        description="Whether to return the taxa associated with each equivalent identifier, as NCBITaxon CURIEs."
    )

    class Config:
        schema_extra = {
            "example": {
                "curies": ['MESH:D014867', 'NCIT:C34373', 'NCBIGene:1756'],
                "conflate": True,
                "description": False,
                "drug_chemical_conflate": True,
                "individual_types": False,
                "include_taxa": True,
            }
        }


class SemanticTypesInput(BaseModel):
    """Semantic type input model"""

    semantic_types: List[str] = Field(
        ...,  # required field
        description="The Biolink semantic types to report on. Pass an empty list for every semantic type.",
    )

    class Config:
        schema_extra = {
            "example": {
                "semantic_types": ['biolink:ChemicalEntity', 'biolink:AnatomicalEntity']
            }
        }


class SetIDQuery(BaseModel):
    """ Query for a single SetID. Includes a set of CURIEs as well as a set of conflations to apply. """

    curies: List[str] = Field(
        ...,  # Ellipsis means field is required
        description="Set of curies to normalize",
        example=["MESH:D014867", "NCIT:C34373"],
    )

    conflations: List[str] = Field(
        [],
        description="Set of conflations to apply. See /get_allowed_conflations for the valid values.",
        example=["GeneProtein", "DrugChemical"],
    )
