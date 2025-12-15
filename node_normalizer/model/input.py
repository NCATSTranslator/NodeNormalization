"""
API Input Models not described in reasoner-pydantic
"""

from pydantic import BaseModel, Field

from typing import List, Dict


class CurieList(BaseModel):
    """Curie list input model"""

    curies: List[str] = Field(
        ...,  # Ellipsis means field is required
        title='List of CURIEs to normalize',
        min_items=1
    )

    conflate:bool = Field(
        True,
        title="Whether to apply gene/protein conflation"
    )

    description: bool = Field(
        False,
        title="Whether to return CURIE descriptions when possible"
    )

    drug_chemical_conflate: bool = Field(
        False,
        title="Whether to apply drug/chemical conflation"
    )

    individual_types: bool = Field(
        False,
        title="Whether to return individual types for equivalent identifiers"
    )

    include_taxa: bool = Field(
        True,
        title="Whether to return taxa for equivalent identifiers"
    )

    include_clique_leaders: bool = Field(
        default=False,
        title="Whether to return clique leaders for conflated identifiers"
    )

    class Config:
        schema_extra = {
            "example": {
                "curies": ['MESH:D014867', 'NCIT:C34373'],
                "conflate": True,
                "description": False,
                "drug_chemical_conflate": False,
            }
        }


class SemanticTypesInput(BaseModel):
    """Semantic type input model"""

    semantic_types: List[str] = Field(
        ...,  # required field
        title='list of semantic types',
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
        description="Set of conflations to apply",
        example=["GeneProtein", "DrugChemical"],
    )


class SetIDs(BaseModel):
    """ Query for Set IDs. You can provide a set of named CURIE sets, and we return a response for each one. """
    sets: Dict[str, SetIDQuery] = Field(
        description="A list of ",
    )