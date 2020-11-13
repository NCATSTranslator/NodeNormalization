"""
API Input Models not described in reasoner-pydantic
"""

from pydantic import BaseModel, Field

from typing import List


class CurieList(BaseModel):
    """Curie list input model"""

    curies: List[str] = Field(
        ...,  # Ellipsis means field is required
        title='list of nodes formatted as curies'
    )

    class Config:
        schema_extra = {
            "example": {
                "curies": ['MESH:D014867', 'NCIT:C34373']
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
                "semantic_types": ['biolink:ChemicalSubstance', 'biolink:AnatomicalEntity']
            }
        }
