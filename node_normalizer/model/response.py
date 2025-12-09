"""
API Response Models not described in reasoner-pydantic
"""

from pydantic import BaseModel

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
    curie_prefix: Dict[str, str]


class ConflationList(BaseModel):
    conflations: List


class SetIDResponse(BaseModel):
    curies: List[str]
    conflations: List[str]
    error: Optional[str]
    normalized_curies: Optional[List[str]]
    normalized_string: Optional[str]
    setid: Optional[str]
    # base64: Optional[str]
    # base64zlib: Optional[str]
    # sha224hash: Optional[str]