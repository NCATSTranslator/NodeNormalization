"""
This file contains some examples that we can use to populate the OpenAPI documentation.
"""

EXAMPLE_QUERY_DRUG_TREATS_ESSENTIAL_HYPERTENSION = {
    "message": {
        "query_graph": {
            "nodes": {
                "n0": {
                    "categories": ["biolink:Drug"],
                    "is_set": False,
                    "constraints": [],
                },
                "n1": {
                    "ids": ["MONDO:0001134"],
                    "categories": ["biolink:Disease"],
                    "is_set": False,
                    "constraints": [],
                },
            },
            "edges": {
                "e01": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:treats"],
                    "attribute_constraints": [],
                    "qualifier_constraints": [],
                }
            },
        },
        "knowledge_graph": {
            "nodes": {
                "DRUGBANK:DB00966": {
                    "categories": ["biolink:Drug"],
                    "name": "Telmisartan",
                },
                "DRUGBANK:DB00678": {
                    "categories": ["biolink:Drug"],
                    "name": "Losartan",
                },
            },
            "edges": {
                "e3": {
                    "subject": "DRUGBANK:DB00966",
                    "object": "MONDO:0001134",
                    "predicate": "biolink:treats",
                    "sources": [
                        {
                            "resource_id": "infores:openpredict",
                            "resource_role": "primary_knowledge_source",
                        },
                        {
                            "resource_id": "infores:cohd",
                            "resource_role": "supporting_data_source",
                        },
                    ],
                    "attributes": [
                        {
                            "description": "model_id",
                            "attribute_type_id": "EDAM:data_1048",
                            "value": "openpredict_baseline",
                        },
                        {
                            "attribute_type_id": "biolink:agent_type",
                            "value": "computational_model",
                            "attribute_source": "infores:openpredict",
                        },
                        {
                            "attribute_type_id": "biolink:knowledge_level",
                            "value": "prediction",
                            "attribute_source": "infores:openpredict",
                        },
                    ],
                },
                "e4": {
                    "subject": "DRUGBANK:DB00678",
                    "object": "MONDO:0001134",
                    "predicate": "biolink:treats",
                    "sources": [
                        {
                            "resource_id": "infores:openpredict",
                            "resource_role": "primary_knowledge_source",
                        },
                        {
                            "resource_id": "infores:cohd",
                            "resource_role": "supporting_data_source",
                        },
                    ],
                    "attributes": [
                        {
                            "description": "model_id",
                            "attribute_type_id": "EDAM:data_1048",
                            "value": "openpredict_baseline",
                        },
                        {
                            "attribute_type_id": "biolink:agent_type",
                            "value": "computational_model",
                            "attribute_source": "infores:openpredict",
                        },
                        {
                            "attribute_type_id": "biolink:knowledge_level",
                            "value": "prediction",
                            "attribute_source": "infores:openpredict",
                        },
                    ],
                },
            },
        },
        "results": [
            {
                "node_bindings": {
                    "n0": [{"id": "DRUGBANK:DB00966"}],
                    "n1": [{"id": "MONDO:0001134"}],
                },
                "analyses": [
                    {
                        "resource_id": "infores:openpredict",
                        "score": "0.7155011411093821",
                        "scoring_method": "Model confidence between 0 and 1",
                        "edge_bindings": {"e01": [{"id": "e3"}]},
                    }
                ],
            },
            {
                "node_bindings": {
                    "n0": [{"id": "DRUGBANK:DB00678"}],
                    "n1": [{"id": "MONDO:0001134"}],
                },
                "analyses": [
                    {
                        "resource_id": "infores:openpredict",
                        "score": "0.682246949249408",
                        "scoring_method": "Model confidence between 0 and 1",
                        "edge_bindings": {"e01": [{"id": "e4"}]},
                    }
                ],
            },
        ],
    }
}


# An example response from /get_normalized_nodes, used to document the endpoint. Shows a CURIE that
# normalizes (MESH:D014867 -> CHEBI:15377) and one that does not (RUBBISH:1234 -> null).
EXAMPLE_NORMALIZED_NODES = {
    "MESH:D014867": {
        "id": {
            "identifier": "CHEBI:15377",
            "label": "Water",
        },
        "equivalent_identifiers": [
            {"identifier": "CHEBI:15377", "label": "water", "type": "biolink:SmallMolecule"},
            {"identifier": "UNII:059QF0KO0R", "label": "WATER", "type": "biolink:SmallMolecule"},
            {"identifier": "PUBCHEM.COMPOUND:962", "label": "Water", "type": "biolink:SmallMolecule"},
            {"identifier": "MESH:D014867", "label": "Water", "type": "biolink:SmallMolecule"},
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
            "biolink:PhysicalEssenceOrOccurrent",
        ],
        "information_content": 47.7,
    },
    "RUBBISH:1234": None,
}
