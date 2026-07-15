"""Test node_normalizer normalizer.py"""
import json
import pytest

from fastapi.encoders import jsonable_encoder

from copy import deepcopy

from deepdiff import DeepDiff
from reasoner_pydantic import KnowledgeGraph, Attribute, CURIE
from pathlib import Path
from unittest.mock import Mock, patch

# Need to add to sources root to avoid linter warnings
from .helpers.redis_mocks import mock_get_equivalent_curies, mock_get_ic
from types import SimpleNamespace

from node_normalizer.normalizer import (
    normalize_kgraph,
    _hash_attributes,
    _merge_node_attributes,
    _clique_props,
    create_node,
    get_normalized_nodes,
)


class _MgetRedis:
    """Minimal stand-in for an aioredis connection: only mget, keyed dict lookup."""

    def __init__(self, data):
        self.data = data

    async def mget(self, *keys, **kwargs):
        return [self.data.get(k) for k in keys]


def find_diffs(x, y, parent_key=None, exclude_keys=[], epsilon_keys=[]):
    """
    Take the diff of JSON-like dictionaries
    """
    EPSILON = 0.5
    rho = 1 - EPSILON

    if x == y:
        return None

    if parent_key in epsilon_keys:
        xfl, yfl = float_or_none(x), float_or_none(y)
        if xfl and yfl and xfl * yfl >= 0 and rho * xfl <= yfl and rho * yfl <= xfl:
            return None

    if type(x) != type(y) or type(x) not in [list, dict]:
        return x, y

    if type(x) == dict:
        d = {}
        for k in x.keys() ^ y.keys():
            if k in exclude_keys:
                continue
            if k in x:
                d[k] = (deepcopy(x[k]), None)
            else:
                d[k] = (None, deepcopy(y[k]))

        for k in x.keys() & y.keys():
            if k in exclude_keys:
                continue

            next_d = find_diffs(
                x[k],
                y[k],
                parent_key=k,
                exclude_keys=exclude_keys,
                epsilon_keys=epsilon_keys,
            )
            if next_d is None:
                continue

            d[k] = next_d

        return d if d else None

    # assume a list:
    d = [None] * max(len(x), len(y))
    flipped = False
    if len(x) > len(y):
        flipped = True
        x, y = y, x

    for i, x_val in enumerate(x):
        d[i] = (
            find_diffs(
                y[i],
                x_val,
                parent_key=i,
                exclude_keys=exclude_keys,
                epsilon_keys=epsilon_keys,
            )
            if flipped
            else find_diffs(
                x_val,
                y[i],
                parent_key=i,
                exclude_keys=exclude_keys,
                epsilon_keys=epsilon_keys,
            )
        )

    for i in range(len(x), len(y)):
        d[i] = (y[i], None) if flipped else (None, y[i])

    return None if all(map(lambda x: x is None, d)) else d


# We need this helper function as well:
def float_or_none(x):
    try:
        return float(x)
    except ValueError:
        return None


premerged_graph = Path(__file__).parent / "resources" / "premerged_kgraph.json"
postmerged_graph = Path(__file__).parent / "resources" / "postmerged_kgraph.json"


class TestNormalizer:
    @pytest.mark.asyncio
    @patch(
        "node_normalizer.normalizer.get_equivalent_curies",
        Mock(side_effect=mock_get_equivalent_curies),
    )
    @patch(
        "node_normalizer.normalizer.get_info_content_attribute",
        Mock(side_effect=mock_get_ic),
    )
    async def test_kg_normalize(self):
        app = None
        with open(premerged_graph, "r") as pre:
            premerged_data = KnowledgeGraph.parse_obj(json.load(pre))

        with open(postmerged_graph, "r") as post:
            postmerged_from_file = json.load(post)

        postmerged_from_api, nmap, emap = await normalize_kgraph(app, premerged_data)

        nodes: dict = {}
        for code, node in postmerged_from_api.nodes.items():
            nodes.update({code: node.dict()})

        edges: dict = {}
        for code, edge in postmerged_from_api.edges.items():
            edges.update({code: jsonable_encoder(edge.dict(exclude_unset=True, exclude_none=True))})

        post = {"nodes": nodes, "edges": edges}

        # get the difference
        diffs = DeepDiff(post, postmerged_from_file, ignore_order=True)
        # diffs = find_diffs(post, postmerged_from_file)

        # no diffs, no problem
        # assert diffs is None
        assert len(diffs) == 0

    def test_hashable_attribute(self):
        # value is a scalar
        # attribute_type_id: CURIE = Field(..., title="type")
        # value: Any = Field(..., title="value")
        # value_type_id: Optional[CURIE] = Field(None, title="value_type_id")
        # original_attribute_name: Optional[str] = Field(None, nullable=True)
        # value_url: Optional[str] = Field(None, nullable=True)
        # attribute_source: Optional[str] = Field(None, nullable=True)

        hashable_attribute = Attribute(
            attribute_type_id=CURIE("foo:bar"),
            value=3,
            original_attribute_name="test",
            attribute_source="test_source",
        )
        assert _hash_attributes([hashable_attribute]) is not False

        # value is None
        hashable_attribute = Attribute(
            attribute_type_id=CURIE("foo:bar"),
            value=None,
            original_attribute_name="test",
            attribute_source="test_source",
        )
        assert _hash_attributes([hashable_attribute]) is not False

        # value is a list
        hashable_attribute = Attribute(
            attribute_type_id=CURIE("foo:bar"),
            value=[1, 2, 3],
            original_attribute_name="test",
            attribute_source="test_source",
        )

        assert _hash_attributes([hashable_attribute]) is not False

        # value is a dict of scalars/lists
        hashable_attribute = Attribute(
            attribute_type_id=CURIE("foo:bar"),
            value={1: 2, 3: [4, 5]},
            original_attribute_name="test",
            attribute_source="test_source",
        )

        assert _hash_attributes([hashable_attribute]) is not False

        # None check
        assert _hash_attributes(None) is not False
        assert _hash_attributes(None) == _hash_attributes(None)

        attribute1 = Attribute(attribute_type_id=CURIE("foo:bar"), value=1)
        attribute2 = Attribute(attribute_type_id=CURIE("foo:bar"), value=2)
        # Sanity checks
        assert _hash_attributes([attribute1]) == _hash_attributes([attribute1])
        assert _hash_attributes([attribute1]) != _hash_attributes([attribute2])

    # this is now hashable, so as written, it does not return False
    # def test_unhashable_attribute(self):
    #     # value is a nested dict
    #     hashable_attribute = Attribute(
    #         attribute_type_id=CURIE("foo:bar"),
    #         value={1: {2: 3}},
    #         original_attribute_name="test",
    #         attribute_source="test_source",
    #     )
    #     assert _hash_attributes([hashable_attribute]) is False

    def test_clique_props(self):
        # New JSON format round-trips.
        assert _clique_props('{"preferred_name": "Foo", "ic": 100.0}') == {"preferred_name": "Foo", "ic": 100.0}
        # Legacy bare float/int/string all normalize to {"ic": ...}.
        assert _clique_props("100.0") == {"ic": 100.0}
        assert _clique_props("100") == {"ic": 100}
        # Missing/absent value -> empty dict.
        assert _clique_props(None) == {}

    @pytest.mark.asyncio
    async def test_create_node_uses_stored_preferred_name(self):
        # When a Babel-computed preferred_name is present, create_node uses it verbatim
        # for the clique label -- even a >demote_labels_longer_than (15) name -- and does
        # NOT run the fallback algorithm. app=None proves no Redis is touched on this path.
        canonical = "MONDO:0011996"
        eids = {canonical: [{"i": canonical, "l": "raw list label"}]}
        node = await create_node(
            app=None,
            canonical_id=canonical,
            equivalent_ids=eids,
            types={canonical: ["biolink:Disease"]},
            info_contents={canonical: None},
            preferred_names={canonical: "Infant Acute Undifferentiated Leukemia"},
            conflations={"GeneProtein": False, "DrugChemical": False},
        )
        assert node["id"] == {"identifier": canonical, "label": "Infant Acute Undifferentiated Leukemia"}

    @pytest.mark.asyncio
    async def test_create_node_empty_preferred_name_falls_back(self):
        # An empty stored preferred_name ("" -- Babel emitted none) must fall through to
        # the legacy algorithm, which picks the clique's own label. No conflation => no Redis.
        canonical = "MONDO:0011996"
        eids = {canonical: [{"i": canonical, "l": "Leukemia"}]}
        node = await create_node(
            app=None,
            canonical_id=canonical,
            equivalent_ids=eids,
            types={canonical: ["biolink:Disease"]},
            info_contents={canonical: None},
            preferred_names={canonical: ""},
            conflations={"GeneProtein": False, "DrugChemical": False},
        )
        assert node["id"] == {"identifier": canonical, "label": "Leukemia"}

    @pytest.mark.asyncio
    async def test_conflation_uses_leading_identifier_preferred_name(self):
        # Under gene/protein conflation, the clique's label must be the *leading*
        # (gene) sub-clique's preferred_name, and the node id its leading identifier
        # -- not the queried protein's. gene_protein_db lists the members gene-first,
        # so get_normalized_nodes merges them gene-first (dereference_ids[c][0]) and
        # keys the db-5 preferred_name lookup on that leading id.
        gene, protein = "NCBIGene:1017", "UniProtKB:P24941"
        app = SimpleNamespace(state=SimpleNamespace(
            eq_id_to_id_db=_MgetRedis({protein.upper(): protein}),
            id_to_eqids_db=_MgetRedis({
                protein: json.dumps([{"i": protein, "l": "CDK2 protein raw label"}]),
                gene: json.dumps([{"i": gene, "l": "CDK2 gene raw label"}]),
            }),
            id_to_type_db=_MgetRedis({protein: "biolink:Protein", gene: "biolink:Gene"}),
            # db 5, keyed by each clique's canonical id: gene carries "CDK2", protein a
            # different name we must NOT surface for the conflated clique.
            info_content_db=_MgetRedis({
                gene: json.dumps({"preferred_name": "CDK2", "ic": 100.0}),
                protein: json.dumps({"preferred_name": "Cyclin-dependent kinase 2", "ic": 100.0}),
            }),
            gene_protein_db=_MgetRedis({protein: json.dumps([gene, protein])}),
            chemical_drug_db=_MgetRedis({}),
            ancestor_map={"biolink:Protein": ["biolink:Protein"], "biolink:Gene": ["biolink:Gene"]},
        ))

        result = await get_normalized_nodes(
            app, [protein], conflate_gene_protein=True, conflate_chemical_drug=False,
        )
        # Leading (gene) identifier and its preferred_name win.
        assert result[protein]["id"] == {"identifier": gene, "label": "CDK2"}

    def test_merge_node_attributes(self):
        node_a = {
            "id": "primary:id",
            "attributes": [{"attribute_type_id": "bar:baz", "value": 1}],
        }

        node_b = {
            "id": "secondary:id",
            "attributes": [{"attribute_type_id": "bar:baz", "value": 2}],
        }
        new_node = _merge_node_attributes(node_a, node_b, 0)
        assert new_node == {
            "id": "primary:id",
            "attributes": [
                {"attribute_type_id.1": "bar:baz", "value.1": 1},
                {"attribute_type_id.2": "bar:baz", "value.2": 2},
            ],
        }

        node_a = {
            "id": "primary:id",
            "attributes": [{"attribute_type_id.1": "bar:baz", "value.1": 1}],
        }

        new_node = _merge_node_attributes(node_a, node_b, 1)
        assert new_node == {
            "id": "primary:id",
            "attributes": [
                {"attribute_type_id.1": "bar:baz", "value.1": 1},
                {"attribute_type_id.3": "bar:baz", "value.3": 2},
            ],
        }
