---
name: nodenorm
description: Normalize biomedical identifiers (CURIEs) using the Translator NodeNorm API — find the preferred identifier for a concept, list its equivalent identifiers in other databases, and get its Biolink type. Use when working with CURIEs such as MESH:D014867, NCBIGene:1756, CHEBI:15377, UniProtKB:P11532 or MONDO:0005148, to connect an identifier to another data source, to normalize a whole column of identifiers, or to check whether two identifiers refer to the same concept.
---

# Normalizing biomedical identifiers with NodeNorm

NodeNorm answers the question **"what is this identifier?"** Given a CURIE, it returns the preferred
identifier for that concept, every equivalent identifier it knows about, and the concept's Biolink
types.

- **Base URL:** `https://nodenormalization-sri.renci.org/`
- **Machine-readable spec:** `GET /openapi.json` — the authority on parameter names and defaults.
  Prefer it over `/docs`, which is a JavaScript-rendered Swagger UI and near-useless to read.
- The one endpoint that matters is `/get_normalized_nodes`. Everything else is supporting.

## Is this the right service?

| You have | Use |
|---|---|
| An identifier (`CHEBI:15377`, `NCBIGene:1756`) | **NodeNorm** — this skill |
| A name or string (`"aspirin"`, `"type 2 diabetes"`) | **NameRes** (`https://name-resolution-sri.renci.org/`), then normalize the CURIE it returns |

If you have free text, resolve it to a CURIE with NameRes first. NodeNorm will not look up names.

## One identifier

```
GET https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=MESH:D014867
```

Repeat `curie=` for a handful of identifiers. Response:

```json
{
  "MESH:D014867": {
    "id": { "identifier": "CHEBI:15377", "label": "Water" },
    "equivalent_identifiers": [
      { "identifier": "CHEBI:15377" },
      { "identifier": "PUBCHEM.COMPOUND:962" },
      { "identifier": "UNII:059QF0KO0R" }
    ],
    "type": ["biolink:SmallMolecule", "biolink:MolecularEntity", "biolink:ChemicalEntity"],
    "information_content": 47.5
  }
}
```

### Reading the response

- **`id.identifier`** — the preferred identifier. Two CURIEs that normalize to the same
  `id.identifier` refer to the same concept. This is what you store, join on, and send downstream.
- **`id.label`** — the preferred name. **Not necessarily the label of `id.identifier`**; for
  chemicals especially, the name may come from a different member of the clique.
- **`equivalent_identifiers`** — every identifier for this concept, in the Biolink Model's preferred
  prefix order. **This is the field that answers "connect this to another database."** Note that
  **`label` is often absent** on individual entries, so read it with `.get("label")` rather than
  `["label"]`. With `individual_types=true` each entry also gains a `type`, which is a **single
  string**, not a list like the clique-level `type`.
- **`type`** — Biolink classes, most specific first.
- **`information_content`** — 0.0 (broad concept) to 100.0 (very specific). Absent when unknown.

Optional flags: `description=true` adds descriptions (many identifiers have none),
`individual_types=true` adds a `type` to each equivalent identifier, `include_taxa=true` (the
default) adds a `taxa` array of NCBITaxon CURIEs both at the top level and on the individual
entries that have one.

Use GET for one or two identifiers you are inspecting by hand. For anything programmatic use POST —
a long `curie=` list will eventually exceed the server's URL length limit, and POST has no such
ceiling.

## Many identifiers

Use POST. It is fast — 1000 identifiers in roughly 2 seconds — so **never loop over GET requests**.

```
POST https://nodenormalization-sri.renci.org/get_normalized_nodes
Content-Type: application/json

{
  "curies": ["MESH:D014867", "NCBIGene:1756", "RUBBISH:1234"],
  "conflate": true,
  "drug_chemical_conflate": false
}
```

The response is keyed by input CURIE, with the same value shape as the GET method.

- **Deduplicate before sending.** The response is a dictionary, so duplicates buy you nothing.
- **Chunk at around 1000 per request.** Larger batches work but tie up a shared public service.
- **An identifier that cannot be normalized comes back as `null`** — the key is present with a null
  value, it is *not* omitted. Check for null values, not for missing keys. This covers input that is
  not a CURIE at all: `"not a curie"`, and even `""`, return `null` with HTTP 200 rather than an
  error, so malformed input fails silently and will not be caught for you.
- **Response keys echo your input exactly**, including any surrounding whitespace. Look results up
  by the exact string you sent, not a cleaned-up version.

## Conflation — decide this deliberately

Conflation merges concepts that are not strictly identical but are often treated as one. It changes
both the preferred identifier and the size of `equivalent_identifiers`, sometimes dramatically.

| Flag | Merges | Preferred identifier becomes |
|---|---|---|
| `conflate` | A gene with the protein it encodes | the **gene** |
| `drug_chemical_conflate` | A drug with its active ingredient | the **active ingredient** |

**Only the conflation matching the concept's kind does anything; the other flag is a no-op.** You do
not have to guess which flag to try:

| The concept is | The flag that matters | The other flag |
|---|---|---|
| A gene or a protein | `conflate` | does nothing |
| A chemical or a drug | `drug_chemical_conflate` | does nothing |
| Anything else (disease, phenotype, anatomy…) | neither — conflation has no effect | does nothing |

Measured across all four flag combinations: `NCBIGene:1756` and `UniProtKB:P11532` move only with
`conflate` (5→22 and 4→22); `CHEBI:15377`, `MESH:D014867` and `CHEBI:15365` move only with
`drug_chemical_conflate` (30→206, 30→206, 21→436); `MONDO:0005148` and `HP:0002465` do not move at
all. Setting both flags is therefore always safe — it just means "conflate whatever is conflatable."

### Always set both flags explicitly

**The defaults differ between GET and POST.** The same query returns different results:

```
GET  /get_normalized_nodes?curie=MESH:D014867          → 206 equivalent identifiers
POST /get_normalized_nodes {"curies":["MESH:D014867"]} →  30 equivalent identifiers
```

GET defaults both flags to true. The POST body also defaults `conflate` to true, but defaults
**`drug_chemical_conflate` to false** — that one flag is the entire difference, which is why the
example above changes for a chemical and would not change for a gene. This is a known bug
([NodeNormalization#398](https://github.com/NCATSTranslator/NodeNormalization/issues/398)). Until it
is fixed, **set `conflate` and `drug_chemical_conflate` explicitly on every request** and you will
not be caught by it.

### Which setting do you want?

**Conflation changes which question you are asking, not just how many rows come back.**
`conflate=false` asks *"what is this protein?"*; `conflate=true` asks *"what gene is this protein a
product of?"* Both are valid; picking the wrong one gives you a confident, error-free, useless
answer.

- **Matching an existing knowledge graph** (Translator, ROBOKOP) — use the conflation that graph was
  built with. If you do not know, try both and see which one's identifiers appear in the target.
- **Crossing between a gene and a protein** — you need `conflate=true`. A UniProtKB accession
  unconflated has no gene identifiers in its clique at all, so a gene lookup will silently come back
  empty rather than failing.
- **Asking about one specific molecule or one specific protein as itself** — turn conflation off, so
  you get that concept rather than a merged one.
- **Linking loosely across sources** — turn it on for the widest set of equivalents.

Conflated results are a **single flat list** — all the members of the first clique, then the second,
with no marker between them. Use `individual_types=true` to tell gene from protein.

## Recipes

**Connect an identifier to another database.** Normalize it with `individual_types=true`, then look
in `equivalent_identifiers` for the prefix you want — **and filter on `type`, never on prefix
alone.**

```
GET /get_normalized_nodes?curie=UniProtKB:P11532&conflate=true&drug_chemical_conflate=false&individual_types=true
→ id.identifier = NCBIGene:1756 ("DMD"), plus HGNC:2928, OMIM:300377, and three ENSEMBL entries:
    ENSEMBL:ENSG00000198947     biolink:Gene      ← the Ensembl *gene*
    ENSEMBL:ENSP00000288447     biolink:Protein
    ENSEMBL:ENSP00000288447.4   biolink:Protein
```

A conflated clique routinely contains **several identifiers sharing one prefix**, because it holds
both halves of a gene/protein or drug/chemical pair — and they may be versioned duplicates too.
Taking the first `ENSEMBL:` you find gets the right answer here only by luck of ordering. Decide
which one you want by its `type`.

**Normalize a column in a file.** Read the column, deduplicate, POST in chunks of 1000, build a
`{input → id.identifier}` map, and report the nulls separately — they are usually retired
identifiers, typos, or prefixes NodeNorm does not cover.

**Are these two identifiers the same concept?** Normalize both with identical conflation settings
and compare `id.identifier`. This is the reliable way; prefer it. See `/get_setid` below if you want
a single comparable hash for a whole set.

**What kind of thing is this?** Read `type[0]` — the most specific Biolink class.

## `/get_setid` — a stable hash for a set of identifiers

Normalizes a set of CURIEs, deduplicates, sorts, and hashes the result. Two sets containing the same
concepts produce the same hash, whatever order or spelling of identifiers you started from. Useful
for comparing or caching sets of concepts without storing the members.

```
GET /get_setid?curie=MESH:D014867&curie=NCBIGene:1756&conflation=GeneProtein&conflation=DrugChemical
```

**This endpoint does not take the same conflation parameters as `/get_normalized_nodes`, and it will
not tell you when you get them wrong.**

- The parameter is **`conflation`** (singular), repeated once per conflation, with the values
  `GeneProtein` and `DrugChemical` — not the `conflate` / `drug_chemical_conflate` booleans used
  everywhere else.
- **It defaults to no conflation at all**, the opposite of GET `/get_normalized_nodes`.
- **Wrong parameter names are silently ignored.** `conflations=GeneProtein` (plural) and
  `conflate=true` both return HTTP 200 with an unconflated hash and no warning:

  ```
  /get_setid?curie=UniProtKB:P11532                          → uuid:e9a5e65f…  (UniProtKB:P11532)
  /get_setid?curie=UniProtKB:P11532&conflation=GeneProtein    → uuid:e020b733…  (NCBIGene:1756)
  /get_setid?curie=UniProtKB:P11532&conflations=GeneProtein   → uuid:e9a5e65f…  silently unconflated
  ```

  Always check the `conflations` field echoed back in the response — if it is `[]` when you asked for
  a conflation, your parameter name was wrong.

One GET request is **one set**, not a batch. To hash several sets in one call, POST a bare JSON
array (not an object wrapping one) and get a parallel array back. Note that the POST body field
*is* `conflations` — plural, the opposite of the GET query parameter, and the singular form is
silently ignored here too:

```json
POST /get_setid
[
  {"curies": ["UniProtKB:P11532"], "conflations": ["GeneProtein"]},
  {"curies": ["NCBIGene:1756"],    "conflations": ["GeneProtein"]}
]
```

The response also carries `normalized_curies` (what the hash was built from — check this if a result
surprises you), `normalized_string`, and `error`.

## Gotchas

- **Lookups are case-insensitive and whitespace-trimmed.** `"  mesh:d014867  "` resolves fine. But
  the response key is still the string you sent.
- **A missing identifier may be missing by design.** Babel keeps only identifiers whose prefix is
  valid for the concept's Biolink type and drops the rest, so a CURIE you expected can be
  legitimately absent from a clique.
- **The data is a fixed snapshot**, not a live database. `GET /status` reports `babel_version` (a
  build name like `2025sep1`). Record it if you need reproducible results — a later build may
  normalize the same identifier differently.
- **Information content from builds before `2025sep1` is unreliable** (it was compared as a string,
  so some values are wrongly 100). Check `/status` first.
- **This is a shared public service.** Batch with POST; do not hammer it with parallel requests.

## Other endpoints

| Endpoint | Purpose |
|---|---|
| `GET /status` | Which Babel build and Biolink Model version this instance serves |
| `GET /get_allowed_conflations` | The conflation names this instance supports |
| `GET/POST /get_setid` | A stable hash for a set of CURIEs — see above, its parameters differ |
| `GET /get_semantic_types` | Every Biolink type present in this instance |
| `GET /get_curie_prefixes` | CURIE prefix counts per Biolink type (approximate) |
| `POST /query`, `/asyncquery` | Normalize a whole TRAPI message — **deprecated**, do not use |

## Why cliques look the way they do

The identifiers, preferred names, types and information content all come from
[Babel](https://github.com/NCATSTranslator/Babel), which is what decides that two identifiers are
equivalent. If a clique looks wrong — two concepts merged, or one concept split in two — that is a
Babel issue, not a NodeNorm one. See
[Where NodeNorm's data comes from](https://github.com/NCATSTranslator/NodeNormalization/blob/main/documentation/Babel.md).
