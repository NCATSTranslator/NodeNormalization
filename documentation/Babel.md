# Where NodeNorm's data comes from

NodeNorm does not decide which identifiers are equivalent, which one is preferred, or what a clique
is called. All of that is computed by [Babel](https://github.com/NCATSTranslator/Babel), a separate
pipeline, and loaded into NodeNorm's Redis databases as a fixed snapshot. NodeNorm is the query
layer over that snapshot.

This page collects the Babel behaviour that shows up directly in a NodeNorm response, so that you
can answer "why did I get *this* answer?" without reading the Babel pipeline. It is the only page in
this repository that links into Babel's documentation; everything else here links to this page.

## Which Babel build am I querying?

`GET /status` reports `babel_version` (a date-based build name such as `2025sep1`) and
`babel_version_url`, a link to that build's release notes. Every NodeNorm deployment serves exactly
one Babel build: the backend databases are written once at load time and never updated in place, so
results cannot change until an operator loads a newer build. If you need reproducible results,
record the `babel_version` alongside them.

Published Babel builds can be downloaded directly — see Babel's
[Downloads.md](https://github.com/NCATSTranslator/Babel/blob/master/docs/Downloads.md).

## Cliques, preferred identifiers, and labels

A **clique** is a set of identifiers Babel believes name the same concept. Normalizing any member
returns the whole clique.

The **preferred identifier** (`id.identifier`) is the first identifier in the clique, ordered by the
Biolink Model's `id_prefixes` list for that clique's type. For a
[`biolink:SmallMolecule`](https://biolink.github.io/biolink-model/SmallMolecule/#valid-id-prefixes),
that means CHEBI before UNII, and so on. `equivalent_identifiers` is returned in the same order.

The **preferred label** (`id.label`) is Babel's `preferred_name` for the clique, and it is **not
necessarily the label of the preferred identifier**. Babel may pick a label from a different
identifier to give a clearer name. Two rules drive this:

- Chemical types boost a specific prefix list when choosing a label — currently DRUGBANK,
  DrugCentral, CHEBI, MESH, GTOPDB — rather than using the Biolink prefix order.
- Labels longer than a configured length are demoted, and used only if nothing shorter exists.

Babel's [Understanding.md](https://github.com/NCATSTranslator/Babel/blob/master/docs/Understanding.md)
has the full rules for both.

## Information content

`information_content` is a decimal from 0.0 to 100.0 describing how specific a concept is: **0.0 is
a broad, high-level term with many subclasses; 100.0 is a very specific term with none**. Babel
takes these from [Ubergraph](https://github.com/INCATools/ubergraph/?tab=readme-ov-file#graph-organization)'s
`normalizedInformationContent`, which is precomputed per ontology class from the count of terms
related to it via `rdfs:subClassOf` or any existential relation.

The value is per clique, not per identifier: where several identifiers in a clique have an IC value,
Babel stores the lowest of them, and NodeNorm returns that stored value unchanged. Not every clique
has one — if no identifier in a clique is known to Ubergraph, the field is omitted entirely.

## Conflation

Conflation merges cliques that are not identical but that you may want treated as one concept. Babel
publishes the groupings; NodeNorm applies them at query time when you ask for them.

- **GeneProtein** (`conflate`) merges a gene with the protein it encodes. **The gene always comes
  first**, so the preferred identifier of a conflated gene/protein clique is the gene.
- **DrugChemical** (`drug_chemical_conflate`) merges a drug with its active ingredient. Babel orders
  these so that **the active ingredient precedes any formulations**.

Two consequences worth knowing:

- **Babel assigns conflated cliques no type at all.** NodeNorm computes the `type` list at query
  time, starting from the most specific type of the first identifier, adding its Biolink ancestors,
  then adding the types and ancestors of every other member.
- **The clique boundaries are lost in the response.** `equivalent_identifiers` is a single flat
  list — the members of the first clique, then the second, and so on — with no marker for where one
  ends. There is currently no way to recover the original clique leaders
  ([#320](https://github.com/NCATSTranslator/NodeNormalization/issues/320)); `individual_types=true`
  will at least give you a Biolink type per identifier.

See Babel's [Conflation.md](https://github.com/NCATSTranslator/Babel/blob/master/docs/Conflation.md).

## Descriptions

All descriptions come from [UberGraph](https://github.com/INCATools/ubergraph/), so most identifiers
have none. They are off by default; pass `description=true` to include them.

## Gotchas

**An identifier missing from a clique may be missing by design.** When Babel writes a compendium it
keeps only identifiers whose prefix appears in the Biolink Model's `id_prefixes` for that clique's
type, and silently drops the rest. A CURIE that Babel knows about can therefore be legitimately
absent from the clique you get back, particularly for types with short prefix lists.

**A CURIE NodeNorm cannot normalize maps to `null`, it is not omitted.** Every CURIE you send back
comes back as a key in the response object; check for `null` values rather than for missing keys.

**Information content from builds before `2025sep1` is unreliable.** Babel compared IC values as
strings rather than numbers until then, so some cliques were assigned an IC of `100` when the true
value was lower. Check `/status` before trusting IC from an older deployment.

**A clique can be wrong in two directions.** A *split* clique is two cliques that should be one; a
*lumped* clique is one clique holding identifiers for genuinely different concepts. Both are Babel
issues, not NodeNorm issues — see below.

## Reporting a problem

| What is wrong | Where to file |
|---|---|
| Wrong identifiers in a clique, wrong Biolink type, wrong preferred label, bad description | [Babel](https://github.com/NCATSTranslator/Babel/issues/) |
| NodeNorm returns an error, behaves oddly, or the API itself is wrong | [NodeNorm](https://github.com/NCATSTranslator/NodeNormalization/issues/) |

When filing against Babel, a link to a NodeNorm query showing the problem is very helpful. Babel's
[NewIssue.md](https://github.com/NCATSTranslator/Babel/blob/master/docs/NewIssue.md) describes what
to include and how issues are prioritized.

## Going deeper

Everything above is what leaks into a NodeNorm response. How Babel actually builds cliques — the
cross-reference concords, the transitive merge, per-source ingest quirks, the DuckDB and Parquet
exports, and the pipeline itself — is documented in
[Babel's documentation index](https://github.com/NCATSTranslator/Babel/blob/master/docs/README.md).
The output file formats NodeNorm's loader reads are specified in
[DataFormats.md](https://github.com/NCATSTranslator/Babel/blob/master/docs/DataFormats.md).
