# Loader notes

## Don't store the clique-level `taxa` field

Babel compendium lines carry a clique-level `taxa` list *and* a per-identifier
`t` list on each entry in `identifiers`. The clique-level `taxa` is just the
union of the per-identifier `t` values, so persisting it separately would be
redundant.

`load_compendium` already writes the whole `identifiers` list (including each
`t`) into `id_to_eqids_db`, so taxa are retrievable per-identifier from there.
Do **not** add `taxa` to the db-5 clique-property JSON — read `t` from the
`id_to_eqids_db` blob instead. (See the taxa assertion in
`tests/test_loader_integration.py`.)
