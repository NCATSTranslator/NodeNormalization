# Test notes

Non-obvious bits of writing tests here. Marker commands and the general
frontend/loader split are in `documentation/CONTRIBUTOR.md`; loader-*driving*
mechanics (the `redis_connect` `lru_cache`, which module to `monkeypatch`, the
requests/docker/testcontainers landmine) are in `documentation/Loader.md`.

## The shared-app-singleton pitfall

`test_norm.py` and `test_setid.py` assign to `app.state.*` at **import time**,
mutating the shared `node_normalizer.server.app` singleton for the rest of the
process. Don't assume that app is pristine in a later test, and don't add more
import-time mutation. New tests should build their own fake app
(`SimpleNamespace(state=SimpleNamespace(...))`) rather than importing the server
app — see the examples below.

## Querying the frontend against loaded data (end-to-end)

To assert on what `get_normalized_nodes` *returns* (not just what keys the loader
wrote), drive the real frontend against a testcontainer instead of a hand-built
mock — it avoids reimplementing the loader's canonical-id/db-5 keying. See
`test_query_gene_protein_conflation_uses_gene_preferred_name` in
`test_loader_integration.py`:

- **Reuse the loader's `redis_config.yaml`.** The loader fixture already writes one
  pointing every logical db at the container; the frontend's
  `RedisConnectionFactory.create_connection_pool(path)` consumes that same file. Have
  the fixture `yield` the config path.
- **`RedisConnectionFactory.connections` is a process-wide class-level cache.** It's
  only populated `if not connections`, so a pool left over from another test wins and
  you attach to a dead container. Set `RedisConnectionFactory.connections = {}` before
  `create_connection_pool`, and in a `finally` close every connection and clear it again.
- **Fake the app, skip the Toolkit.** Build `app = SimpleNamespace(state=SimpleNamespace(
  eq_id_to_id_db=..., id_to_eqids_db=..., info_content_db=..., gene_protein_db=..., ...))`
  from `get_connection(name)`, and pre-seed `state.ancestor_map` for the types in play
  (e.g. `{"biolink:Gene": ["biolink:Gene"], ...}`) so `get_ancestors` never needs a
  Biolink `Toolkit` (`state.toolkit=None`). Only the conflation *fallback* in
  `create_node` reads Redis beyond these dbs, so the stored-`preferred_name` path needs
  nothing more.

For a pure-unit version that skips Docker entirely, `test_normalizer.py` drives
`get_normalized_nodes` / `create_node` against a tiny `_MgetRedis` mock — cheaper, but
you must construct the redis values (canonical-id keys, db-5 JSON, gene-first conflation
list) by hand, so it's easy to encode an assumption the loader doesn't actually make.
