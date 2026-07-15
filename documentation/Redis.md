# Redis databases

NodeNorm stores everything in Redis. Locally and in tests, all seven logical
databases live in a single Redis instance at distinct `db` indices (configured in
`redis_config.yaml`). In production each logical database is its own Redis host,
all at `db: 0` (configured by the Helm chart's generated ConfigMap).

## Layout

| db | Name (`redis_config.yaml`) | Key → Value | Written by | Read by |
|----|----------------------------|-------------|------------|---------|
| 0 | `eq_id_to_id_db` | `UPPER(equivalent id)` → canonical id | `load_compendium` | normalization lookup |
| 1 | `id_to_eqids_db` | canonical id → equivalent identifiers (JSON) | `load_compendium` | normalization lookup |
| 2 | `id_to_type_db` | canonical id → Biolink leaf type | `load_compendium` | normalization lookup |
| 3 | `curie_to_bl_type_db` | `file-*` per-file prefix counts; `semantic_types` list; per-type prefix counts | `load_compendium` / `merge_semantic_meta_data` | `/get_semantic_types`, `/get_curie_prefixes` |
| 4 | `gene_protein_db` | member id → gene/protein clique line | `load_conflation` | gene/protein conflation |
| 5 | `info_content_db` | canonical id → information content | `load_compendium` | IC filtering |
| 6 | `chemical_drug_db` | member id → chemical/drug clique line | `load_conflation` | chemical/drug conflation |

The `db` indices above are what `redis_config.yaml` and `tests/redis_config.yaml`
use. They only matter locally, where all seven share one Redis; in production the
index is always 0 and the databases are separated by host.

## Cluster mode

NodeNorm used to support a clustered Redis backend (`is_cluster: true`) via the
`redis-py-cluster` package. It was removed (see the "Drop Redis cluster support"
commit; the last release that shipped it was **v2.4.1**), because no deployment
uses it any more — `is_cluster` is `false` for all seven databases in every values file
(ncats dev/test/prod, RENCI exp) — and the cluster branch was the main source of
complexity in the codebase:

- `redis_adapter.RedisConnection` forked every method between a synchronous
  `rediscluster` client and the asynchronous `aioredis` client, which is why
  `execute_pipeline` existed as a static dispatcher and why the old loader was
  littered with `asyncio.coroutines.iscoroutine(...)` checks.
- Cluster pipelines could not span slots the way standalone pipelines do.

`RedisConnection.create` now **raises** on `is_cluster: true` rather than silently
mishandling it. The `is_cluster` field is still parsed so existing config files
load.

### Bringing it back

If clustering is needed again:

1. It would be for the **frontend only**. The loader's `mode: load` is a one-time
   population that produces the RDB backups; you would not cluster during loading.
2. The `mode: restore` piping (`rdb-cli | redis-cli --pipe`) lives in the Helm
   chart repo, not here — so cluster-aware restore logic belongs there, not in
   this codebase. These notes exist mainly for developers working in that repo.
3. In this repo, the work would be: reintroduce a cluster client in
   `redis_adapter.py` (ideally on `redis-py` >= 4.2, which has both asyncio and
   cluster support, rather than the unmaintained `aioredis` + `redis-py-cluster`
   pair), and restore the `is_cluster` branch in `RedisConnection.create`. Git
   history for the removed code is the reference implementation.
