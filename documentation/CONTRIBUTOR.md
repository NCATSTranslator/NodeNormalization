# Contributing to NodeNorm

How NodeNorm is put together and how to run it locally. For the two large
subsystems see [Loader.md](Loader.md) (the data loader) and [Redis.md](Redis.md)
(the backend database layout).

## Two tools in one repo

NodeNorm is really two programs that share some code:

- **The frontend** — a FastAPI service (`node_normalizer/server.py`) that answers
  normalization queries by reading the backend Redis databases. This is what runs
  in production, both at `/` and behind a TRAPI version like `/1.5/`. See
  [Frontend](#frontend) below.
- **The loader** — a batch program (`node_normalizer/loader/`, invoked via the
  root `load.py`) that reads Babel compendium/conflation files and populates those
  Redis databases. See [Loader.md](Loader.md).

Shared code lives directly in `node_normalizer/` (`config.py`, `util.py`,
`normalizer.py`, `redis_adapter.py`, `model/`). Loader-only code lives in
`node_normalizer/loader/`. The frontend has **no** dependency on the loader.

`node_normalizer/config.py` is the single source of truth for repo-relative paths
(`config.json`, `redis_config.yaml`, `resources/`). Prefer it over recomputing
`Path(__file__).parents[N]`.

## Running locally

```bash
# Start a local Redis (all 7 logical DBs live in one instance, distinct db index)
docker compose -f docker-compose-redis.yml up -d

# Point config.json at your compendium/conflation directory, then load (see Loader.md):
python load.py

# Run the frontend:
uvicorn --host 0.0.0.0 --port 8000 --workers 1 node_normalizer.server:app
```

Tests:

```bash
pip install -r requirements.txt -r requirements-loader.txt -r requirements-test.txt

pytest -m "not integration"   # unit tests, no Docker needed
pytest -m integration         # loader against a real Redis (needs Docker)
```

Integration tests are marked `integration` and start a real Redis via
`testcontainers`. CI runs both in `.github/workflows/test.yml`. For the loader
test gotchas and the requests/docker/testcontainers landmine, see
[Loader.md](Loader.md#writing-loader-integration-tests).

## Frontend

The FastAPI app in `node_normalizer/server.py` serves the normalization API both
at `/` and behind a TRAPI version like `/1.5/` (`GET/POST /get_normalized_nodes`,
`/query`, `/get_setid`, `/get_semantic_types`, `/get_curie_prefixes`, `/status`).
It reads the backend
Redis databases via the async `RedisConnection` in `node_normalizer/redis_adapter.py`
and never touches the loader. Core logic lives in `node_normalizer/normalizer.py`;
request/response models in `node_normalizer/model/`. Run it with the `uvicorn`
command above; interactive docs are at `http://localhost:8000/docs`.

## Backlog

Larger cleanups that are out of scope for any single PR, filed as issues on the
**NodeNorm v2.5.0** milestone:

- **`semantic_types` should be a Redis SET, not a LIST** ([#379](https://github.com/NCATSTranslator/NodeNormalization/issues/379)).
  `merge_semantic_meta_data` `LPUSH`es into `semantic_types`, and because it runs
  once per loader Job the list ends up with one copy of every type per file. The
  frontend papers over this by deduping on read. The proper fix (`SADD`/`SMEMBERS`)
  is a stored-format change, and `mode: restore` loads RDB backups built by the
  *old* loader — a new server doing `SMEMBERS` on a restored LIST would get
  `WRONGTYPE`. Needs a coordinated loader/server/backup rollout.
- **`merge_semantic_meta_data` runs once per Job, and races** ([#380](https://github.com/NCATSTranslator/NodeNormalization/issues/380)).
  It should run once, after all compendium Jobs finish, rather than re-reading
  every `file-*` key and re-summing on every Job. Beyond the redundancy, running
  it concurrently is a **read-modify-write race**: each Job does `KEYS file-*` →
  `GET` → sum → `SET <bl_type>`, with no lock, `WATCH`/`MULTI`, or Lua script. Two
  Jobs that read different subsets of `file-*` keys and then `SET` the same
  per-type key are last-writer-wins, so the surviving count can reflect only a
  subset of files. The `file-*` keys themselves don't collide (one distinct key
  per file) and the aggregates feed only the statistics endpoints
  (`/get_curie_prefixes`, `/get_semantic_types`), not normalization, so it's
  tolerated for now — but the fix is architectural (a single post-load merge
  step), not a lock around this function.
- **Migrate the frontend off `aioredis`** ([#381](https://github.com/NCATSTranslator/NodeNormalization/issues/381)),
  unmaintained and pinned at 1.3.1. `redis-py` >= 4.2 has asyncio built in, which
  would let the frontend and loader share one client library.
- **`uv` / `pyproject.toml` migration** ([#382](https://github.com/NCATSTranslator/NodeNormalization/issues/382)).
  Turn this into a proper package with dependency groups, and replace the root
  `load.py` shim with a `nodenorm-load` console script (which will require a
  matching chart change).
- **Wire the docker-compose-based tests into CI** ([#383](https://github.com/NCATSTranslator/NodeNormalization/issues/383)).
  `.github/workflows/test.yml` runs the headless unit + loader-integration tests;
  `test_endpoints.py` and `test_callback.py` still need porting off the legacy
  docker-compose harness.
