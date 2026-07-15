# NodeNorm development notes

This document records how NodeNorm is built and, in particular, how a data load
actually works — details that are otherwise scattered across this repo and the
[translator-devops](https://github.com/helxplatform/translator-devops) Helm
charts. It also lists larger cleanups that are out of scope for any single PR.

## Two tools in one repo

NodeNorm is really two programs that share some code:

- **The frontend** — a FastAPI service (`node_normalizer/server.py`) that answers
  normalization queries by reading the backend Redis databases. This is what runs
  in production, both at `/` and behind a TRAPI version like `/1.5/`.
- **The loader** — a batch program (`node_normalizer/loader/`, invoked via the
  root `load.py`) that reads Babel compendium/conflation files and populates those
  Redis databases.

Shared code lives directly in `node_normalizer/` (`config.py`, `util.py`,
`normalizer.py`, `redis_adapter.py`, `model/`). Loader-only code lives in
`node_normalizer/loader/`. The frontend has **no** dependency on the loader.

`node_normalizer/config.py` is the single source of truth for repo-relative
paths (`config.json`, `redis_config.yaml`, `resources/`). Prefer it over
recomputing `Path(__file__).parents[N]`.

## How a data load actually runs

Loading is driven by the `node-normalization-loader` Helm chart in the
translator-devops repo, which has two modes selected by `values.yaml: mode`.

### `mode: load` — the Python path

The chart generates, **per file**, a `config.json` containing a single-element
`data_files` (or a single-element `conflations`) list, mounts it at
`/code/config.json`, and runs a small shell script whose payload is literally
`python load.py`. It creates **one Kubernetes Job per compendium file** (dozens,
in parallel) and one per conflation.

Consequences worth knowing:

- The image used is the **main webserver image** (root `Dockerfile`, `WORKDIR
  /code`). That is why the webserver image ships the loader and its dependencies
  (`requirements.txt` **and** `requirements-loader.txt`).
- `load.py` takes **no arguments**; everything comes from `config.json`. The
  loader reads exactly five keys: `compendium_directory`, `conflation_directory`,
  `test_mode`, `data_files`, `conflations`.
- Redis connection details come **only** from `redis_config.yaml`. In Kubernetes
  the chart overwrites it with a generated ConfigMap, so the copy checked into
  this repo governs **local dev and tests only**.
- Each Job runs `merge_semantic_meta_data()` at the end, so that aggregation
  currently runs once per file rather than once per load (see backlog below).

### `mode: restore` — no Python

Downloads pre-built `.rdb.gz` Redis backups and pipes them straight into Redis
with `redis-cli --pipe`. No Python runs. This is what the `data-loading/` image
is for: it is a `redis:latest` base that compiles `rdb-cli` from
[redis/librdb](https://github.com/redis/librdb) and ships
`data-loading/rdb-to-resp.sh`, which the chart calls as
`bash rdb-to-resp.sh <file.rdb> <redis_version> | redis-cli --pipe`.

(The older chart used `rdb -c protocol` from `redis-rdb-tools`, which does not
work against Redis > 6; that was replaced by `rdb-cli` in PR #349.)

## Redis databases

See [Redis.md](Redis.md) for the database layout and what each one stores.

## Running locally

```bash
# Start a local Redis (all 7 logical DBs live in one instance, distinct db index)
docker compose -f docker-compose-redis.yml up -d

# Point config.json at your compendium/conflation directory, then:
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
`testcontainers` (see `tests/test_loader_integration.py`). CI runs both in
`.github/workflows/test.yml`.

### Writing loader integration tests

`tests/test_loader_integration.py` is the pattern to copy. Two non-obvious
things about driving the loader from a test:

- **`redis_connect` is `@lru_cache`d.** It memoizes one client per db name for
  the life of the process, so call `loader.loader.redis_connect.cache_clear()`
  before and after a test that points it at a throwaway Redis — otherwise a
  later test reuses the previous container's (now-dead) connection.
- **Patch paths on the loader module, not on `config`.** The loader does
  `from ..config import get_config, REDIS_CONFIG_PATH`, so it holds its *own*
  reference to `REDIS_CONFIG_PATH`. To redirect it, `monkeypatch.setattr` on
  `node_normalizer.loader.loader.REDIS_CONFIG_PATH`. `CONFIG_PATH` is different:
  `get_config()` reads `config.CONFIG_PATH` at call time, so patch it on
  `node_normalizer.config`.

### Dependency landmine: requests / docker / testcontainers

`requests >= 2.32` changed its `HTTPAdapter`, which breaks the `docker` SDK
< 7.1 that `testcontainers` pulls in — its unix-socket adapter can no longer
reach the Docker daemon, and `testcontainers` (hence every `integration` test)
fails at container startup. If you bump `requests`, keep `docker>=7.1.0` pinned
in `requirements-test.txt`. This is easy to misdiagnose as a Docker/environment
problem rather than a dependency conflict.

## Backlog (out of scope for the loader-reorg PR)

Filed as issues on the **NodeNorm v2.5.0** milestone:

- **`semantic_types` should be a Redis SET, not a LIST** ([#379](https://github.com/NCATSTranslator/NodeNormalization/issues/379)).
  `merge_semantic_meta_data` `LPUSH`es into `semantic_types`, and because it runs
  once per loader Job the list ends up with one copy of every type per file. The
  frontend papers over this by deduping on read. The proper fix (`SADD`/`SMEMBERS`)
  is a stored-format change, and `mode: restore` loads RDB backups built by the
  *old* loader — a new server doing `SMEMBERS` on a restored LIST would get
  `WRONGTYPE`. Needs a coordinated loader/server/backup rollout.
- **`merge_semantic_meta_data` runs once per Job** ([#380](https://github.com/NCATSTranslator/NodeNormalization/issues/380)).
  It should run once, after all compendium Jobs finish, rather than re-reading
  every `file-*` key and re-summing on every Job.
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
