# The loader

Everything about the NodeNorm data loader in one place: what it does, how a load
runs in production, how to develop and test it, and why it's tuned the way it is.

The loader is a batch program — `node_normalizer/loader/`, invoked via the root
`load.py` — that reads Babel compendium/conflation JSONL files and populates the
backend Redis databases the frontend queries. It is synchronous `redis-py` code
(read a file, fill a pipeline, execute, repeat). For the database layout and what
each database stores, see [Redis.md](Redis.md); for the project as a whole, see
[CONTRIBUTOR.md](CONTRIBUTOR.md).

## The two ways to populate the backend

The `node-normalization-loader` Helm chart (in the `translator-devops` repo) has
two modes, selected by `values.yaml: mode`:

- **`mode: load`** — the Python path. Runs `python load.py` (this loader). One
  Kubernetes Job per file. This is the slow, once-per-Babel-release step described
  below.
- **`mode: restore`** — no Python. Downloads pre-built `.rdb.gz` backups and pipes
  them straight into Redis with `redis-cli --pipe`. This is how a database is
  normally stood up; it only exists because someone ran `mode: load` once and
  saved the result. It uses the `data-loading/` image: a `redis:latest` base that
  compiles `rdb-cli` from [redis/librdb](https://github.com/redis/librdb) and
  ships `data-loading/rdb-to-resp.sh`, which the chart calls as
  `bash rdb-to-resp.sh <file.rdb> <redis_version> | redis-cli --pipe`. (The older
  chart used `rdb -c protocol` from `redis-rdb-tools`, which does not work against
  Redis > 6; replaced by `rdb-cli` in PR #349.)

## How `mode: load` runs

The chart generates, **per file**, a `config.json` containing a single-element
`data_files` (or a single-element `conflations`) list, mounts it at
`/code/config.json`, and runs a small shell script whose payload is literally
`python load.py`. It creates **one Kubernetes Job per compendium file** and one
per conflation. Large compendia are pre-split into 10M-line chunks
(`split -d -l 10000000 …`), so `SmallMolecule.txt` / `Protein.txt` become dozens
of Jobs. Cluster capacity means ~5–6 Jobs run at once; a full load takes roughly
0.5–1.5 days.

Consequences worth knowing:

- The image used is the **main webserver image** (root `Dockerfile`, `WORKDIR
  /code`). That is why the webserver image ships the loader and its dependencies
  (`requirements.txt` **and** `requirements-loader.txt`).
- `load.py` takes **no arguments**; everything comes from `config.json`. The
  loader reads exactly six keys: `compendium_directory`, `conflation_directory`,
  `biolink_version`, `test_mode`, `data_files`, `conflations`. `biolink_version`
  is a tag/branch/commit in the biolink-model repo, pinned so ancestors are
  computed against the same model Babel built the data with (the frontend pins
  its own version separately — see `BIOLINK_MODEL_TAG` in `server.py`).
- Redis connection details come **only** from `redis_config.yaml`. In Kubernetes
  the chart overwrites it with a generated ConfigMap, so the copy checked into
  this repo governs **local dev and tests only**.
- Each Job runs `merge_semantic_meta_data()` at the end, so that aggregation
  currently runs once per file rather than once per load (see #380).

### Where the time goes

The seven backend databases are **separate single-threaded Redis instances**. The
two write-heavy ones are the ceiling, because **every** compendium Job writes to
them at the same time:

- `eq_id_to_id_db` — one `SET` per *equivalent identifier* (N per input line).
- `id_to_eqids_db` — one `SET` per line, but the values are large JSON blobs; this
  is the biggest database (150–220 GB).

So load throughput is bounded by how fast those two single-threaded servers can
absorb writes, not by the number of Jobs.

## The database lifecycle: write-once

A backend database is **created empty, loaded once, then frozen**:

1. Stand up empty Redis instances (`redis-r3-external` Helm chart).
2. Populate them with `mode: load`.
3. **Manually** `BGSAVE` each instance to flush it to disk (per the loader
   [README](https://github.com/helxplatform/translator-devops/blob/develop/helm/node-normalization-loader/README.md)),
   then copy the resulting `dump.rdb` files elsewhere.
4. Those RDB files seed all future instances via `mode: restore`.

Nothing writes to a database after its initial load. This is why the loader can
safely disable snapshotting during the load (next section).

## Why the loader disables periodic saves

The `redis-r3-external` instances default to `--save 300 1000000` (snapshot every
5 min if ≥1M keys changed). During a load every hot database easily clears that
threshold, so Redis forks a **multi-GB BGSAVE every few minutes** throughout the
write storm. On the 150 GB+ databases the copy-on-write churn from forking under
continuous writes is a real, pure-waste drag — the periodic snapshot is thrown
away anyway, because persistence is the deliberate manual `BGSAVE` in step 3.

So `load_all()` calls `disable_periodic_save()`, which issues `CONFIG SET save ""`
on each backend at the start of the load. Notes:

- **Scoped to `mode: load` automatically.** `mode: restore` runs no Python, so its
  instances keep their periodic-save safety net untouched. (That matters: the
  restore Job's own persistence step is less robust — see #390.)
- **Best-effort.** A server that refuses `CONFIG SET` is logged and skipped, not
  fatal.
- **Accepted trade-off:** a crashed load leaves the database *empty* rather than
  partially populated. That's fine — loads are re-runnable, and re-running
  overwrites cleanly.

## Other loader performance choices

- **`pipeline(transaction=False)`** everywhere — this is a bulk load, not an atomic
  update, so the per-block `MULTI`/`EXEC` framing is pure overhead.
- **Prefix stats counted once per line.** `_accumulate_source_prefixes` computes a
  line's CURIE-prefix counts once and folds them into every implied Biolink type,
  instead of re-splitting each identifier once per ancestor type.

## Writing loader integration tests

`tests/test_loader_integration.py` is the pattern to copy. Two non-obvious things
about driving the loader from a test:

- **`redis_connect` is `@lru_cache`d.** It memoizes one client per db name for the
  life of the process, so call `loader.loader.redis_connect.cache_clear()` before
  and after a test that points it at a throwaway Redis — otherwise a later test
  reuses the previous container's (now-dead) connection.
- **Patch paths on the loader module, not on `config`.** The loader does
  `from ..config import get_config, REDIS_CONFIG_PATH`, so it holds its *own*
  reference to `REDIS_CONFIG_PATH`. To redirect it, `monkeypatch.setattr` on
  `node_normalizer.loader.loader.REDIS_CONFIG_PATH`. `CONFIG_PATH` is different:
  `get_config()` reads `config.CONFIG_PATH` at call time, so patch it on
  `node_normalizer.config`.

### Dependency landmine: requests / docker / testcontainers

`requests >= 2.32` changed its `HTTPAdapter`, which breaks the `docker` SDK < 7.1
that `testcontainers` pulls in — its unix-socket adapter can no longer reach the
Docker daemon, and `testcontainers` (hence every `integration` test) fails at
container startup. If you bump `requests`, keep `docker>=7.1.0` pinned in
`requirements-test.txt`. This is easy to misdiagnose as a Docker/environment
problem rather than a dependency conflict.

## Backlog

Bigger loader-speed ideas that need more than a contained change are filed on the
**NodeNorm v2.6.0** milestone:

- **`MSET`-batch the high-cardinality writes** ([#387](https://github.com/NCATSTranslator/NodeNormalization/issues/387))
  — cut command count on the shared single-threaded servers. Held out of the 2.5
  work because it's the one change that could *silently* drop data if the flush
  logic is wrong; needs a correctness test first.
- **Upgrade `redis-py` 3.5.3 → 4/5 (+ hiredis)** ([#388](https://github.com/NCATSTranslator/NodeNormalization/issues/388))
  — faster client, and ties into migrating the frontend off `aioredis` (#381).
- **Overlap the four per-block pipeline flushes** ([#389](https://github.com/NCATSTranslator/NodeNormalization/issues/389))
  — they target independent databases but currently flush serially.
- **Harden the `mode: restore` BGSAVE** ([#390](https://github.com/NCATSTranslator/NodeNormalization/issues/390))
  — it's fire-and-forget, so a restored database may not be persisted before the
  Job exits. (A `translator-devops` chart fix.)
