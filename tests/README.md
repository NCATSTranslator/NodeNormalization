# Test Suite Notes

## Running tests

```bash
pytest -m "not integration"   # unit tests — no Redis needed
pytest -m "integration"       # integration tests — requires Redis on localhost:6379
```

Start Redis locally with:
```bash
docker compose -f docker-compose-redis.yml up -d
```

---

## Tests that do not currently pass

### All integration tests (`tests/test_integration.py`)

**Status:** `xfail` (expected failure)

**Root cause:** `bmt==1.4.3` (installed in this environment) is a "lite" variant that
raises `ValueError: bmt-lite does not support the 'schema' argument` when `server.py`
calls `Toolkit(BIOLINK_MODEL_URL)` during `startup_event`. The integration test fixture
(`integration_client` in `conftest.py`) starts the app via `TestClient`, which triggers
`startup_event`, so all 28 integration tests fail at setup before any test body runs.

**Estimated fix:** Determine the correct `bmt` version that supports passing a schema URL
to `Toolkit()`, update `requirements.txt`, and re-run. If no such version is easily
available, the alternative is to make the Biolink Model URL optional in `startup_event`
and fall back to `Toolkit()` (no URL) for local/test use. Probably 30–60 minutes of work.

---

### `tests/test_loader.py::test_nn_load`

**Status:** Passes, but emits a `RuntimeWarning: coroutine 'NodeLoader.load_compendium' was never awaited`.

**Root cause:** `load_compendium` is an `async` method but `test_nn_load` calls it
synchronously: `assert node_loader.load_compendium(good_json, 5)`. This passes today
only because the return value of an unawaited coroutine is truthy. The actual loading
logic never executes.

**Estimated fix:** Refactor the test to `await` the call, or add a synchronous wrapper.
Low effort (~5 minutes) but requires understanding whether the test was intentionally
bypassing the async path via `_test_mode = 1`.
