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

### `tests/test_loader.py::test_nn_load`

**Status:** Passes, but emits a `RuntimeWarning: coroutine 'NodeLoader.load_compendium' was never awaited`.

**Root cause:** `load_compendium` is an `async` method but `test_nn_load` calls it
synchronously: `assert node_loader.load_compendium(good_json, 5)`. This passes today
only because the return value of an unawaited coroutine is truthy. The actual loading
logic never executes.

**Estimated fix:** Refactor the test to `await` the call, or add a synchronous wrapper.
Low effort (~5 minutes) but requires understanding whether the test was intentionally
bypassing the async path via `_test_mode = 1`.
