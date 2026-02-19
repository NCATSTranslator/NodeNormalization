# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NodeNormalization is a FastAPI microservice for the NIH NCATS Translator project. It normalizes biomedical CURIEs (Compact URIs) and finds equivalent identifiers across databases. Given a CURIE, it returns the preferred CURIE, equivalent identifiers, and Biolink semantic types.

Data comes from Babel (identifier equivalence project) and is stored in Redis across 7 separate databases. The service supports both standalone and clustered Redis.

## Common Commands

**Setup:**
```bash
python -m venv nodeNormalization-env
source nodeNormalization-env/bin/activate
pip install -r requirements.txt
```

**Run web server:**
```bash
uvicorn --host 0.0.0.0 --port 8000 --workers 1 node_normalizer.server:app
# API docs at http://localhost:8000/docs
```

**Run with Docker:**
```bash
docker-compose up  # Starts Redis + web service on port 8080
```

**Load data into Redis:**
```bash
python load.py  # Requires Redis running and compendia files in configured directory
```

**Run tests:**
```bash
pytest                          # All tests
pytest tests/test_endpoints.py  # Single test file
pytest tests/test_endpoints.py::test_function_name  # Single test
```

**Formatting:**
```bash
black --line-length 160 .
```

## Architecture

### Data Flow
```
Babel Compendia Files → loader.py → Redis (7 DBs) → FastAPI (server.py) → REST/TRAPI responses
```

### Redis Database Layout (`redis_config.yaml`)
| DB | Name | Purpose |
|----|------|---------|
| 0 | eq_id_to_id_db | Equivalent ID → canonical ID |
| 1 | id_to_eqids_db | ID → all equivalent IDs |
| 2 | id_to_type_db | ID → semantic types |
| 3 | curie_to_bl_type_db | CURIE → Biolink types |
| 4 | info_content_db | Information content scores |
| 5 | gene_protein_db | Gene/protein conflation |
| 6 | chemical_drug_db | Chemical/drug conflation |

### Key Modules

- **`node_normalizer/server.py`** — FastAPI app with all REST endpoints. Uses lifespan events for Redis connection setup/teardown. Root path is `/1.3`.
- **`node_normalizer/normalizer.py`** — Core logic: `get_normalized_nodes()`, `normalize_message()` (for TRAPI), and equivalent CURIE discovery. Traverses Biolink Model ancestors for semantic type expansion.
- **`node_normalizer/loader.py`** — `NodeLoader` class that reads flat compendia files and populates Redis. Validates input against `resources/valid_data_format.json`. Batch size: 100,000.
- **`node_normalizer/redis_adapter.py`** — `RedisConnectionFactory` and `RedisConnection` classes abstracting both clustered and standalone Redis, with async and sync support.
- **`node_normalizer/model/`** — Pydantic request/response models (`input.py`, `response.py`).
- **`config.json`** — Lists compendia and conflation files to load, preferred name boost prefixes, and feature flags (test mode, debug).

### Key API Endpoints
- `GET/POST /get_normalized_nodes` — Main normalization endpoint; accepts CURIE list
- `GET/POST /get_setid` — Deterministic hash for a set of CURIEs
- `GET /get_semantic_types` — Lists available Biolink semantic types
- `GET /get_curie_prefixes` — Lists CURIE prefixes per semantic type
- `POST /query` — Normalizes full TRAPI response objects
- `GET /status` — Health check with database info

### Docker Images
Two separate images are built and released to `ghcr.io`:
1. Main webserver (`Dockerfile`) — uvicorn entry point
2. Data loader (`data-loading/Dockerfile`) — for loading Babel compendia into Redis

### Testing
- Uses `pytest-asyncio` (async mode enabled via `pytest.ini`)
- Redis testcontainers for isolated integration tests
- Fixtures in `tests/conftest.py`
- Test data in `tests/resources/`
