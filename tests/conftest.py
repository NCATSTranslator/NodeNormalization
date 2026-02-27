import sys
import os
import time
import socket

import pytest
import logging
from testcontainers.compose import DockerCompose
from testcontainers.core.docker_client import DockerClient
from fastapi.testclient import TestClient

from node_normalizer.util import LoggingUtil

sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))

logger = LoggingUtil.init_logging()


@pytest.fixture(scope="session")
def session(request):
    logger.info("starting docker container")

    compose = DockerCompose(filepath=".", compose_file_name="docker-compose-test.yml", env_file=".env", build=True, pull=True)
    compose.start()
    nn_service_name = compose.get_service_host(service_name="r3", port=8080)
    nn_service_port = compose.get_service_port(service_name="r3", port=8080)
    nn_url = f"http://{nn_service_name}:{nn_service_port}"
    logger.info(f"nn_url: {nn_url}")
    compose.wait_for(f"{nn_url}")

    callback_service_name = compose.get_service_host(service_name="callback-app", port=8008)
    callback_service_port = compose.get_service_port(service_name="callback-app", port=8008)
    callback_url = f"http://{callback_service_name}:{callback_service_port}"
    logger.info(f"callback_url: {callback_url}")
    compose.wait_for(f"{callback_url}")

    (stdout, stderr, exit_code) = compose.exec_in_container(service_name="r3", command=["python", "load.py"])
    logger.info(f"stdout: {stdout}, stderr: {stderr}")

    logger.info(f"done building docker containers...ready to proceed")

    def stop():
        logger.info("stopping docker container")
        stdout, stderr = compose.get_logs()
        if stderr:
            logger.error(f"{stderr}")
        if stdout:
            logger.info(f"{stdout}")
        compose.stop()

    request.addfinalizer(stop)

    return compose, nn_url, callback_url


@pytest.fixture(scope="session")
def integration_client():
    """
    Session-scoped fixture for integration tests.

    Requires Redis running on localhost:6379. In CI this is provided by the
    GitHub Actions services.redis block in .github/workflows/test.yml.
    Locally: docker compose -f docker-compose-redis.yml up -d

    The TestClient context manager triggers startup_event(), which establishes
    real Redis connections via redis_config.yaml. All integration tests share
    this single session; tests must be read-only (no writes to Redis).

    Note: startup_event also downloads the Biolink Model YAML from GitHub via
    bmt. This is slow on the first run; bmt caches it in ~/.cache afterward.
    """
    # Verify Redis is reachable before starting the app; fail fast with a clear
    # message rather than a cryptic connection error from deep inside the app.
    redis_host, redis_port = "127.0.0.1", 6379
    try:
        with socket.create_connection((redis_host, redis_port), timeout=5):
            pass
    except OSError as exc:
        raise RuntimeError(
            f"Integration tests require Redis on {redis_host}:{redis_port}. "
            "Start it with: docker compose -f docker-compose-redis.yml up -d"
        ) from exc

    # PLACEHOLDER: load test data into Redis before starting the app.
    # Fill this in once tests/data/config.json is finalized, e.g.:
    #
    #   import asyncio
    #   from node_normalizer.loader import NodeLoader
    #   loader = NodeLoader("tests/data/config.json")
    #   asyncio.run(loader.load(100_000))

    from node_normalizer.server import app
    with TestClient(app) as client:
        yield client
