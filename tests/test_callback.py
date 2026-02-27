"""Test node_normalizer server.py"""
import json
import time

import pytest
import reasoner_pydantic
import requests
import fastapi
from pathlib import Path

from node_normalizer.util import LoggingUtil

logger = LoggingUtil.init_logging()

premerged_response = Path(__file__).parent / "resources" / "premerged_response.json"


@pytest.mark.skip(reason="Requires external callback-app container. See docker-compose-test.yml.")
def test_async_query_callback(session):

    # host_url = "http://r3:8080/asyncquery"
    # host_url = f"{session[1]}/asyncquery"
    host_url = "http://127.0.0.1:8080/asyncquery"
    logger.info(f"host_url: {host_url}")

    # callback_url = f"http://callback-app:8008/receive_callback_query"
    # callback_url = f"{session[2]}/receive_callback_query"
    callback_url = f"http://callback-app:8008/receive_callback_query"
    logger.info(f"callback_url: {callback_url}")

    with open(premerged_response, "r") as pre:
        premerged_data = json.load(pre)

    premerged_data["callback"] = callback_url

    async_query = reasoner_pydantic.message.AsyncQuery.parse_obj(premerged_data)

    response = requests.post(url=host_url, headers={"Content-Type": "application/json", "Accept": "application/json"}, data=async_query.json())

    # sleep here to allow the /asyncquery background task to run through
    time.sleep(10)

    assert response.status_code == 200
    assert response.json() == {"description": f"Query commenced. Will send result to {callback_url}"}

    compose_stdout, compose_stderr = session[0].get_logs()
    assert b'Successfully received message from "http://callback-app:8008/receive_callback_query"' in compose_stdout


