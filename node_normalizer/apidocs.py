"""
Open API configuration

# TODO use examples from openapi.yml to drive examples
"""

from pathlib import Path
from typing import Dict

from yaml import load, SafeLoader
from fastapi.openapi.utils import get_openapi
import os


def get_app_info() -> Dict[str, str]:
    """
    Get title, version, description from openapi.yml
    """
    with open(Path(__file__).parent / 'resources' / 'openapi.yml', 'r') as apd_file:
        api_docs = load(apd_file, Loader=SafeLoader)

    return {
        k : v for k,v in api_docs['info'].items() if k in [
            'title',
            'version',
            'description'
        ]
    }


def construct_open_api_schema(app) -> Dict[str, str]:
    """
    Constructs open api schema
    https://fastapi.tiangolo.com/advanced/extending-openapi/
    """

    with open(Path(__file__).parent / 'resources' / 'openapi.yml', 'r') as apd_file:
        api_docs = load(apd_file, Loader=SafeLoader)

    if app.openapi_schema:
        return app.openapi_schema()

    open_api_schema = get_openapi(
        title=api_docs['info']['title'],
        version=api_docs['info']['version'],
        routes=app.routes
    )

    if 'tags' in api_docs:
        open_api_schema['tags'] = api_docs['tags']

    if 'x-translator' in api_docs['info']:
        open_api_schema['info']['x-translator'] = api_docs['info']['x-translator']

    if 'x-trapi' in api_docs['info']:
        open_api_schema['info']['x-trapi'] = api_docs['info']['x-trapi']

    if 'contact' in api_docs['info']:
        open_api_schema['info']['contact'] = api_docs['info']['contact']

    if 'termsOfService' in api_docs['info']:
        open_api_schema['info']['termsOfService'] = api_docs['info']['termsOfService']

    if 'description' in api_docs['info']:
        open_api_schema['info']['description'] = api_docs['info']['description']

    # adds support to override server root path
    server_root = os.environ.get('SERVER_ROOT', '/')
    # make sure not to add double slash at the end.
    server_root = server_root.rstrip('/') + '/'
    if 'servers' in api_docs:
        for s in api_docs['servers']:
            # override if server root env var is provided
            s['url'] = server_root + os.environ.get("TRAPI_VERSION", "1.5") if server_root != '/' else s['url']
            s['x-maturity'] = os.environ.get("MATURITY_VALUE", "maturity")
            s['x-location'] = os.environ.get("LOCATION_VALUE", "location")
        open_api_schema['servers'] = api_docs['servers']

    return open_api_schema
