"""
Shared configuration and path resolution for both the NodeNorm frontend and
the loader.

These constants are the single source of truth for repo-relative paths. Keeping
them here — in a module that does not move — means code that lives deeper in the
package (e.g. node_normalizer/loader/) does not have to reason about how many
parent directories separate __file__ from the repo root.
"""
import json
from pathlib import Path

# node_normalizer/ is one level below the repo root.
REPO_ROOT = Path(__file__).parents[1]
CONFIG_PATH = REPO_ROOT / "config.json"
REDIS_CONFIG_PATH = REPO_ROOT / "redis_config.yaml"
RESOURCES_DIR = Path(__file__).parent / "resources"


def get_config() -> dict:
    """Load and return the parsed config.json."""
    with open(CONFIG_PATH, "r") as config_file:
        return json.load(config_file)
