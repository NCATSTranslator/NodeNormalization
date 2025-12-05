#!/usr/bin/env bash
#
# A Bash script to convert an RDB file into a stream of RESP commands.
# Uses https://github.com/redis/librdb?tab=readme-ov-file#rdb-cli-usage
#
# bash rdb-to-resp.sh RDB_FILENAME [REDIS_VERSION]
set -euo pipefail

# Check the number of arguments
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <RDB_FILENAME>" >&2
    exit 1
fi
REDIS_VERSION="${2:-6.2}" # Default to a Redis version of 6.2

rdb-cli "$1" --show-progress 1000 resp --target-redis-ver "${REDIS_VERSION}"