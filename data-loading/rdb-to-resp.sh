#!/usr/bin/env bash
#
# A Bash script to convert an RDB file into a stream of RESP commands.
#
# bash rdb-to-resp.sh RDB_FILENAME
set -euo pipefail

# Check the number of arguments
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <RDB_FILENAME>" >&2
    exit 1
fi

rdb-cli "$1" -l stdout -s 1000 -d 0 resp