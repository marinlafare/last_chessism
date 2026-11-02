#!/bin/bash
set -euo pipefail

# Local Compose secrets are bind mounts and retain the host UID and mode.
# Read the owner-only file during root bootstrap; the official entrypoint then
# drops to the neo4j user. Never make the host credential world-readable.
if [[ ! -r "${NEO4J_AUTH_FILE:?Neo4j secret file is required}" ]]; then
    echo 'Neo4j authentication secret is unreadable.' >&2
    exit 1
fi
NEO4J_AUTH="$(<"${NEO4J_AUTH_FILE}")"
if [[ "${NEO4J_AUTH}" != neo4j/?* || "${NEO4J_AUTH}" == neo4j/neo4j ]]; then
    echo 'Neo4j authentication secret must contain neo4j/a-private-password.' >&2
    exit 1
fi
export NEO4J_AUTH
unset NEO4J_AUTH_FILE
exec /startup/docker-entrypoint.sh "$@"
