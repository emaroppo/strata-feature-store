#!/usr/bin/env bash
# Bring a fresh Garage container to the point where it accepts writes, and
# print credentials for it.
#
# Garage answers on its ports well before it will store anything: a new node
# has no layout, and without one every PUT fails with an unhelpful error. That
# is the step people lose an afternoon to, and it is why this script exists
# rather than a services: block in CI.
#
# Idempotent. Running it against an already-bootstrapped instance re-prints
# the credentials rather than failing.
set -euo pipefail

COMPOSE="${COMPOSE:-docker compose}"
BUCKET="${BUCKET:-feature-store}"
KEY_NAME="${KEY_NAME:-feature-store-dev}"

g() { $COMPOSE exec -T storage /garage "$@"; }

echo "==> waiting for garage to answer"
for _ in $(seq 1 60); do
    if g status >/dev/null 2>&1; then break; fi
    sleep 1
done
g status >/dev/null 2>&1 || { echo "garage never came up" >&2; exit 1; }

# The node id is the first field of the line marked NO ROLE ASSIGNED, or of
# the node itself once a role exists.
NODE_ID="$(g node id -q 2>/dev/null | cut -d@ -f1)"
echo "==> node ${NODE_ID}"

if g layout show 2>/dev/null | grep -q "NO ROLE ASSIGNED"; then
    echo "==> assigning layout"
    g layout assign -z dc1 -c 1G "$NODE_ID"
    # apply needs the version it is moving to, which is current + 1.
    g layout apply --version 1
else
    echo "==> layout already assigned"
fi

if ! g bucket info "$BUCKET" >/dev/null 2>&1; then
    echo "==> creating bucket ${BUCKET}"
    g bucket create "$BUCKET"
fi

if ! g key info "$KEY_NAME" >/dev/null 2>&1; then
    echo "==> creating key ${KEY_NAME}"
    g key create "$KEY_NAME" >/dev/null
fi
g bucket allow --read --write --owner "$BUCKET" --key "$KEY_NAME" >/dev/null

echo
echo "==> credentials — export these, or put them in .env"
g key info "$KEY_NAME" --show-secret | awk '
    /Key ID:/     { printf "FEATURE_STORE_S3_ACCESS_KEY=%s\n", $3 }
    /Secret key:/ { printf "FEATURE_STORE_S3_SECRET_KEY=%s\n", $3 }
'
