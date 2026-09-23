#!/bin/bash
# Entry point for one node of a two-node source cluster that can fail over in either direction.
#
# A node decides its role every time it starts, by asking the other node:
#
#   - The other node is a primary: this node must be a standby. With no data yet it takes a base backup. With
#     data from a previous life as primary it rewinds that data onto the other node's timeline first, which is
#     what makes a killed primary able to come back as a standby without a rebuild.
#   - Otherwise: start with whatever data is there. On the very first start that means initialising a fresh
#     primary; after a restart it means carrying on as whatever it was.
#
# The failover harness is then just: kill the primary, promote the standby, start the old primary again.
#
# NODE is this node's name and PEER the other's. Each name is also the physical slot the node streams through
# when it is the standby, which is why synchronized_standby_slots on each node names the other.
set -euo pipefail

: "${NODE:?NODE is required}"
: "${PEER:?PEER is required}"

export PGPASSWORD="${POSTGRES_PASSWORD}"
export PGCONNECT_TIMEOUT=3
data="${PGDATA}"

peer_is_primary() {
    [ "$(psql -h "${PEER}" -U postgres -d postgres -Atc 'select not pg_is_in_recovery()' 2>/dev/null || true)" = "t" ]
}

ensure_slot_on_peer() {
    # The slot this node will stream through. Created without reserving log, so it holds nothing until the node
    # actually connects.
    psql -h "${PEER}" -U postgres -d postgres -Atc \
        "select pg_create_physical_replication_slot('${NODE}') where not exists (select 1 from pg_replication_slots where slot_name = '${NODE}')" \
        >/dev/null
}

if peer_is_primary; then
    ensure_slot_on_peer

    mkdir -p "${data}"
    chown postgres:postgres "${data}"
    chmod 700 "${data}"

    # pg_rewind refuses to run as root, and files it or pg_basebackup write must belong to postgres anyway.
    if [ ! -s "${data}/PG_VERSION" ]; then
        echo "node ${NODE}: no data and ${PEER} is primary; taking a base backup"
        gosu postgres pg_basebackup -h "${PEER}" -U postgres -D "${data}" -X stream -S "${NODE}"
        gosu postgres touch "${data}/standby.signal"
    elif [ ! -f "${data}/standby.signal" ]; then
        echo "node ${NODE}: was a primary and ${PEER} is primary now; rewinding onto its timeline"
        gosu postgres pg_rewind --target-pgdata="${data}" --source-server="host=${PEER} user=postgres dbname=postgres" --progress
        gosu postgres touch "${data}/standby.signal"
    else
        echo "node ${NODE}: already a standby of ${PEER}"
    fi
fi

exec docker-entrypoint.sh "$@"
