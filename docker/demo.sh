#!/bin/bash
# Registers the demo sinks and writes a few rows to both regions, so the console has something to show and the
# README's example has something to query. Safe to run twice: a sink that exists already is left alone.
set -euo pipefail

api="${WALRUS_API:-http://127.0.0.1:5190}"
operator="Authorization: Bearer ${WALRUS_OPERATOR_TOKEN:-walrus-operator-token-for-local-use-only}"

register() {
    code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${api}/v1/sinks" -H "${operator}" -H 'content-type: application/json' -d "$1")
    case "${code}" in
        201) echo "registered $(echo "$1" | sed -E 's/.*"id":"([^"]+)".*/\1/')" ;;
        409) ;;
        *) echo "registering failed with ${code}: $1" >&2; exit 1 ;;
    esac
}

# A replica of both tables in another database, bootstrapped from a snapshot, and a search index over them.
register '{"id":"replica","kind":"Postgres","tables":["public.accounts","public.profiles"],"target":"replica","start":"Snapshot"}'
register '{"id":"search","kind":"Index","tables":["public.accounts","public.profiles"],"start":"Beginning"}'

eu() { docker compose exec -T pg-a psql -U postgres -d shop -qc "$1" 2>/dev/null || docker compose exec -T pg-b psql -U postgres -d shop -qc "$1"; }
us() { docker compose exec -T pg-c psql -U postgres -d shop -qc "$1"; }

eu "insert into accounts (id, owner, email, balance, seq) values (1, 'ada', 'ada@example.com', 100, 1), (2, 'grace', 'grace@example.com', 50, 1) on conflict (id) do nothing"

# One profile both regions edit, in different columns. profiles is a merge table, so both edits survive.
eu "insert into profiles values (7, 'linus', 'Helsinki', 'free') on conflict (id) do nothing"
us "insert into profiles values (7, 'linus', 'Helsinki', 'free') on conflict (id) do nothing"
us "update profiles set plan = 'team' where id = 7"
eu "update profiles set city = 'Portland' where id = 7"

echo "demo data written"
