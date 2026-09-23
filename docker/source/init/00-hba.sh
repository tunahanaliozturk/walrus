#!/bin/bash
# The image allows ordinary connections from the network but not physical replication, which the standby and
# pg_rewind need. Logical replication connects to a named database and is covered by the existing rule.
set -euo pipefail
echo "host replication all all scram-sha-256" >> "${PGDATA}/pg_hba.conf"
