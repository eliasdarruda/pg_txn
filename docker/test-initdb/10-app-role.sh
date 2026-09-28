#!/bin/bash
# The tests connect as an ordinary role that owns its database (as on a
# managed service): pg_txn installs itself without superuser rights.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
  CREATE ROLE app LOGIN PASSWORD 'app';
  CREATE DATABASE app OWNER app;
EOSQL
