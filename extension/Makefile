# Installs pg_txn as an extension on a self-managed server:
#   make install && psql -c 'CREATE EXTENSION pg_txn'
# (The SDKs can also install the same schema themselves, or run sql/pg_txn--1.0.sql as a migration.)
EXTENSION = pg_txn
DATA = sql/pg_txn--1.0.sql
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
