#!/usr/bin/env bash
# Full verification run against stock PostgreSQL images (pg_txn installs
# itself as a non-superuser): PostgreSQL 18, 14 and PgBouncer in transaction
# mode, Node and Bun, the TypeScript and Elixir clients, application replicas
# in containers and a Kubernetes Deployment. Prints a summary table.
#
#   scripts/test-all.sh
#   ONLY="unit core" scripts/test-all.sh      # groups: unit core compat bun elixir containers k8s
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
export PATH="$ROOT/.tools/node/bin:$PATH"
LOG_DIR=${LOG_DIR:-$ROOT/tests/.logs}
mkdir -p "$LOG_DIR"
declare -a RESULTS=()
FAILED=0

want() { [ -z "${ONLY:-}" ] || [[ " $ONLY " == *" $1 "* ]]; }

step() { # name command...
  local name=$1
  shift
  local log="$LOG_DIR/$name.log"
  local t0=$(date +%s)
  printf '== %-24s ' "$name"
  if "$@" >"$log" 2>&1; then
    RESULTS+=("PASS  $name ($(( $(date +%s) - t0 ))s)")
    echo "PASS ($(( $(date +%s) - t0 ))s)"
  else
    RESULTS+=("FAIL  $name ($(( $(date +%s) - t0 ))s)  -> $log")
    echo "FAIL ($(( $(date +%s) - t0 ))s) see $log"
    FAILED=1
  fi
}

node_test() { # globs... (serial: suites share one database)
  node --test --test-concurrency=1 --test-timeout=600000 --test-reporter=spec "$@"
}

fresh_servers() {
  docker compose --profile matrix down -v --remove-orphans &&
    docker compose --profile matrix up -d --wait
}

want unit && step schema-sync node scripts/sync-schema.mjs
want unit && step typecheck npx tsc -p tsconfig.json
want unit && step ts-unit node_test 'clients/typescript/*/test/*.test.ts'

step fresh-servers fresh_servers

want core && step core-pg18 node_test 'tests/core/*.test.ts'
if want compat; then
  step core-pg14 env PG_TXN_URL=postgres://app:app@localhost:55433/app bash -c "$(declare -f node_test); node_test 'tests/core/*.test.ts'"
  step core-pgbouncer env PG_TXN_URL=postgres://app:app@localhost:55434/app bash -c "$(declare -f node_test); node_test 'tests/core/*.test.ts'"
fi
want bun && step core-bun "$ROOT/.tools/bun" test --timeout 600000 ./tests/core

if want elixir; then
  step elixir-image docker build -q -t pg-txn-elixir -f docker/Dockerfile.elixir .
  step elixir-ecto docker run --rm --network host -v "$ROOT:/work" -e MIX_ENV=test \
    -e PG_TXN_ECTO_URL=ecto://app:app@localhost:55432/app pg-txn-elixir mix test
fi

# the npm package in application containers (Node slim/alpine, Bun) and as a
# Kubernetes Deployment (k3s in Docker; the cluster container is reused)
if want containers || want k8s; then
  step pack-npm scripts/pack-npm.sh
fi
want containers && step containers-horizontal node_test 'tests/containers/*.test.ts'
want k8s && step k8s-deployment node_test 'tests/k8s/*.test.ts'

echo
echo "================ summary ================"
printf '%s\n' "${RESULTS[@]}"
exit $FAILED
