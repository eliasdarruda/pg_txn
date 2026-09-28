#!/usr/bin/env bash
# Builds the publishable npm packages into dist/npm/*.tgz: @pg-txn/client,
# @pg-txn/drizzle and @pg-txn/knex as JavaScript + .d.ts (the workspace runs
# the TypeScript sources; Node does not strip types inside node_modules).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=$ROOT/dist/npm
export PATH="$ROOT/.tools/node/bin:$PATH"
node "$ROOT/scripts/sync-schema.mjs" >/dev/null
rm -rf "$OUT" && mkdir -p "$OUT"

for pkg in client drizzle knex; do
  SRC=$ROOT/clients/typescript/$pkg
  STAGE=$OUT/$pkg
  mkdir -p "$STAGE"
  cat > "$OUT/tsconfig.$pkg.json" <<EOF
{
  "extends": "$ROOT/tsconfig.json",
  "compilerOptions": {
    "noEmit": false,
    "declaration": true,
    "rewriteRelativeImportExtensions": true,
    "rootDir": "$SRC/src",
    "outDir": "$STAGE/dist"
  },
  "include": ["$SRC/src/**/*.ts"]
}
EOF
  (cd "$ROOT" && npx tsc -p "$OUT/tsconfig.$pkg.json")
  node - "$SRC/package.json" "$STAGE/package.json" <<'EOF'
const fs = require("node:fs");
const [src, dst] = process.argv.slice(2);
const p = JSON.parse(fs.readFileSync(src, "utf8"));
const js = (f) => f.replace(/^\.\/src\//, "./dist/").replace(/\.ts$/, ".js");
const exports = {};
for (const [k, v] of Object.entries(p.exports)) exports[k] = { types: js(v).replace(/\.js$/, ".d.ts"), default: js(v) };
fs.writeFileSync(dst, JSON.stringify({
  name: p.name, version: p.version, type: "module", license: p.license ?? "Apache-2.0",
  engines: { node: ">=20" }, files: ["dist"], exports,
  dependencies: p.dependencies, peerDependencies: p.peerDependencies,
}, null, 2) + "\n");
EOF
  cp "$ROOT/clients/typescript/README.md" "$ROOT/LICENSE" "$STAGE/"
  (cd "$OUT" && npm pack --silent "$STAGE" >/dev/null)
  rm -f "$OUT/tsconfig.$pkg.json"
done
ls -la "$OUT"/*.tgz
