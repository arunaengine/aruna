#!/bin/sh
# Builds ARUNA_SOURCE_DIR in writable scratch space into ARUNA_OUTPUT_DIR, offline when possible.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0
set -eu
SRC="${ARUNA_SOURCE_DIR:-/work}"
OUT="${ARUNA_OUTPUT_DIR:-/work/dist}"
STARTER="${ARUNA_STARTER_DIR:-/starter}"
if [ ! -d "$SRC" ]; then
    echo "aruna-build: source directory $SRC does not exist" >&2
    exit 2
fi
WORK="$(mktemp -d "${TMPDIR:-/tmp}/aruna-build.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
cp -R "$SRC"/. "$WORK"/
chmod -R u+w "$WORK"
cd "$WORK"
if [ ! -f package.json ] || [ ! -f package-lock.json ]; then
    echo "aruna-build: $SRC needs package.json and package-lock.json" >&2
    exit 2
fi
rm -rf node_modules
# Offline when every locked package is in the starter at the same version and integrity.
if node -e '
const fs = require("fs");
const read = (file) => JSON.parse(fs.readFileSync(file, "utf8")).packages ?? {};
const [mine, starter] = process.argv.slice(1).map(read);
const same = Object.entries(mine).every(([path, entry]) =>
  path === "" || (starter[path] && starter[path].version === entry.version &&
    starter[path].integrity === entry.integrity));
process.exit(same ? 0 : 1);
' package-lock.json "$STARTER/package-lock.json"; then
    ln -sfn "$STARTER/node_modules" node_modules
elif [ -n "${NPM_CONFIG_REGISTRY:-}" ]; then
    npm ci
else
    echo "aruna-build: the lockfile needs packages the starter lacks, and no NPM_CONFIG_REGISTRY is set" >&2
    exit 3
fi
export ARUNA_PREVIEW_SOURCE_MAP=1
node node_modules/vite/bin/vite.js build --configLoader runner --outDir "$WORK/dist" --emptyOutDir
mkdir -p "$OUT"
cp -R "$WORK/dist"/. "$OUT"/
cp "$WORK/package-lock.json" "$OUT/package-lock.json"
