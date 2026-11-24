#!/bin/sh
# Builds the applet source in /work into /work/dist, offline when the starter has every package.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0
set -eu
cd /work
if [ ! -f package.json ] || [ ! -f package-lock.json ]; then
    echo "aruna-build: /work needs package.json and package-lock.json" >&2
    exit 2
fi
# Offline when every locked package is in the starter at the same version and integrity.
if node -e '
const fs = require("fs");
const read = (file) => JSON.parse(fs.readFileSync(file, "utf8")).packages ?? {};
const [mine, starter] = process.argv.slice(1).map(read);
const same = Object.entries(mine).every(([path, entry]) =>
  path === "" || (starter[path] && starter[path].version === entry.version &&
    starter[path].integrity === entry.integrity));
process.exit(same ? 0 : 1);
' package-lock.json /starter/package-lock.json; then
    ln -sfn /starter/node_modules node_modules
elif [ -n "${NPM_CONFIG_REGISTRY:-}" ]; then
    npm ci
else
    echo "aruna-build: the lockfile needs packages the starter lacks, and no NPM_CONFIG_REGISTRY is set" >&2
    exit 3
fi
export ARUNA_PREVIEW_SOURCE_MAP=1
exec node node_modules/vite/bin/vite.js build --configLoader runner
