# Builds the applet builder image: the starter's libraries pre-installed for offline builds.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

# node:24-trixie-slim, pinned by its index digest so every rebuild starts from the same base.
FROM node:24-trixie-slim@sha256:4d00acb4bcc42c454c4eb801526f7ad7f772ea3bd4fd168c5cc2e384abcbb9d4

# Install scripts never run, and npm writes only to /tmp, so a read-only root works.
ENV NPM_CONFIG_IGNORE_SCRIPTS=true \
    NPM_CONFIG_UPDATE_NOTIFIER=false \
    NPM_CONFIG_FUND=false \
    NPM_CONFIG_AUDIT=false \
    NPM_CONFIG_CACHE=/tmp/npm-cache \
    HOME=/tmp \
    ARUNA_PREVIEW_SOURCE_MAP=1

WORKDIR /starter
COPY starter/package.json starter/package-lock.json ./
RUN npm ci && rm -rf /tmp/npm-cache
COPY starter/ ./
COPY build.sh /usr/local/bin/aruna-build

USER node
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/aruna-build"]
