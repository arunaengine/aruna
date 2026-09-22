# Builds the node image: a Cargo release build, then a distroless runtime stage.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

# glibc, not musl: musl's `cmsghdr` is 4-byte aligned, so noq-udp's receive
# timestamp decode trips its alignment assertion and aborts the process.
FROM rust:1.97.1-trixie@sha256:1bcff4befb740599103a2c7cb51058e14479b2e35e3a34a3f0dc4ede09927488 AS builder
WORKDIR /build
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
# mold and clang back `.cargo/config.toml`, which only applies to the gnu target.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang cmake mold git gpg gpg-agent python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*
# Stage the website build in .portal-embed, as the image helper does.
# An absent staging directory produces a headless image; PORTAL_EMBED_DIR can override it.
ARG PORTAL_EMBED_DIR=.portal-embed
COPY . .
RUN python3 -m venv /opt/arctrl \
    && /opt/arctrl/bin/pip install --no-cache-dir -r blob/arc-requirements.txt
RUN cargo build --release --locked -p aruna
RUN cargo build --release --locked -p aruna-doctor
RUN cargo install --locked --version 0.101.0 --root target iroh-doctor
# The runtime image has no shell, so copy the staged portal in the builder.
RUN mkdir -p /portal ${PORTAL_EMBED_DIR} && cp -r ${PORTAL_EMBED_DIR}/. /portal/
RUN mkdir -p /git-runtime/usr/bin /git-runtime/usr/lib/git-core \
    && cp /usr/bin/git /usr/bin/gpg /usr/bin/gpg-agent /usr/bin/gpgconf /git-runtime/usr/bin/ \
    && cp -L /usr/bin/python3 /git-runtime/usr/bin/python3 \
    && ln -s python3 /git-runtime/usr/bin/python3.13 \
    && mkdir -p /git-runtime/usr/lib \
    && cp -a /usr/lib/python3.13 /git-runtime/usr/lib/ \
    && cp /usr/lib/git-core/git-http-backend /git-runtime/usr/lib/git-core/ \
    && ldd /usr/bin/git /usr/lib/git-core/git-http-backend /usr/bin/gpg /usr/bin/gpg-agent \
       /usr/bin/gpgconf /usr/bin/python3 /usr/lib/python3.13/lib-dynload/*.so \
       | awk '/=> \// { print $3 }' | sort -u \
       | xargs -I '{}' cp --parents '{}' /git-runtime/

FROM gcr.io/distroless/cc-debian13@sha256:ed7c407fd64eb0af9dddb9456b94cee188a40a7f53cf38c9836e1e9ae14fca02
WORKDIR /run
COPY --from=builder /git-runtime/ /
COPY --from=builder /opt/arctrl/ /opt/arctrl/
ENV PATH=/opt/arctrl/bin:/usr/bin:/bin
COPY --from=builder /build/target/release/aruna .
COPY --from=builder /build/target/release/aruna-doctor .
COPY --from=builder /build/target/bin/iroh-doctor .
COPY --from=builder /portal/ /run/portal/
# Leave PORTAL_MODE unset: process env would override mounted /run/.env and pin the portal off.
# PORTAL_DIR points at the embedded copy.
ENV PORTAL_DIR=/run/portal

CMD [ "/run/aruna" ]
