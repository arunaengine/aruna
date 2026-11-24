# Builds the workflow coordinator image: a Cargo release build, then a non-root runtime stage.
# Image: harbor.computational.bio.uni-giessen.de/aruna/coordinator
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

# The build context is the repository root, which holds the contract crate and its workspace.
FROM rust:1.99.1-trixie@sha256:8a4e90c7946b74626488b1446f712c0fe0ed1a596cbd3d871684bc9c5433ae07 AS builder
WORKDIR /build
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
# mold and clang back `.cargo/config.toml`, which only applies to the gnu target.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang cmake mold git \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --locked --manifest-path apps/coordinator/Cargo.toml \
    && mkdir -p /opt/aruna \
    && ln -s /run/aruna-coordinator /opt/aruna/session-helper

FROM gcr.io/distroless/cc-debian13@sha256:ed7c407fd64eb0af9dddb9456b94cee188a40a7f53cf38c9836e1e9ae14fca02
WORKDIR /run
COPY --from=builder /build/apps/coordinator/target/release/aruna-coordinator .
COPY --from=builder /opt/aruna /opt/aruna
# The node starts it with a read-only root and no network; it writes only below /tmp.
USER 65532:65532
ENTRYPOINT [ "/run/aruna-coordinator" ]
