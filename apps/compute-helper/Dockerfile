# Builds the static compute helper binary into a scratch image.
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

FROM rust:1.98.1-alpine AS build

RUN rustup target add x86_64-unknown-linux-musl
WORKDIR /source
COPY . .
# Record the compiler and target the copied rust-toolchain.toml selects before building.
RUN rustc -vV && rustup target list --installed
RUN cargo build --locked --release --target x86_64-unknown-linux-musl -p aruna-compute-helper

FROM scratch
COPY --from=build /source/target/x86_64-unknown-linux-musl/release/aruna-compute-helper /aruna-compute-helper
USER 65534:65534
ENTRYPOINT ["/aruna-compute-helper"]
