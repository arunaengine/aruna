FROM rust:1.95-alpine AS build

RUN rustup target add x86_64-unknown-linux-musl
WORKDIR /source
COPY . .
RUN cargo build --locked --release --target x86_64-unknown-linux-musl -p aruna-compute-helper

FROM scratch
COPY --from=build /source/target/x86_64-unknown-linux-musl/release/aruna-compute-helper /aruna-compute-helper
USER 65534:65534
ENTRYPOINT ["/aruna-compute-helper"]
