# Build Stage
FROM rust:1-alpine3.20 AS builder
WORKDIR /build
RUN apk update
RUN apk upgrade
ENV RUSTFLAGS="-C target-feature=-crt-static"
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev protoc protobuf-dev protobuf-dev libpq-dev musl-dev git
COPY . .
RUN cargo build --release -p data_proxy

FROM alpine:3.20
WORKDIR /run
RUN apk update
RUN apk upgrade
RUN apk add libgcc gcompat ca-certificates libpq
COPY --from=builder /build/target/release/data_proxy .
COPY --from=builder /build/components/data_proxy/.env .
COPY --from=builder /build/components/data_proxy/src/database/schema.sql .

CMD [ "/run/data_proxy" ]
