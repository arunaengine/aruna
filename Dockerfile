# Build Stage
FROM rust:1-alpine3.23 AS builder
WORKDIR /build
RUN apk update
RUN apk upgrade
ENV RUSTFLAGS="-C target-feature=-crt-static"
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN apk add llvm cmake gcc ca-certificates libc-dev pkgconfig openssl-dev libpq-dev musl-dev git curl #protoc protobuf-dev protobuf-dev 
COPY . .
RUN cargo build --release -p aruna
RUN cargo build --release -p aruna-doctor

FROM alpine:3.23
WORKDIR /run
RUN apk update
RUN apk upgrade
RUN apk add libgcc gcompat ca-certificates libpq
COPY --from=builder /build/target/release/aruna .
COPY --from=builder /build/target/release/aruna-doctor .

CMD [ "/run/aruna" ]
