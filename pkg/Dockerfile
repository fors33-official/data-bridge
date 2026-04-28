# Use a modern Rust toolchain (deps currently require rustc >= 1.88).
FROM rust:1.88-bookworm AS builder

WORKDIR /app
COPY . .

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    pkg-config \
    build-essential \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

ARG CARGO_BUILD_FLAGS="--features full_engine"
RUN cargo build --release $CARGO_BUILD_FLAGS

FROM debian:13-slim

LABEL org.opencontainers.image.version="0.4.0"

RUN apt-get update && apt-get upgrade -y --no-install-recommends && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

RUN useradd -m bridge && mkdir -p /app/config && chown -R bridge:bridge /app
WORKDIR /app

COPY --from=builder /app/target/release/t3thr /usr/local/bin/t3thr

USER bridge

ENTRYPOINT ["t3thr"]
