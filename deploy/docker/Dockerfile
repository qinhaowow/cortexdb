FROM rust:1.75-alpine AS builder

WORKDIR /app

RUN apk add --no-cache openssl-dev musl-dev

COPY Cargo.toml ./
COPY Cargo.lock ./
COPY src/ ./src/

RUN cargo build --release --bin cortex

FROM alpine:3.19

RUN apk add --no-cache ca-certificates curl libstdc++ openssl

WORKDIR /app

COPY --from=builder /app/target/release/cortex ./cortex
COPY --from=builder /app/LICENSE* ./
COPY --from=builder /app/README.md ./

ENV CORTEXDB_DATA_DIR=/data
ENV CORTEXDB_LOG_DIR=/var/log/coretexdb

VOLUME ["/data", "/var/log/coretexdb"]

EXPOSE 8080 50051

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/api/health || exit 1

ENTRYPOINT ["./cortex"]

LABEL \
    maintainer="CoretexDB Team <support@coretexdb.io>" \
    coretexdb.version="${VERSION:-latest}" \
    coretexdb.mode="standalone"
