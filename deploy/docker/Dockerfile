# CortexDB Dockerfile
FROM rust:1.70 as builder

WORKDIR /app

# Copy source code
COPY . .

# Build CortexDB
RUN cargo build --release

# Final image
FROM debian:bullseye-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy binary
COPY --from=builder /app/target/release/cortex /usr/local/bin/

# Expose ports
EXPOSE 8000 9000 5432

# Set environment variables
ENV RUST_LOG=info

# Run CortexDB
CMD ["cortex", "--host", "0.0.0.0", "--port", "8000"]