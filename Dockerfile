# === Build Stage: Rust ===
FROM rust:1.89-bookworm AS builder

WORKDIR /usr/src/fusiondb
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY tests/ tests/

# Build release binary
RUN cargo build --release --locked

# === Runtime Stage ===
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/bin

# Copy server binary
COPY --from=builder /usr/src/fusiondb/target/release/fusiondb .

# Install a container-specific config outside the data volume.
RUN mkdir -p /data/sstables /etc/fusiondb
COPY docker/fusiondb.toml /etc/fusiondb/fusiondb.toml
WORKDIR /etc/fusiondb

# Expose ports: HTTP API + PostgreSQL Protocol
EXPOSE 8091 8092

# Run the binary
CMD ["/usr/local/bin/fusiondb"]
