# === Build Stage: Rust ===
FROM rust:1.83 AS builder

WORKDIR /usr/src/fusiondb
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY tests/ tests/

# Build release binary
RUN cargo build --release

# === Build Stage: Dashboard ===
FROM node:20-slim AS dashboard-builder

WORKDIR /app
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ .
RUN npm run build

# === Runtime Stage ===
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/bin

# Copy server binary
COPY --from=builder /usr/src/fusiondb/target/release/fusiondb .

# Copy dashboard static files
COPY --from=dashboard-builder /app/dist /usr/local/share/fusiondb/dashboard

# Create data directories
RUN mkdir -p /data/sstables /data/wal
WORKDIR /data

# Expose ports: HTTP API + PostgreSQL Protocol
EXPOSE 8091 8092

# Run the binary
CMD ["/usr/local/bin/fusiondb"]
