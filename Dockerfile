# Build Stage
FROM rust:1.83 as builder

WORKDIR /usr/src/fusiondb
COPY . .

# Build the release binary
RUN cargo build --release

# Runtime Stage
FROM debian:bookworm-slim

# Install OpenSSL/Ca-certificates if needed (depending on dependencies)
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/bin

# Copy the binary from builder
COPY --from=builder /usr/src/fusiondb/target/release/fusiondb .

# Create data directories
RUN mkdir -p /data/sstables /data/wal
WORKDIR /data

# Expose ports
# 5432: PostgreSQL Protocol
# 8080: HTTP API (if enabled)
EXPOSE 5432 8080

# Run the binary
CMD ["/usr/local/bin/fusiondb"]
