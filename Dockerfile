# Use the official Rust image as the base image
FROM rust:latest AS builder

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Copy the source code
COPY src ./src
COPY semantic_models.json ./semantic_models.json

# Build the application
RUN cargo build --release

# Use a minimal base image for the final container
FROM debian:bookworm-slim

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the compiled binaries from the builder stage
COPY --from=builder /usr/src/app/target/release/development .
COPY --from=builder /usr/src/app/target/release/production .
COPY --from=builder /usr/src/app/semantic_models.json ./semantic_models.json
# Expose the port that the server will run on
EXPOSE 5432

# Install Doppler CLI
RUN apt-get update \
    && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    openssl \
    build-essential \
    && curl -sLf --retry 3 --tlsv1.2 --proto "=https" 'https://packages.doppler.com/public/cli/gpg.DE2A7741A397C129.key' | gpg --dearmor -o /usr/share/keyrings/doppler-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/doppler-archive-keyring.gpg] https://packages.doppler.com/public/cli/deb/debian any-version main" | tee /etc/apt/sources.list.d/doppler-cli.list \
    && apt-get update \
    && apt-get -y install doppler

# Command to run the application
CMD ["doppler", "run", "--", "./production"]
