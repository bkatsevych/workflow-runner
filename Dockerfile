# Use a Rust base image
FROM rust:latest as builder

# Set working directory inside the container
WORKDIR /usr/src/app

# Copy the entire local project into the container
COPY . .

# Build the application (if needed)
# RUN cargo build --release

# Define entrypoint
ENTRYPOINT ["cargo", "run", "--"]

