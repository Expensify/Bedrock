#!/bin/bash

# Set the script to exit immediately if a command exits with a non-zero status.
set -e

# Define variables
IMAGE_NAME="bedrock-build"
CONTAINER_NAME="bedrock-build-container"
OUTPUT_DIR="../output"

# Ensure the output directory exists
mkdir -p "$OUTPUT_DIR"

echo "Building Docker image..."
docker buildx build -t "$IMAGE_NAME" -f Dockerfile ..

echo "Creating container..."
docker create --name "$CONTAINER_NAME" "$IMAGE_NAME"

echo "Copying bedrock binary from container..."
docker cp "$CONTAINER_NAME:/usr/local/bin/bedrock" "$OUTPUT_DIR/bedrock"

echo "Removing container..."
docker rm "$CONTAINER_NAME"

echo "Build complete. The bedrock binary is now in the $OUTPUT_DIR directory."
