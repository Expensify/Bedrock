# Bedrock Docker Setup

This folder contains the necessary files to build and run Bedrock in a Docker container.

## Contents

- `Dockerfile`: Defines the Docker image for Bedrock
- `libstuff/bedrock.sh`: Startup script for Bedrock within the container
- `docker-compose.yml`: Docker Compose configuration for running a Bedrock cluster

## Dockerfile

The Dockerfile sets up a container based on [`phusion/baseimage:noble-1.0.0`](https://github.com/phusion/baseimage-docker) 
and includes the following key steps:

1. Installs necessary packages
2. Copies the Bedrock binary to the container
3. Sets up the database directory
4. Exposes ports 8888 and 9000
5. Configures the startup script
6. Creates a non-root user for running Bedrock

## bedrock.sh

This script is responsible for starting Bedrock with the appropriate parameters. It:

1. Checks for a compatible Bash version
2. Sets up default parameters
3. Processes environment variables to configure Bedrock
4. Starts Bedrock as the non-root user

## Docker Compose

The `docker-compose.yml` file defines a Bedrock cluster with three nodes. It includes:

1. Three Bedrock services (node0, node1, node2)
2. Network configuration for inter-node communication
3. Volume mounts for data persistence
4. Environment variable configuration for each node

## Usage

To build and run the Bedrock cluster using Docker Compose:

```bash
docker-compose up --build
```

This command will build the Docker image and start the Bedrock cluster as defined in the `docker-compose.yml` file.

## build_and_extract.sh

This script automates the process of building the Docker image and extracting the Bedrock binary. It performs the following steps:

1. Builds the Docker image using the Dockerfile
2. Creates a temporary container from the built image
3. Copies the Bedrock binary from the container to a local output directory
4. Removes the temporary container

To use this script:

```bash
./build_and_extract.sh
```

After running the script, you'll find the Bedrock binary in the `../output` directory.

To run a single Bedrock container:

```bash
docker run -d \
  --name bedrock-node \
  -p 8888:8888 \
  -p 9000:9000 \
  -e NODE_NAME=node1 \
  -e PRIORITY=100 \
  -e PEER_LIST=node2:9000,node3:9000 \
  bedrock
```

## Environment Variables

The following environment variables can be used to configure Bedrock:

- `NODE_NAME`: Name of the Bedrock node
- `PRIORITY`: Priority of the node in the cluster
- `PEER_LIST`: Comma-separated list of peer nodes
- `PLUGINS`: Plugins to enable
- `CACHE_SIZE`: Size of the cache
- `WORKER_THREADS`: Number of worker threads
- `QUERY_LOG`: Query log configuration
- `MAX_JOURNAL_SIZE`: Maximum journal size
- `SYNCHRONOUS`: Synchronous mode setting

Additional flags can be set using the `VERBOSE`, `QUIET`, and `CLEAN` environment variables.

## Ports

- 8888: HTTP server port
- 9000: Peer communication port

## Data Persistence

The Bedrock database is stored at `/var/db/bedrock.db` within the container. In the Docker Compose setup, volumes are used to persist data for each node.

## Logs

Bedrock logs are written to `/var/log/bedrock.log` inside the container.

Example of log output:
```
$ docker logs bedrock-node2
bedrock-node2  | Oct 17 16:14:52 da62315a18ec bedrock: xxxxxx (SQLiteNode.cpp:1253) _onMESSAGE [sync] [info] {node2/LEADING} Received PING from peer 'node0'. Sending PONG.
bedrock-node2  | Oct 17 16:14:52 da62315a18ec bedrock: xxxxxx (SQLiteNode.cpp:1261) _onMESSAGE [sync] [info] {node2/LEADING} Received PONG from peer 'node0' (0ms latency)
bedrock-node2  | Oct 17 16:14:52 da62315a18ec bedrock: xxxxxx (SQLiteNode.cpp:1253) _onMESSAGE [sync] [info] {node2/LEADING} Received PING from peer 'node1'. Sending PONG.
bedrock-node2  | Oct 17 16:14:52 da62315a18ec bedrock: xxxxxx (SQLiteNode.cpp:1261) _onMESSAGE [sync] [info] {node2/LEADING} Received PONG from peer 'node1' (0ms latency)
```

## Network

The Bedrock nodes are connected to a custom bridge network `bedrock-cluster`, allowing for easy communication between nodes.