# Test port server

`BedrockTester::ports` is a `PortMap` alias for `PortServerClient`. Its first
allocation connects to a per-user server, starting `bedrock-test-port-server` if
necessary. Separate test executables and worktrees share
`/tmp/bedrock-test-ports-<uid>/server.sock`; the directory has mode `0700`.

Each client connection registers its PID and owns its reservations. There can be
multiple connections for one PID. Returning a port releases just that reservation;
disconnecting releases the connection's entire allocation. The server uses Linux
pidfds or macOS kqueue process notifications to reclaim all connections when their
PID exits, even if descendants inherited open sockets or the PID is still a zombie.

The server reserves ports in `10000–20000` and checks loopback and wildcard IPv4
bind availability before allocating. Reservations prevent duplicate allocations
among testers; unrelated programs can still bind a port before Bedrock starts.
Stopping a Bedrock instance does not release its reservations, allowing it to
restart on the same ports. `waitForPort()` is an independent local bind check.

## Startup and shutdown

The server holds an exclusive `flock()` on `server.lock` throughout its lifetime.
Clients first try the socket, then try the lock and check the socket again before
launching a helper. The helper inherits the lock and a bootstrap connection, so it
starts with a client already present. The lock file is never removed during normal
operation. Stale sockets are removed only while holding the lock.

The helper runs in a separate process group and closes unrelated inherited file
descriptors. Client fork handlers close inherited sessions and release client
mutexes in children; allocations in a child establish a connection under its own
PID. Client sockets are also close-on-exec.

The helper logs startup, shutdown, allocations, and releases to syslog at INFO
level under `bedrock-test-port-server`. Allocation and release messages include
the client PID and connection; releases identify returned ports, disconnects, or
PID exits. The syslog tag includes the server PID.

When the last connection disappears, the server removes its socket and exits
before releasing the singleton lock. A registration racing with shutdown retries.
Once a connection has registered, transport failure invalidates that client
session instead of silently recreating outstanding reservations. Server crash
recovery for existing reservations is not supported.

## Build and tests

`make bedrock`, `make test`, and `make clustertest` build the helper automatically.
The client finds it through `BEDROCK_DIR`, an ancestor of the test executable, or
`PATH`. Consumers with explicit source lists can continue compiling
`test/lib/PortMap.cpp`, which forwards to the client implementation.

Run the lifecycle and concurrency tests with:

```sh
test/test -only PortServer
```

These tests use explicitly supplied private endpoints, allowing them to exercise
server shutdown, stale sockets, and crashes without affecting other test suites.
