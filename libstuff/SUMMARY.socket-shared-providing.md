# Cluster: socket-shared-providing (libstuff)

## What these units have in common

This cluster is libstuff's non-blocking networking stack, built around one
shared model: a `poll()`-loop state machine where every "manager" or socket
type exposes `prePoll`/`postPoll` and advances by fd readiness rather than
blocking. `STCPManager` and its `Socket`/`Port` types are the base the rest
build on; `SSSLState` adds TLS underneath a socket; `SHTTPSProxySocket`
layers an HTTP CONNECT tunnel on top; `SSocketPool` and
`SMultiHostSocketPool` add keep-alive pooling (one host, then many);
`SHTTPSManager`/`SStandaloneHTTPSManager` drive outbound HTTPS requests as
pollable `Transaction`s; `SFastBuffer` is the buffer type sockets read/write
into; `SSynchronizedQueue` is a thread-safe queue designed to be waited on
in that same poll loop (via an internal wake pipe) alongside real sockets.
`SRandom` rides along in this batch but, as detailed below, isn't actually
part of the theme.

## Units

- **SFastBuffer** (`SFastBuffer.h/.cpp`, 180 lines) — append-friendly byte
  buffer avoiding front-erase reallocation; used as the send/recv buffer
  polled incrementally by sockets.
- **SHTTPSManager** (`SHTTPSManager.h/.cpp`, 341 lines) — non-blocking base
  for driving outbound HTTPS/HTTP requests as `Transaction`s;
  `SStandaloneHTTPSManager` needs no Bedrock plugin, `SHTTPSManager` binds
  one to an owning `BedrockPlugin`.
- **SHTTPSProxySocket** (`SHTTPSProxySocket.h/.cpp`, 176 lines) — an
  `STCPManager::Socket` that negotiates an HTTP CONNECT tunnel through a
  forward proxy before switching to normal TLS to the real destination.
- **SMultiHostSocketPool** (`SMultiHostSocketPool.h/.cpp`, 48 lines) — keys
  one `SSocketPool` per hostname behind a mutex-guarded map, so callers
  don't manage per-host pools themselves.
- **SRandom** (`SRandom.h/.cpp`, 57 lines) — static-only wrapper around a
  shared `mt19937_64`: bounded random integers, random alphanumeric
  strings, weighted booleans. Generic utility, unrelated to sockets.
- **SSSLState** (`SSSLState.h/.cpp`, 269 lines) — thin mbedTLS wrapper:
  per-connection handshake/send/recv over a raw fd, plus shared
  process-wide mbedTLS init.
- **SSocketPool** (`SSocketPool.h/.cpp`, 168 lines) — keep-alive connection
  pool for one host; hands out idle sockets or opens new ones, prunes
  timed-out ones on a background thread.
- **SSynchronizedQueue** (`SSynchronizedQueue.h`, 175 lines, header-only
  template) — thread-safe FIFO that multiplexes with poll() by writing to
  an internal pipe on every push.
- **STCPManager** (`STCPManager.h/.cpp`, 553 lines) — the shared
  prePoll/postPoll state machine base, plus the `Socket` (connection state
  machine) and `Port` (listening-socket RAII) value types everything else
  in this cluster builds on.

## Misfits

Two kinds surface here: things that don't fit *this cluster's* theme even
though they belong in libstuff, and things flagged by unit summaries that
may not belong in libstuff at all.

- **SRandom does not fit this cluster.** It has no relationship to sockets,
  TCP, TLS, or the poll-loop model that ties the other eight units
  together — it's a general-purpose random-number utility that happens to
  have landed in this thematic group. It clearly still fits the `libstuff`
  directory (generic, dependency-free utility), so this is a
  resolvable-locally case at the directory level: the parent should treat
  SRandom as belonging to a different (or its own) cluster, not this one.

- **SHTTPSManager (class) depends on `BedrockPlugin.h`** — flagged `med` by
  the unit summary: this inverts the expected dependency direction, since
  libstuff is meant to be usable without the Bedrock application layer
  above it (`SStandaloneHTTPSManager` next to it in the same file proves
  the plugin coupling isn't load-bearing for the networking logic itself).
  Escalating — the right fix (thinner adapter in Bedrock proper, vs. moving
  the type out of libstuff) needs a view wider than this cluster.

- **SHTTPSManager.cpp includes `BedrockServer.h` and `sqlitecluster/SQLiteNode.h` unused** —
  neither symbol is referenced in the file. Low severity, likely safe to
  drop, but touches files outside this cluster's own scope to confirm —
  escalating rather than guessing.

- **SFastBuffer::startsWithHTTPRequest** bakes HTTP terminator-scanning
  (`\r\n\r\n` / `\n\n`) into an otherwise protocol-agnostic buffer. Low
  severity; resolvable within libstuff (split into a small HTTP-framing
  helper layered on top of SFastBuffer) rather than escalated further.

- **SFastBuffer::contentLength** is a dead private field (reset to 0 in
  four places, never read or set otherwise). Low severity, resolvable
  locally — safe to remove within this file.

- **SStandaloneHTTPSManager::Transaction constructor throws on
  `isBlockingCommitThread`** — a Bedrock-specific execution-model rule
  embedded in an otherwise generic transaction constructor. Low severity;
  same shape as the SHTTPSManager/BedrockPlugin issue above but smaller —
  noting here, not separately escalating.

- **Naming-convention drift on private/protected members** across three
  units: `SHTTPSProxySocket`'s private members (`proxyAddress`, `hostname`,
  `requestID`, etc.) and `STCPManager::Socket`'s protected data members
  (`sendBuffer`, `sendRecvMutex`, `https`, `dnsResolution`, `hostToResolve`,
  `socketCount`) lack the `_` prefix used consistently elsewhere in this
  same cluster (`SSocketPool`, `SSynchronizedQueue`, `SMultiHostSocketPool`,
  `SStandaloneHTTPSManager`'s `_pem`). Low severity, resolvable locally —
  a mechanical rename within libstuff, not a structural issue.

<!-- ROLLUP
theme: non-blocking poll()-loop socket/TLS/HTTPS networking primitives (base socket state machine, TLS, proxy tunneling, connection pooling, HTTPS request driving) plus the buffer and poll-integrated queue types they use
exports: [STCPManager, STCPManager::Socket, STCPManager::Port, SSSLState, SHTTPSProxySocket, SStandaloneHTTPSManager, SHTTPSManager, SSocketPool, SMultiHostSocketPool, SFastBuffer, SSynchronizedQueue]
depends_on_dirs: [libstuff, sqlitecluster, .]
depended_on_by: []
misfit_count: {high: 0, med: 1, low: 6}
resolved_locally: 4
escalate:
  - item: SHTTPSManager (class, depends on BedrockPlugin.h)
    from: libstuff/SHTTPSManager.h
    why: inverts libstuff's expected dependency direction on the application layer above it
    suggested_home: near BedrockPlugin, or a thinner adapter in Bedrock proper leaving libstuff plugin-free
  - item: SHTTPSManager.cpp includes of BedrockServer.h and sqlitecluster/SQLiteNode.h
    from: libstuff/SHTTPSManager.cpp
    why: neither header's symbols are referenced in the file; unused cross-layer coupling
    suggested_home: null
  - item: SRandom (whole unit)
    from: libstuff/SRandom.h/.cpp
    why: fits the libstuff directory but not this cluster's socket/poll theme - a clustering artifact, not a location problem
    suggested_home: a different cluster (or its own) within libstuff; directory placement is already fine
-->
