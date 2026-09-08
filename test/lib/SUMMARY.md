# test/lib

## 1. Theme

`test/lib` is the test-harness layer that lets the rest of the test suite
drive a *real* bedrock server as a subprocess instead of mocking it. It owns
process lifecycle (fork/exec/stop), network plumbing to talk to that process,
a couple of narrow test doubles (a remote-forwarding SQLite, a bare-bones
HTTPS client) that let product code run unmodified against a test server, and
the shared assertion/printing glue the tests are written against. It also
hosts a vendored micro test-framework the rest of `test/` is built on.

## 2. Contents

| Child | Lines | What it is |
|---|---|---|
| `BedrockTester.{h,cpp}` | 876 | Central harness: forks/execs/stops a bedrock server subprocess, sends it commands over TCP, reads its DB file directly, tracks all live instances. Also carries an LLDB auto-attach client (see Misfits). |
| `PortMap.{h,cpp}` | 107 | Hands out non-conflicting TCP ports to `BedrockTester` instances from a fixed range, recycling returned ones once confirmed free, so parallel test runs don't collide. |
| `PrintEquality.h` | 54 | Backs tpunit++'s `ASSERT_EQUAL`/`ASSERT_NOT_EQUAL`, printing both sides with `=`/`!=` on failure. Also carries generic `operator<<` overloads for `list`/`set`/`map`/`optional` (see Misfits). |
| `RemoteSQLite.{h,cpp}` | 138 | A `SQLite` subclass that forwards every read/write to a `BedrockTester`'s server as a `Query` command instead of touching a local DB file, so HC-Tree-mode tests keep SQLite-shaped call sites. |
| `TestHTTPS.{h,cpp}` | 47 | Minimal `SHTTPSManager` subclass tests use to fire outbound HTTPS requests and read back the status code, with no product-specific response handling. |
| `tpunit++.cpp` | 606 | Implementation of the vendored tpunit++ framework's runner: fixture registration, multithreaded execution with name filtering, before/after hooks, pass/fail bookkeeping, console output. |

No subdirectories.

**Vendored third-party code:** this directory also hosts `tpunit++.hpp`
(present on disk, depended on by `BedrockTester` and by `tpunit++.cpp`
itself), which was excluded from annotation as vendored and so carries no
unit here. Nothing further is claimed about its contents.

## 3. Coherence

The children cohere around one job: stand up a real server process and give
tests a way to talk to it (`BedrockTester` at the center, `PortMap` and
`RemoteSQLite` and `TestHTTPS` as its satellites), plus the vendored test
runner (`tpunit++.cpp`) tests execute under and the printing glue
(`PrintEquality`) their assertions use. `BedrockTester`, `PortMap`,
`RemoteSQLite`, and `TestHTTPS` fit tightly (5/5 name and location fit each).
`PrintEquality` fits the weakest (3/3): its file does more than its name
promises — see Misfits.

## 4. Misfits

- **`BedrockTester::autoAttachDebugger`** (med) — a self-contained LLDB
  RPC-socket client with its own wire protocol, unrelated to forking/managing
  the server subprocess and sending it commands. Its purpose (attach a
  debugger to a `BedrockTester`-spawned process) is still test-tooling
  specific to this directory, not a general-purpose library facility.
  **Resolved locally**: split out of `BedrockTester` into its own small
  helper (e.g. `test/lib/DebugAttach.{h,cpp}`), decoupled from process
  management.
- **`operator<<` overloads for `list`/`set`/`map`/`optional`** in
  `PrintEquality.h` (med) — generic stream-formatting utilities with no
  dependency on `PrintEquality` or tpunit; they're only bundled here because
  `PrintEquality`'s own printing happens to need them, and they build on
  `SComposeList`, which already lives in libstuff. **Escalated**: their
  natural home is a shared stream-formatting utility header in libstuff,
  which is outside this directory.

<!-- ROLLUP
theme: Test-harness layer that forks and drives a real bedrock server subprocess for the test suite, plus the vendored micro test-framework tests run under.
exports: [BedrockTester, PortMap, RemoteSQLite, TestHTTPS, tpunit++ framework runner, PrintEquality/ASSERT_EQUAL printing]
depends_on_dirs: [libstuff, sqlitecluster]
depended_on_by: []
misfit_count: {high: 0, med: 2, low: 0}
resolved_locally: 1
escalate:
  - item: "operator<<(ostream&, const list<T>&/set<T>&/map<T,U>&/optional<T>&)"
    from: test/lib/PrintEquality.h
    why: Generic container-printing utilities with no tie to PrintEquality or testing, already built on libstuff's SComposeList
    suggested_home: a shared stream-formatting utility header in libstuff
-->
