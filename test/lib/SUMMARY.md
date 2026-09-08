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

## 5. Role in the system

**What this directory owns that its siblings don't:** the only code in
`test/` that forks/execs a real bedrock server process and speaks to it —
process lifecycle, port allocation, the request/response wire path, and the
test doubles (`RemoteSQLite`, `TestHTTPS`) that let product code run
unmodified against a test server — plus the vendored `tpunit++` runner
everything else in `test/` executes under, and the assertion-printing glue
tests are written against. Both `test/tests` and `test/clustertest` consume
this; neither implements any part of it themselves.

**Boundary with `test/tests`.** Clean and one-directional — `test/tests`
depends on `BedrockTester` (and implicitly the `tpunit++` runner and
`PrintEquality`) for its single-command integration fixtures; nothing here
depends back. No leak.

**Boundary with `test/clustertest`.** Also dependency-clean in direction —
`test/clustertest` depends on `BedrockTester` via `ClusterTester<T>`, never
the reverse — but per `test/clustertest`'s own Pass B rollup, not clean in
*placement*: `BedrockClusterTester.h` is a generic composition over
`BedrockTester` with no remaining logic specific to `test/clustertest` once
its one clustertest-specific wart (the `testplugin.so` detection block) is
removed. That means the boundary between "harness" and "cluster suite"
is currently drawn one directory later than the dependency graph supports —
see the next point.

**`BedrockTester` vs. `BedrockClusterTester`.** These two units, at blast
radius 34 and 36 respectively, are not independent peer harnesses that
happen to both be heavily depended on — `BedrockClusterTester` *is*
`ClusterTester<BedrockTester>`, a generic multi-node composition built
directly on top of `BedrockTester`, with per-node process management
delegated straight through to it. Splitting them across two directories
(and two SUMMARY files) currently makes them look organizationally
unrelated when the dependency graph says one is a thin layer over the
other. That split is **not fully principled** as it stands: this directory
already draws its own internal line at "single-process harness plus its
direct satellites" (`PortMap`, `RemoteSQLite`, `TestHTTPS` all extend
`BedrockTester`'s job, not a separate concern), and `BedrockClusterTester`
fits that same description — a further satellite of `BedrockTester`, just
one that composes several instances of it instead of decorating one. If
`test/clustertest` acts on its own Misfit (moving `BedrockClusterTester.h`
here), this directory would absorb it as a second harness type sitting
beside `BedrockTester`; nothing about this directory's current dependencies
or exports would need to change to accommodate that, since `libstuff` is
already the only other thing `BedrockClusterTester.h` needs.

## 6. Inbound expectations

Both siblings depend on this directory — `test/tests` for the single-command
integration pattern, `test/clustertest` for the per-node harness underneath
`BedrockClusterTester` — so `depended_on_by` now lists both. Everything
either sibling's rollup names as a dependency on this directory
(`BedrockTester`, the `tpunit++` runner, `PrintEquality`/`ASSERT_EQUAL`) is
already exported here; nothing relied upon is missing. Two things are
exposed that go slightly beyond what a pure "run one server and talk to it"
contract would need, both already flagged as this directory's own misfits
rather than new findings: `BedrockTester::autoAttachDebugger`'s LLDB
RPC-socket client rides along on every `BedrockTester` consumer even though
only interactive debugging sessions use it, and `PrintEquality.h`'s
container `operator<<` overloads are pulled in by anything that includes it
for `ASSERT_EQUAL`, whether or not that caller ever prints a `list`/`set`/
`map`/`optional`. Pass B does not change either judgment; if anything, the
volume of code depending on this directory (both `test/tests`'s ~25 units
and all of `test/clustertest/tests`'s ~36) raises the cost of leaving them
bundled, since every consumer's link surface grows to match.

Pass B also surfaces two candidate future additions to this directory, both
originating in siblings' own misfit lists rather than anything found here:
`BedrockClusterTester.h` (from `test/clustertest`, see above) and the shared
Jobs-plugin test helper `JobTestHelper` (from `test/tests/jobs`, flagged in
`test/tests`'s own Pass B misfits as depended on by `test/clustertest/tests`
without a designed shared home). Both are low-risk fits: neither needs
anything from `test/clustertest` or `test/tests/jobs` beyond what already
lives in `libstuff`, so absorbing either would not create a new dependency
this directory doesn't already have.

<!-- ROLLUP
theme: Test-harness layer that forks and drives a real bedrock server subprocess for the test suite, plus the vendored micro test-framework tests run under.
exports: [BedrockTester, PortMap, RemoteSQLite, TestHTTPS, tpunit++ framework runner, PrintEquality/ASSERT_EQUAL printing]
depends_on_dirs: [libstuff, sqlitecluster]
depended_on_by: [test/clustertest, test/tests]
misfit_count: {high: 0, med: 2, low: 0}
resolved_locally: 1
escalate:
  - item: "operator<<(ostream&, const list<T>&/set<T>&/map<T,U>&/optional<T>&)"
    from: test/lib/PrintEquality.h
    why: Generic container-printing utilities with no tie to PrintEquality or testing, already built on libstuff's SComposeList
    suggested_home: a shared stream-formatting utility header in libstuff
-->
