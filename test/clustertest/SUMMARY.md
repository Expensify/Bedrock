# test/clustertest

## 1. Theme

This directory is the root of Bedrock's live multi-node cluster-integration
test subsystem. It does not itself contain test *cases* — it owns the
reusable infrastructure that the rest of the subtree is built on: the harness
that boots and tears down a real multi-node Bedrock cluster
(`BedrockClusterTester`), and the entry point for the test binary that runs
against it. Everything else in the subtree — the test-only plugin in
`testplugin/` and the ~36 test cases in `tests/` — exists to be driven by, or
loaded into, the cluster this directory knows how to stand up.

## 2. Contents

| Child | Kind | Lines | What it is |
|---|---|---|---|
| `BedrockClusterTester.h` | unit | 252 | Header-only `ClusterTester<T>` template (typedef'd as `BedrockClusterTester`) that starts N `BedrockTester` subprocesses as a peer-linked cluster, waits for all nodes to answer Status, and tears the cluster down leader-last. Blast radius 36 — every test in `tests/` depends on it. |
| `main.cpp` | unit | 112 | Entry point for the cluster-test binary: CLI-flag parsing (mirrors `test/main.cpp`'s flag family), a SIGINT handler that stops running servers, and the `tpunit::Tests::run` driver loop — deliberately not resetting state between tests. |
| `testplugin/` | subdirectory | — | Test-only `BedrockPlugin` (`TestPlugin`) that exposes otherwise-unreachable server code paths — crashes, HTTPS timeouts, escalation, commit hooks — as commands the cluster nodes can be told to run. |
| `tests/` | subdirectory | — | The actual test suite: ~36 `tpunit` fixtures, each provisioning a real cluster via `BedrockClusterTester` and asserting a cluster-level invariant (replication, failover, escalation, membership, adversarial stress, and a long tail of command/plugin edge cases). |

## 3. Coherence

All four children belong to one system and there is no trenchcoat here. The
two direct units are infrastructure (harness + runner); the two
subdirectories are consumers of that infrastructure (a plugin loaded into the
cluster under test, and the test cases that drive it). The split is clean and
intentional, not incidental:

- **`test/clustertest` (this directory)** owns cluster *lifecycle*: how a
  multi-node Bedrock cluster is constructed, started, waited-on, and torn
  down, plus the test binary that hosts the whole run. Nothing here asserts
  application behavior.
- **`test/clustertest/testplugin`** owns the *server-side test surface*: a
  `BedrockPlugin` loaded into the nodes themselves so tests can trigger
  internal behavior (crash, stall, escalate) from outside the process.
- **`test/clustertest/tests`** owns the *assertions*: the actual test cases
  that use this directory's harness (and, incidentally, `testplugin`'s
  commands) to verify cluster-level correctness.

That boundary mostly holds, but leaks in one direction already flagged by a
unit here: `BedrockClusterTester`'s constructor contains a `testplugin.so`
auto-detection block, i.e. this directory's lifecycle code has a hardcoded
special case for a specific subdirectory's plugin artifact — see Misfits.

## 4. Misfits

Three items require a decision at this level: two from this directory's own
units, one escalated up from `testplugin/`.

1. **`ClusterTester<T>`'s sized constructor: `testplugin.so` auto-detection
   block** (med, **resolved-locally**). Plugin-specific special-casing
   (checking cwd for `testplugin/testplugin.so`) inside an otherwise generic
   cluster-bootstrap helper — the code's own comment admits it "should get
   moved somewhere else, really. Probably inside TestPlugin." This directory
   can make that call: the better home is `test/clustertest/testplugin`
   (e.g. a small setup helper there that `BedrockClusterTester` calls into,
   or that testplugin exposes for the harness to use), not the generic
   harness file. Resolved here — the fix stays inside this subtree.

2. **`main.cpp`'s `log()`** (low, **resolved-locally**). Dead code — defined,
   never invoked — and its name collides in intent with the repo's
   SINFO/SWARN logging macros despite doing something unrelated (`exec`s into
   `tail -f syslog | grep bedrock`). Delete it; no relocation needed, no
   wider decision required.

3. **`fileAppend` / `fileLockAndLoad`** (low, **escalate**, from
   `test/clustertest/testplugin/TestPlugin.cpp`). Generic flock-guarded
   file-append/read helpers with no plugin-specific logic, used by exactly
   one test scenario (`testescalate`). `testplugin/`'s own agent already
   correctly identified these as misplaced and named `libstuff` as the
   suggested home — that is outside this entire directory's subtree, so
   there is nothing to resolve here. This directory has no authority over
   `libstuff`; passing it up unchanged is the right call, not a deferral.

No other items need attention at this level: `tests/`'s three cluster
agents resolved all twelve of their own misfits internally (mostly same-file
naming/hygiene fixes and one test-registration gap), and none of them
propagated up — correctly, since their suggested homes were all inside
`test/clustertest/tests` itself.

<!-- ROLLUP
theme: Root of Bedrock's cluster-integration test subsystem — owns the multi-node cluster bootstrap harness (BedrockClusterTester) and the test-binary entrypoint that test/clustertest/testplugin and test/clustertest/tests are built on.
exports: [BedrockClusterTester / ClusterTester<T> multi-node cluster harness, cluster-test binary entrypoint (CLI parsing, signal cleanup, test-run driver), TestPlugin test-only command surface for crashes/timeouts/escalation/commit-hooks (testplugin/), BedrockClusterTester-driven multi-node integration test suite covering replication/failover/escalation/membership/adversarial-stress (tests/)]
depends_on_dirs: [libstuff, test/lib, sqlitecluster, test/tests/jobs]
depended_on_by: []
misfit_count: {high: 0, med: 1, low: 2}
resolved_locally: 2
escalate:
  - item: fileAppend / fileLockAndLoad
    from: test/clustertest/testplugin/TestPlugin.cpp
    why: generic flock-guarded file I/O helpers with no plugin-specific logic and no relation to test/clustertest's remit either; belongs in a shared utility location
    suggested_home: libstuff
-->
