# test

## 1. Theme

`test` is the root of Bedrock's entire test tree. It does not contain test
*cases* itself, beyond the single-node test binary's entry point — it exists
to divide "verify Bedrock" into three non-overlapping jobs and hand each to a
subdirectory: **harness** (how do we even talk to a running server),
**unit/single-node tests** (does one server, given one command, do what it
promises), and **cluster-integration tests** (does a multi-node cluster
survive replication, failover, and escalation). `main.cpp` is the thin wire
that turns "harness" + "single-node tests" into a runnable binary.

## 2. Contents

| Child | Kind | What it is |
|---|---|---|
| `main.cpp` | unit (127 lines) | Entry point for the single-node bedrock test binary: CLI-flag parsing, SIGINT cleanup, drives `tpunit::Tests::run` over the fixtures registered by `test/tests`. |
| `lib/` | subdirectory | The harness layer: forks/execs/drives a real bedrock server subprocess (`BedrockTester`), port allocation, HTTPS/SQLite test doubles, and the vendored `tpunit++` framework everything else runs under. |
| `tests/` | subdirectory | The single-node regression suite: one fixture per core server command (via `BedrockTester`) plus unit coverage of the `libstuff`/JSON support libraries underneath those commands, with the Jobs plugin's own command suite nested in `jobs/`. |
| `clustertest/` | subdirectory | The multi-node cluster-integration subsystem: its own harness (`BedrockClusterTester`) and binary entry point, a test-only plugin (`testplugin/`) that exposes internal server behavior as commands, and ~36 cluster-level test cases (`tests/`). |

## 3. Coherence

The three-way split — harness / single-node cases / cluster cases — is real
and each subdirectory's own SUMMARY confirms its half is internally coherent.
But the split is not applied at a consistent grain, and that's worth naming
rather than glossing over:

- At the top level, **harness and cases are separated into siblings**:
  `test/lib` is pure infrastructure, `test/tests` is pure cases, and the two
  are stitched together only by `test/main.cpp`.
- Inside `clustertest/`, the same conceptual split — harness
  (`BedrockClusterTester.h`, `main.cpp`) vs. cases (`tests/`) — happens
  **again, one level down, inside a single directory**, rather than
  `clustertest`'s harness living next to `test/lib` and its cases living
  next to `test/tests`. That's a defensible call (cluster tests need a
  cluster-specific harness that single-node tests don't), but it means the
  same organizing principle (separate harness from cases) is expressed at
  two different depths in the tree, which is the kind of thing that reads as
  inconsistent structure from outside even though each half is locally fine.
- The split also leaks across siblings in one concrete place:
  `test/clustertest` lists `test/tests/jobs` as a dependency — cluster-level
  tests reach into the single-node suite's `jobs/` subdirectory (almost
  certainly for shared Jobs-plugin test helpers/fixtures). That is a
  cluster-integration concern depending on a single-node-suite-internal path,
  which is exactly the kind of boundary crossing "harness vs. unit vs.
  cluster" is supposed to prevent. It is not necessarily wrong — sharing
  Jobs test fixtures instead of duplicating them is reasonable — but it
  should be an explicit, named shared dependency (e.g. hoisted to somewhere
  both suites include from) rather than one suite reaching into the other's
  subtree. This is a `test/`-level observation neither child could make on
  its own, since neither `test/clustertest` nor `test/tests` can see the
  other's SUMMARY.

Otherwise: `main.cpp`'s only role is to be the single-node runner, and it
fits that role exactly (5/5 across the board per its own record). No child
here is a poor fit for `test/` as a whole.

## 4. Misfits

`main.cpp` carries no misfits of its own. All three subdirectories resolved
their *own* internal misfits without propagating them (13 in `test/tests`, 2
in `test/clustertest`, 1 in `test/lib` — see each SUMMARY for detail); those
are correctly absent here. What remains are the three items each
subdirectory could not place within itself:

- **`fileAppend` / `fileLockAndLoad`** (low) — from
  `test/clustertest/testplugin/TestPlugin.cpp`, escalated by `test/clustertest`.
  Generic flock-guarded file-append/read helpers with no plugin-specific
  logic. Their suggested home, `libstuff`, is outside this entire subtree —
  `test/` has no more authority over `libstuff` than `test/clustertest` did.
  **Carried up unchanged**, not reopened.
- **`operator<<(ostream&, const list<T>&/set<T>&/map<T,U>&/optional<T>&)`**
  (med) — from `test/lib/PrintEquality.h`, escalated by `test/lib`. Generic
  container-printing utilities already built on `libstuff`'s `SComposeList`,
  with no dependency on `PrintEquality` or `tpunit`. Same reasoning as
  above: the suggested home is a shared stream-formatting header in
  `libstuff`, outside `test/`. **Carried up unchanged.**
- **`QueryTest::testPercentile`** (low) — from `test/tests/QueryTest.cpp`,
  escalated by `test/tests` with `suggested_home: null`. It tests a
  compiled-in SQLite extension (percentile/median aggregates), not
  Bedrock's own `Query` command handling. Having now seen all of `test/`'s
  other children (`lib/`, `clustertest/`), there is no better home for it
  inside this subtree either — neither is "SQLite feature tests" territory.
  The question of whether one exists elsewhere in the repo (e.g. near
  `sqlitecluster` or `libstuff`) still needs a wider view than `test/` has.
  **Carried up unchanged**, still with no suggested home.

Two of the three items above point toward `libstuff` specifically; both are
genuine escalations rather than something `test/` is positioned to resolve —
this directory owns test infrastructure and test cases, not shared
general-purpose utility code, so "the fix belongs in a production support
library" is by definition outside its remit.

<!-- ROLLUP
theme: Root of Bedrock's test tree — divides "verify Bedrock" into a harness layer (test/lib), a single-node command/support-library test suite (test/tests), and a multi-node cluster-integration subsystem (test/clustertest), tied together by the single-node test binary's entry point (main.cpp).
exports: [single-node bedrock test-binary entry point (main.cpp), BedrockTester test harness + vendored tpunit++ framework (test/lib), single-node command and libstuff/JSON support-library test suite incl. Jobs-plugin coverage (test/tests), BedrockClusterTester multi-node cluster harness plus test-only plugin and cluster-integration test suite (test/clustertest)]
depends_on_dirs: [libstuff, libstuff/JSON, sqlitecluster, plugins]
depended_on_by: []
misfit_count: {high: 0, med: 1, low: 2}
resolved_locally: 0
escalate:
  - item: fileAppend / fileLockAndLoad
    from: test/clustertest/testplugin/TestPlugin.cpp
    why: generic flock-guarded file I/O helpers with no plugin-specific logic and no relation to any part of test/'s remit; belongs in a shared utility location outside this subtree
    suggested_home: libstuff
  - item: "operator<<(ostream&, const list<T>&/set<T>&/map<T,U>&/optional<T>&)"
    from: test/lib/PrintEquality.h
    why: generic container-printing utilities with no tie to PrintEquality or testing, already built on libstuff's SComposeList
    suggested_home: a shared stream-formatting utility header in libstuff
  - item: QueryTest::testPercentile
    from: test/tests/QueryTest.cpp
    why: tests a compiled-in SQLite extension (percentile/median aggregates), not Bedrock's Query command handling; no better home exists within test/'s own subtree, and whether one exists elsewhere in the repo needs a wider view
    suggested_home: null
-->
