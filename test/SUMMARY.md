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

**Revisiting the `libstuff` destination now that `libstuff`'s own rollup is
visible.** `libstuff` describes itself as "undercut by a 4612-line catch-all
file that still duplicates several of its own dedicated units," and
separately escalates several of its own items outward (to `sqlitecluster`,
to the application layer). It is not a stable dumping ground right now — it
is itself flagged for decomposition. That changes the emphasis but not the
destination for `fileAppend`/`fileLockAndLoad` and the container
`operator<<` overloads: both are exactly the shape of thing `libstuff`
already holds under "libstuff-core" (`SString`/`STable`/`SException`/logging
macros) — small, dependency-free, generic utility code is squarely
`libstuff`'s job, so `libstuff`-the-directory is still the right home. What's
no longer safe to assume is that they belong in the 4612-line catch-all file
that is the source of `libstuff`'s own problem; landing two more orphaned
utilities there would repeat the exact anti-pattern `libstuff`'s own summary
flags. Both should arrive as their own small dedicated units (a file-I/O
helper header, a stream-formatting header) alongside `libstuff`'s other
dedicated units, not folded into the catch-all file. It's also worth root
noticing that `plugins/` independently escalates its own generic-utility
misfits toward `libstuff` (`LRUMap`, a sqlite3-CLI arg/error wrapper,
`scopedDisableNoopMode`) — three different subtrees converging on `libstuff`
as the right home for generic shared code is itself an argument for giving
`libstuff` room for a proper "shared utilities" unit during its
decomposition, rather than treating each escalation as a one-off patch.
`QueryTest::testPercentile` is unaffected by this — nothing in `libstuff`'s,
`plugins`'s, or `sqlitecluster`'s visible rollups owns a SQLite-extension
test home, so it is carried up with `suggested_home: null` unchanged.

## 5. Role in the system

`test/` owns *verification*, not production behavior, of everything below
it: root's command pipeline (`BedrockServer`/`BedrockCommand`/`BedrockCore`/
`BedrockCommandQueue`/`BedrockPlugin`/`BedrockConflictManager`), `plugins/`'s
five `BedrockPlugin_*` implementations, `sqlitecluster`'s replicated-SQLite
engine, and `libstuff`'s foundation layer. No sibling shares that role —
`libstuff`, `plugins`, `sqlitecluster`, and root all own production behavior;
`test/` owns none of it. The boundary is one-directional by construction:
`test/` is the only directory in the tree whose `depends_on_dirs` draws from
all of `libstuff`, `libstuff/JSON`, `sqlitecluster`, and `plugins` at once,
and it is the only directory none of the others lists back in their own
`depends_on_dirs`. In dependency terms `test/` is a strict leaf — nothing it
exports is load-bearing for production code.

That boundary mostly holds, but leaks in one place visible only from here:
`test/clustertest/testplugin` is a real `BedrockPlugin_*` implementation (a
"TestPlugin test-only command surface") that must be loaded through root's
plugin-loading mechanism at runtime to drive cluster tests — the same
contract `plugins/`'s five production plugins implement. So while no
production code depends on `test/` at build/link time, root's plugin
contract does have a live, if narrow, consumer inside `test/`: the interface
`plugins/` implements against needs to stay open enough for a non-production,
test-only plugin to keep registering through it. That never shows up as a
directory dependency, but it's a real coupling root and `plugins/` should
know about.

Does `test/`'s shape actually mirror what `libstuff`/`plugins`/`sqlitecluster`
claim to export? Only partially:

- **`plugins/`** exports five plugins (Cache, Compression, DB, Jobs, MySQL).
  `test/tests` mirrors exactly one of them structurally — a nested `jobs/`
  subdirectory — and names no dedicated coverage for Cache, Compression, DB,
  or MySQL anywhere in the visible rollups. Either those four are exercised
  anonymously within the flat "one `BedrockTester` fixture per core server
  command" set `test/tests` describes, or they aren't tested at the level of
  a named plugin at all. From here that can't be distinguished, but the
  asymmetry — Jobs alone gets a directory — is real, and suggests that
  coverage grew organically rather than the suite being modeled 1:1 on
  `plugins/`'s shape.
- **`sqlitecluster/`** exports seven symbols (`SQLite`, `SQLiteNode`,
  `SQLitePeer`, `SQLiteCommand`, `SQLiteClusterMessenger`, `SQLitePool`,
  `SQLiteServer`). Named coverage across `test/tests` + `test/clustertest`
  accounts for `SQLite` (commit/rollback) and `SQLiteNode` (peer-selection,
  replication/failover/escalation) — the consensus core. `SQLitePeer`,
  `SQLiteCommand`, `SQLiteClusterMessenger`, `SQLitePool`, and
  `SQLiteServer` — the wire-format/pooling plumbing — have no named coverage
  anywhere in this subtree's rollups.
- **`libstuff/`** exports seven groups. `test/tests` names coverage for only
  a slice of `libstuff-core` (async DNS, ring buffer, string/date
  validators). `SData`, the `SQResult`/`SQValue`/`SQliteParameter` typed-SQL
  layer, the entire `STCPManager`/`SHTTPSManager`/`SSSLState`
  networking/TLS stack, `SLog`/`SSignal`, `SThread`, and `SFluentdLogger`
  have no named coverage anywhere visible.

This is the honest limit of what this rollup can claim: an export not named
in a child's curated export list isn't proof of zero coverage — a fixture
could exercise it without calling it out. But the same pattern repeating
across three independent children (Jobs singled out; the consensus core
named but the messenger/pool/server layer never mentioned; all of
`libstuff`'s non-core surfaces silent) is consistent enough to be a real
signal, not noise, and worth root treating it as such.

## 6. Inbound expectations

Because nothing in this subtree exports anything a sibling's
`depends_on_dirs` lists, `test/` owes production code nothing in the
conventional sense — `depended_on_by` stays empty. The one inbound
expectation runs through the plugin contract described above: root's/
`plugins/`'s `BedrockPlugin` registration mechanism must stay stable and
generic enough for `test/clustertest/testplugin` to keep loading as a
plugin, or the entire cluster-integration suite silently loses its ability
to drive the behavior it exists to test. That's a narrow but real thing
`test/` needs from root that isn't visible as a directory dependency, and
nothing in the visible rollups suggests it's currently at risk.

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
