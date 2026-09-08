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

Five items require a decision at this level: two from this directory's own
units, one escalated up from `testplugin/`, and two more surfaced only by
Pass B's sibling and parent context (a cross-suite dependency, and a
harness-placement question).

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

4. **Dependency on `test/tests/jobs` for shared Job fixtures** (med,
   **escalate** — new in Pass B). The parent's rollup surfaces that
   `test/clustertest/tests` depends on `test/tests/jobs` (e.g.
   `JobTestHelper::getTimestampForDateTimeString`) for shared Jobs-plugin
   test fixtures — a dependency neither this directory nor `test/tests`
   could see in Pass A, since it crosses a sibling boundary in the middle of
   the tree, two levels down on both sides. Judgment: this is a **boundary
   violation**, not a sanctioned shared-fixture arrangement. `test/tests/jobs`
   is organized and described, by its own rollup, as single-node
   command-by-command coverage of the Jobs plugin — nothing marks it as a
   shared-infrastructure location, and it was never designed to be imported
   by the cluster suite. The practical cost: `test/tests/jobs` cannot be
   freely restructured without someone remembering to check for a consumer
   two directories away that its own SUMMARY has no way to name. Escalated,
   because the fix is a move between siblings and belongs with whichever of
   `test/tests`/`test/clustertest` owns the file today — see `test/tests`'s
   own Pass B misfit list for the paired half of this same finding.
   **Suggested home:** `test/lib`, the one layer both `test/tests/jobs` and
   `test/clustertest/tests` already depend on, so the fixture would sit
   somewhere both suites reach *into* rather than one reaching into the
   other.

5. **`BedrockClusterTester.h`'s directory placement** (med, **escalate** —
   new in Pass B). Comparing against `test/lib`'s Pass B rollup: once Misfit
   #1 above is applied (the `testplugin.so` detection block moved out to
   `testplugin/`), `BedrockClusterTester.h`/`ClusterTester<T>` has no
   remaining dependency on anything in `test/clustertest` — only on
   `libstuff` and `test/lib/BedrockTester.h`, the harness type it is generic
   over. Structurally it is a peer of `BedrockTester`, not an inhabitant of
   this directory: it is the multi-node composition of the same
   single-process harness `test/lib` already owns, the same relationship
   `test/lib`'s own `PortMap`/`RemoteSQLite`/`TestHTTPS` have to
   `BedrockTester`. Its presence here looks like an artifact of
   `test/clustertest` having been built as its own self-contained subsystem,
   not a deliberate layering choice. `main.cpp` does **not** share this
   problem: the parent's own top-level structure already sets the precedent
   that a binary's entry point lives beside the cases it drives
   (`test/main.cpp` sits at `test/` top level beside `test/tests`, not
   inside `test/lib`), so `main.cpp` staying here beside `testplugin/` and
   `tests/` is consistent, not a misfit. Escalated because moving a unit
   into a sibling directory is not this directory's call to make alone.
   **Suggested home:** `test/lib`.

No other items need attention at this level: `tests/`'s three cluster
agents resolved all twelve of their own misfits internally (mostly same-file
naming/hygiene fixes and one test-registration gap), and none of them
propagated up — correctly, since their suggested homes were all inside
`test/clustertest/tests` itself.

## 5. Role in the system

**What this directory owns that its siblings don't:** the multi-node cluster
*lifecycle* — constructing a peer-linked cluster of N nodes, waiting for
every node to answer Status before tests proceed, and tearing it down
leader-last — plus the entry point for the binary that hosts the whole
multi-node run. `test/lib` owns running *one* server process; `test/tests`
owns asserting single-node command behavior against it. Neither has, or
needs, any notion of a cluster of nodes, quorum, or leader/follower topology
— that concept exists only here.

**Boundary with `test/lib`.** Clean in dependency direction: this directory
depends on `test/lib`'s `BedrockTester` (via `ClusterTester<T>`), never the
reverse. Not clean in placement, though — see Misfit #5. The boundary
*works* today (nothing is broken by it), but it draws the line one directory
later than the dependency structure actually supports.

**Boundary with `test/tests`.** This is where Pass B's biggest finding sits:
`test/clustertest/tests` depends on `test/tests/jobs`, i.e. the multi-node
suite reaches across into the single-node suite's own subtree for shared
Jobs-plugin test fixtures. This boundary **leaks** — see Misfit #4. The two
suites are meant to be independent top-level test subsystems (single-node
vs. multi-node), each free to be restructured on its own; a hidden
subdirectory-level dependency between them breaks that independence in one
direction. It is not disqualifying in principle — wanting to share fixtures
across a single-node and multi-node suite that both exercise the same Jobs
plugin is reasonable — but the fixtures should live somewhere both suites
depend on declaratively, not somewhere one suite happens to already keep
them.

**The harness/cases split, recurring one level down.** `test/clustertest`
reproduces, at its own level, the same organizing principle `test/` uses at
the top: a harness (`BedrockClusterTester.h` + `main.cpp`) next to the cases
it serves (`testplugin/`, `tests/`). That recursion is legitimate in
principle — cluster-mode testing is different enough from single-node
testing to need its own bootstrap step — but per Misfit #5, the harness half
of the split does not need its *own* directory the way `test/lib` does.
`BedrockClusterTester.h` can be absorbed into `test/lib` as a second,
composed harness type sitting beside `BedrockTester`. `main.cpp`, by
contrast, correctly recurs the *other* half of the top-level pattern
(`test/main.cpp` lives beside its cases, not inside the harness directory)
and should stay put. So the recursion is half real, half accidental: the
binary-entrypoint-beside-its-cases pattern is a deliberate, correct echo of
`test/`'s own structure; the separate-harness-subdirectory pattern is not
needed here, because the multi-node harness is small enough — and dependent
enough on `test/lib`'s harness — to be one more class inside it rather than
a second harness directory one level down.

**`BedrockTester` vs. `BedrockClusterTester`.** These are not two
independent, competing harnesses that happen to both have a high blast
radius — they are two layers of the same stack. `BedrockClusterTester` *is*
`ClusterTester<BedrockTester>`: N `BedrockTester` instances wired into a peer
list with cluster-level start/wait/teardown semantics layered on top. Its
blast radius (36) is high because every cluster test needs a cluster, not
because it independently re-implements process management — that part is
delegated straight through to `BedrockTester` (blast radius 34, in
`test/lib`). Organizing them into two different directories with two
different SUMMARY files makes them look like unrelated peers when the
dependency graph says one is a thin composition over the other. That split
is **not fully principled** as it stands; see Misfit #5 for the concrete
fix.

## 6. Inbound expectations

No sibling's rollup lists this directory in its own `depends_on_dirs`, so
nothing here is currently depended on by `test/lib` or `test/tests` —
`depended_on_by` stays `[]`. The parent's exports listing does describe
`BedrockClusterTester` as the multi-node harness the whole subsystem is
built on, but that is an expectation on *this directory's own children*
(`testplugin/`, `tests/`), already satisfied, not something owed to a
sibling. If Misfit #5 is acted on, this flips: `test/lib` would then be
depended on for cluster bootstrapping as well as single-node bootstrapping,
and this directory's own `depends_on_dirs` would need `test/lib` to keep
covering that role — which it already lists, so no gap opens up.

<!-- ROLLUP
theme: Root of Bedrock's cluster-integration test subsystem — owns the multi-node cluster bootstrap harness (BedrockClusterTester) and the test-binary entrypoint that test/clustertest/testplugin and test/clustertest/tests are built on.
exports: [BedrockClusterTester / ClusterTester<T> multi-node cluster harness, cluster-test binary entrypoint (CLI parsing, signal cleanup, test-run driver), TestPlugin test-only command surface for crashes/timeouts/escalation/commit-hooks (testplugin/), BedrockClusterTester-driven multi-node integration test suite covering replication/failover/escalation/membership/adversarial-stress (tests/)]
depends_on_dirs: [libstuff, test/lib, sqlitecluster, test/tests/jobs]
depended_on_by: []
misfit_count: {high: 0, med: 3, low: 2}
resolved_locally: 2
escalate:
  - item: fileAppend / fileLockAndLoad
    from: test/clustertest/testplugin/TestPlugin.cpp
    why: generic flock-guarded file I/O helpers with no plugin-specific logic and no relation to test/clustertest's remit either; belongs in a shared utility location
    suggested_home: libstuff
  - item: dependency on test/tests/jobs for shared Job fixtures
    from: test/clustertest/tests
    why: multi-node suite reaches into a sibling single-node suite's own subdirectory for shared fixtures; test/tests/jobs was never designed as a shared-fixture location
    suggested_home: test/lib
  - item: BedrockClusterTester.h (whole unit)
    from: test/clustertest
    why: generic ClusterTester<BedrockTester> composition over test/lib's harness, with no remaining clustertest-specific logic once the testplugin.so misfit above is fixed; organizationally a peer of BedrockTester, not an inhabitant of this directory
    suggested_home: test/lib
-->
