# test/clustertest/tests

## 1. Theme

This directory is Bedrock's live multi-node integration suite: every one of
its ~36 units spins up a real 3-to-5-node Bedrock cluster through
`test/clustertest/BedrockClusterTester.h` and drives it with actual
`libstuff/SData.h` requests, then asserts on cluster-level behavior —
replication, leader/follower failover, command escalation, membership and
priority control, version-skew handling, and a long tail of individual
command/plugin edge cases (Jobs, HTTPS-thread interaction, timeouts,
compression, unique constraints). Its job in the system is to catch bugs that
only exist *between* nodes — the things a single-process unit test in
`libstuff/` or `sqlitecluster/` structurally cannot exercise, because they
depend on real replication, real elections, and real socket-level escalation
across independently-running processes.

## 2. Contents

This directory has no subdirectories and 36 direct file-units, pre-grouped by
Pass C into 3 clusters (`.arch/clusters.json`). Per spec for wide directories,
the table below is at cluster granularity; see each cluster's own SUMMARY for
the per-unit table.

| Cluster | Units | What it covers |
|---|---|---|
| [leader-cluster-follower](./SUMMARY.leader-cluster-follower.md) | 15 | Leader/follower state-machine surface: replication correctness, command escalation, failover/lifecycle, version-skew regressions, status/role reporting. The most internally coherent of the three — a real feature grouping, not just phrasing. |
| [cluster-verifies-through](./SUMMARY.cluster-verifies-through.md) | 5 | Adversarial/stress scenarios, each provisioning a cluster and forcing one specific failure condition (rate-limit pressure, bound-param replication, write-conflict spam, journal-hash corruption, repeated leader death) to check one invariant survives it. |
| [cluster-verifying-command](./SUMMARY.cluster-verifying-command.md) | 16 | A grab-bag unified only by the sentence template "cluster test verifying X": compression, membership/priority control, command-execution edge cases, the Jobs plugin, and a SQLite constraint check. No shared feature area. |

## 3. Coherence

All three cluster agents independently reported the same finding, and it is
worth stating plainly rather than papering over: **the cluster labels are not
the directory's real organizing principle.** They were formed algorithmically
from name/intent-token overlap, and because every sibling in this directory
is a `tpunit` fixture whose intent string reads "cluster test verifying X" (or
a close paraphrase), the tokens that drove clustering — "cluster", "test",
"verifying" — are shared by nearly the entire directory. That produces
clusters that look coherent (they group by shared vocabulary) without
necessarily grouping by shared subject matter. `cluster-verifying-command`
(16 units) is the clearest case: its members span compression, membership
control, Jobs-plugin behavior, HTTPS-thread interaction, and a plain SQLite
constraint check — subjects that share nothing except the sentence template
they were named with. `cluster-verifies-through` is coherent on substance
(all five are adversarial multi-node stress/failure scenarios) but its own
agent flagged that its label token ("through") is only literally present in
two of its five members, and that a differently-tokenized cluster could have
picked up the same units under a different name. Only `leader-cluster-follower`
reads as a genuine feature grouping (the leader/follower replication-and-
failover surface) rather than a vocabulary artifact.

The directory's actual organizing principle, underneath all three clusters,
is the shared `BedrockClusterTester` harness and the "provision a real
multi-node cluster, drive it with real requests, assert a cluster-level
invariant" pattern every single unit follows — not the labels attached to
the three groups. A reader of this SUMMARY should treat the cluster split as
a navigation aid for a wide directory, not as a claim that
`test/clustertest/tests` has three thematic sub-areas; it functionally has
one (integration testing via the harness) with one coherent feature-shaped
subset (leader/follower) and a long tail of otherwise-unrelated individual
scenarios.

No child cluster is a poor fit for the directory itself — every unit, across
all three clusters, genuinely is a `BedrockClusterTester`-driven cluster
integration test. The weakness is entirely in the sub-grouping, not in
directory membership.

## 4. Misfits

Every misfit surfaced by the three cluster agents was already judged
resolvable inside `test/clustertest/tests` — most within the same file — and
none require a home outside this directory. Consolidated:

- **BroadcastTest.cpp / `BroadcastCommandTest`** (med, leader-cluster-follower) —
  file name and fixture/class name disagree. `resolved-locally`: rename one to
  match the other.
- **GracefulFailoverTest** (med, leader-cluster-follower) — `threads`/`counts`/
  `allresults` fixture members are shadowed by same-named locals in `test()`,
  set once and never read as members. `resolved-locally`: drop the dead
  members or stop shadowing.
- **FinishJobTest** (med, cluster-verifying-command) — `negativeDelay`/
  `positiveDelay` are implemented but never registered via `TEST(...)`, so they
  silently never run; a real coverage gap in the Jobs plugin's repeat/delay
  semantics, not a placement problem. `resolved-locally`: register the two
  methods in the fixture constructor.
- **ClusterUpgradeTest::setup** (low) — runs a full build pipeline via
  `system()` (git clone/checkout, hardcoded `clang++-18`) from inside the
  fixture. `resolved-locally`, suggested as a standalone build script this
  test shells out to — still local to this directory.
- **LeadingTest::standDownTimeout** (low) — fully implemented, permanently
  disabled via comment rather than removed or gated. `resolved-locally`.
- **UpgradeTest** (low) — unused `#include <libstuff/SRandom.h>`.
  `resolved-locally`.
- **ForkCheckTest** (low) — includes `sqlitecluster/SQLite.h` and
  `SQLiteNode.h` but only uses the transitively-supplied raw `sqlite3` C API.
  `resolved-locally`: swap for `libstuff/sqlite3.h` directly.
- **CompressionTest** (low) — `readDictionaryFile` hardcodes relative
  filesystem paths. `resolved-locally`.
- **ControlCommandTest** / **DoubleDetachTest** (low each) — unused
  `#include <iostream>`. `resolved-locally`.
- **FutureExecutionTest** (low) — unused `#include <fstream>`; also its
  PascalCase test methods break the `testXxx` convention every sibling uses.
  `resolved-locally`.
- **PrePeekPostProcessTest** (low) — `checkWithoutThis` is a tautological
  `ASSERT_EQUAL(1,1)` that exercises nothing; leftover probe code.
  `resolved-locally`.

Nothing found *bottom-up* needs escalation: every misfit above is a same-file
naming/hygiene issue or a test-coverage gap fixable inside the existing file,
and all three cluster agents already made that call. Pass B, with parent
context now available, adds one more misfit that does need to escalate — see
§5.

## 5. Role in the system

Within `test/clustertest`, this directory owns *the assertions*: every
scenario that provisions a live multi-node cluster and checks a cross-node
invariant holds. `test/clustertest/testplugin` owns the opposite half — the
server-side command surface those assertions drive. The boundary between the
two is clean in the direction that matters: this directory never includes
`TestPlugin.h`/`TestPlugin.cpp` (confirmed by grep), it only speaks the wire
protocol testplugin exposes. Nothing to fix there.

The boundary that does leak is with `test/tests/jobs`, reached across an
entirely different top-level test branch. `FinishJobTest.cpp` in this
directory `#include`s `test/tests/jobs/JobTestHelper.h` and calls
`JobTestHelper::getTimestampForDateTimeString` — the single static method it
actually needs — six times. `test/tests/jobs` was designed (per its own
theme) as single-node, `BedrockTester`-driven Jobs-plugin coverage, with no
notion that a multi-node suite under a different top-level test directory
would compile against one of its internal headers. The parent's rollup
already flags this from its own vantage point; this Pass B confirms it from
the consumer's side, and it was not visible bottom-up in Pass A — one
`#include` line inside one file among 36 does not read as a misfit without
the cross-branch context Pass B supplies. Added to this directory's own
escalate list below, with `misfit_count` incremented accordingly.

What this directory needs from `JobTestHelper` is narrow and mundane: one
timestamp-parsing method, used for the same kind of nextRun/lastRun delta
checks `test/tests/jobs`'s own fixtures use it for. Nothing about
`FinishJobTest`'s usage is multi-node-specific — it's boilerplate the two
suites happen to share because they both test the Jobs plugin's timing
semantics, not because this directory needs anything cluster-specific from
that header.

Separately, `BedrockClusterTester`'s prospective move to `test/lib` (raised
by the parent) affects every file in this directory mechanically — all 36
units `#include <test/clustertest/BedrockClusterTester.h>` by full
repo-relative path (confirmed by grep) — but not architecturally: this
directory already lists `test/lib` in `depends_on_dirs` for `BedrockTester.h`,
so the harness would simply move from one already-adjacent location to one
already directly depended upon. From this directory's perspective it is a
find/replace on an include path across the whole directory, not a design
change, and requires no rework of any test logic.

## 6. Inbound expectations

Nothing in `test/clustertest` or its siblings imports symbols *from* this
directory — each unit here is a self-registering `tpunit` fixture linked
into the clustertest binary as a whole, not a library other code calls into.
`depended_on_by` stays empty. What this directory is owed by its own
dependencies, and receives cleanly, is `test/lib`'s `BedrockTester`/
`TestHTTPS` machinery and `test/clustertest/testplugin`'s command surface —
both covered in §5. The one place an expectation crosses a boundary it
shouldn't is the `test/tests/jobs/JobTestHelper.h` reach above, which is this
directory's obligation to stop relying on once that helper moves.

<!-- ROLLUP
theme: Bedrock's multi-node live-cluster integration suite — BedrockClusterTester-driven tests covering leader/follower replication, failover, escalation, membership/priority control, version-skew, and a long tail of individual command/plugin edge cases.
exports: [BedrockClusterTester-based multi-node integration test harness pattern, leader-failover and resync verification, follower-to-leader command escalation checks, replication-consistency checks (writes/schema changes/callbacks/bound params), cluster-membership and priority control coverage, adversarial stress/failure-injection scenarios (rate limits, conflict spam, journal-fork abort, repeated leader death), Jobs-plugin and command-execution edge-case coverage]
depends_on_dirs: [test/clustertest, libstuff, test/lib, sqlitecluster, test/tests/jobs]
depended_on_by: []
misfit_count: {high: 0, med: 4, low: 9}
resolved_locally: 12
escalate:
  - item: JobTestHelper::getTimestampForDateTimeString usage (via test/tests/jobs/JobTestHelper.h)
    from: test/clustertest/tests/FinishJobTest.cpp
    why: cross-branch reach into a sibling top-level test suite's own helper header, which test/tests/jobs was never designed to expose as a shared fixture; this directory needs only one static method from it
    suggested_home: test/lib
-->
