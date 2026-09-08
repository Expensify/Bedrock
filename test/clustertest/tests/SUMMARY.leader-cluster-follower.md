# Cluster: leader-cluster-follower

## 1. What these units have in common

All 15 units are `tpunit`-based cluster tests that spin up a multi-node Bedrock
cluster via `test/clustertest/BedrockClusterTester.h` and assert on some
specific interaction between the **leader** and its **follower(s)**: does a
write replicate, does a command escalate correctly, does the cluster survive a
failover, does version skew between nodes get handled safely. Every unit
depends on `test/clustertest/BedrockClusterTester.h` and `libstuff/SData.h`;
several add `libstuff/SRandom.h`, `libstuff/SQResult.h`, `libstuff/libstuff.h`,
or `test/lib/tpunit++.hpp`. Unlike some algorithmically-formed clusters, this
one is not just token overlap from a uniform "test of X" vocabulary — it is a
genuinely coherent group: every unit here exercises the same underlying
leader/follower state-machine surface (replication, escalation, failover,
version compatibility, status reporting) through the same test harness. This
looks like the real substance of what makes `test/clustertest/tests` a
"clustertest" directory in the first place.

Within that shared shape, the units split into a few sub-themes:
- **Replication correctness**: does a write/schema-change/callback reach every
  follower (AfterCommitCallbackClusterTest, MassiveQueryTest, UpgradeDBTest,
  WriteLocalUnreplicatedClusterTest).
- **Escalation correctness**: does a follower-received command reach and
  return from the leader properly (EscalateTest, VersionMismatchTest,
  SynchronousCommandsTest).
- **Failover / lifecycle**: leader election, restart, resync, rolling upgrade
  (LeadingTest, GracefulFailoverTest, ClusterUpgradeTest, BadCommandTest).
- **Status / health reporting**: what a node claims about its own role and
  peers (StatusTest, StatusHandlingCommandsTest).
- **Version-skew regression**: pinned bugs from specific version-mismatch
  incidents (UpgradeTest, BroadcastTest).

## 2. Units

| Unit | Lines | What it verifies |
|---|---|---|
| AfterCommitCallbackClusterTest | 65 | After-commit callback fires on both leader-local commit and follower-applied replication |
| BadCommandTest | 127 | Leader survives bad commands (500s, encoded exceptions) and a crash triggers clean follower failover |
| BroadcastTest | 87 | `broadcastwithtimeouts` command's peekedAt/timeout values are received correctly by every follower |
| ClusterUpgradeTest | 243 | Rolling upgrade across a mixed old/new 3-node cluster survives failover in both upgrade directions |
| EscalateTest | 107 | Follower→leader escalation works, including serialized-data round-trip, a deserialize-failure path, and socket reuse |
| GracefulFailoverTest | 228 | Cluster under continuous client load survives graceful and SIGKILL restarts of leader and follower with no bad responses |
| LeadingTest | 190 | Leader lifecycle: initial election, failover, leader restart reclaiming leadership, follower resync after downtime |
| MassiveQueryTest | 53 | A large write sent to a follower escalates, commits on leader, and replicates to other followers |
| StatusHandlingCommandsTest | 60 | `/status/handlingCommands` on a follower correctly reports its own LEADING/FOLLOWING transition while leader is down |
| StatusTest | 63 | Concurrent Status calls to all 3 nodes report correct role and peer-list size from each node's own view |
| SynchronousCommandsTest | 66 | `-synchronousCommands` routes a named command to the blocking commit thread, on leader and via follower escalation, without affecting an unlisted sibling command |
| UpgradeDBTest | 51 | A schema change from `upgradeDatabase` replicates to every follower |
| UpgradeTest | 62 | Regression guard (PR #1293): pipelining two commands on one socket to a version-mismatched follower no longer hangs |
| VersionMismatchTest | 70 | A version-mismatched follower escalates every command, reads included, instead of running reads locally |
| WriteLocalUnreplicatedClusterTest | 77 | `writeLocalUnreplicated` writes commit on leader without journaling/replication, without breaking normal replicated commits |

## 3. Misfits

No unit in this cluster is thematically out of place — all 15 test some facet
of leader/follower cluster behavior through the same harness, so there is
nothing here that belongs in a different cluster instead. The unit-level
`misfits` that were flagged are internal quality issues, not cluster-fit
problems, and all look resolvable in place (inside `test/clustertest/tests`,
usually inside the same file):

- **BroadcastTest.cpp / `BroadcastCommandTest`** (med): the file is named
  `BroadcastTest.cpp` but its fixture struct, registered name, and global
  instance are all `BroadcastCommandTest` — file and contents disagree.
  Resolvable locally: rename the file to match, or vice versa.
- **GracefulFailoverTest** (med): `threads`/`counts`/`allresults` struct
  members are shadowed by identically-named locals inside `test()`; the
  members are set once and never read again, which reads as live state but
  isn't. Resolvable locally: drop the dead members or stop shadowing them.
- **ClusterUpgradeTest::setup** (low): runs a full build/release pipeline via
  `system()` (git clone/checkout, a hardcoded `clang++-18` compile command)
  from inside the test fixture, plus a bare `brdata.txt` temp file instead of
  `BedrockTester::getTempFileName`. Suggested home: a standalone build script
  this test shells out to — still local to this test's needs, not a
  different directory.
- **LeadingTest::standDownTimeout** (low): fully implemented but permanently
  disabled via a "Disabled for speed" comment rather than removed or gated —
  dead test code sitting in a live file.
- **UpgradeTest** (low): `#include <libstuff/SRandom.h>` appears unused —
  likely a leftover from an earlier version of the test.

## 4. ROLLUP

<!-- ROLLUP
theme: Multi-node leader/follower cluster tests (BedrockClusterTester-driven) covering replication, command escalation, failover/lifecycle, version-skew handling, and status reporting.
exports: [leader-failover and resync verification, follower-to-leader command escalation checks, replication-consistency checks (writes/schema changes/callbacks), rolling-upgrade and version-mismatch handling, cluster status/role reporting, BedrockClusterTester-based multi-node test harness usage]
depends_on_dirs: [test/clustertest, libstuff, test/lib]
depended_on_by: []
misfit_count: {high: 0, med: 2, low: 3}
resolved_locally: 5
escalate: []
-->
