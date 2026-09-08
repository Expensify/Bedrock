# Cluster: cluster-verifying-command

`test/clustertest/tests` — 16 of the directory's ~36 units (Pass C, one cluster of a wide directory; see `.arch/clusters.json`).

## 1. What these units have in common

Weakly, a shared *sentence template*, not a shared subject. Every `intent` string
in this cluster opens with "Cluster test verifying..." or "Cluster-level tpunit
fixture verifying/covering...", which is almost certainly why the algorithmic
grouping pulled these 16 together out of the directory's ~36 — token overlap on
"cluster"/"test"/"verifying", not a common feature area.

What they actually verify is scattered across unrelated parts of Bedrock:
journal compression (Compression), cluster-membership/priority control
(ControlCommand's attach-prevention, DoubleDetach, Permafollower, SetPriority),
command-execution edge cases under a live cluster (FutureExecution, Timeout,
Timing, ThreadException, HTTPSBlockingCommit, HTTPS, PrePeekPostProcess), fast
leader stand-down under load (FastStandDown), the Jobs plugin (FinishJob,
JobID), and a plain SQLite unique-constraint check (UniqueConstraints). These
would sort into at least four or five different feature-based clusters if the
grouping used behavior rather than phrasing.

The one real thing they share mechanically: all 16 are `tpunit` fixtures built
on `test/clustertest/BedrockClusterTester.h`, each spinning up a multi-node
cluster and driving it through `libstuff/SData.h` requests to assert on
responses — the standard clustertest harness pattern, used here for whatever
single command or behavior each file names itself after. That harness usage,
not the "verifying" phrasing, is the durable commonality worth carrying
forward.

**Verdict on the clustering itself:** this cluster is not thematically
coherent as a feature group. It is coherent only as "instances of the
clustertest-harness pattern," which is true of nearly the whole directory and
so carries little discriminating information. Flagging per the task's note:
this is a case where the directory's vocabulary is too uniform ("cluster test
verifying X" repeated 36 times) for name/intent-token clustering to produce a
meaningful split.

## 2. One line per unit

| Unit | Lines | Verifies |
|---|---|---|
| CompressionTest | 279 | Journal entries compress under zstd once a dictionary is configured, decompress correctly, including after a full-cluster restart. |
| ControlCommandTest | 53 | A `preventattach` control command blocks a subsequent Attach until its internal delay passes. |
| DoubleDetachTest | 52 | Detaching an already-detached follower is rejected the second time; the node still re-attaches cleanly. |
| FastStandDownTest | 120 | Leader stands down/fails over quickly even with a slow (5s HTTPS or 5s-future-scheduled) command outstanding, which still completes afterward. |
| FinishJobTest | 605 | Jobs plugin's FinishJob command: error handling, parent/child pause-and-requeue, repeat/delay/nextRun. |
| FutureExecutionTest | 87 | Leader-only scheduled writes land only once their time passes; a query blocked on an unreachable commit count times out rather than hanging. |
| HTTPSBlockingCommitTest | 99 | A command that would make an HTTPS request on the serialized blockingCommit thread is refused, whether caught pre-check or during peek's wait. |
| HTTPSTest | 123 | Concurrent HTTPS + conflicting commands never cause the test plugin to run peek twice for the same HTTPS attempt. |
| JobIDTest | 97 | A node becoming leader after restart correctly re-initializes the jobs table's last-used ID, avoiding unique-ID conflicts. |
| PermafollowerTest | 53 | A priority-0 permafollower never counts toward write quorum, whether it is up or down. |
| PrePeekPostProcessTest | 91 | testplugin commands can hook prePeek/postProcess; process()-time DB writes are visible in postProcess but not in peek/prePeek. |
| SetPriorityTest | 210 | SetPriority command's input validation plus six-node priority/leadership/quorum/conflict scenarios. |
| ThreadExceptionTest | 23 | An exception thrown on a worker thread surfaces as a 500 rather than crashing or hanging the server. |
| TimeoutTest | 249 | Command timeouts at every stage (peek/process/postProcess/total/HTTPS/future-commit) and client-disconnect (abort-and-rollback vs. fire-and-forget) semantics. |
| TimingTest | 111 | Per-command timing instrumentation (leader's own + follower-reported upstream peek/process/total) is internally consistent. |
| UniqueConstraintsTest | 21 | A duplicate-key INSERT succeeds once, then is rejected 400 the second time by SQLite's unique constraint. |

All 16 depend on `libstuff/SData.h` and `test/clustertest/BedrockClusterTester.h`; a few additionally pull in `libstuff/SQResult.h`, `libstuff/libstuff.h`, `libstuff/SFastBuffer.h`, `test/lib/tpunit++.hpp`, `test/tests/jobs/JobTestHelper.h`, or the top-level `BedrockCommand.h`.

## 3. Misfits

**Relative to this cluster specifically:** none of the 16 are excluded by the
cluster's nominal theme in any meaningful way, because the theme itself
("verifying" phrasing) is broad enough to admit almost anything in the parent
directory. There is no unit here that belongs to some *other*, better-fitting
cluster that this grouping wrongly split off — the miss is the cluster
boundary itself, not any one member.

**Per-unit issues surfaced in the input** (none of these are placement
problems; all are fixable in the same file, so treated as resolved-locally
rather than escalated):

- **FinishJobTest** (med) — `negativeDelay` / `positiveDelay` are fully
  implemented test methods that are never registered via `TEST(...)` in the
  fixture's constructor, so they silently never run. This is a correctness gap
  in test coverage (repeat/delay semantics may be untested despite looking
  covered), not a location issue — worth a human's attention regardless.
- CompressionTest (low) — `readDictionaryFile` hardcodes two relative
  filesystem paths instead of using a shared fixture-path helper.
- ControlCommandTest (low) / DoubleDetachTest (low) — unused `#include <iostream>`.
- FutureExecutionTest (low) — unused `#include <fstream>`.
- PrePeekPostProcessTest (low) — `checkWithoutThis` is a tautological
  assertion (`ASSERT_EQUAL(1,1)`) that doesn't exercise any Bedrock behavior;
  reads as leftover probe code.

No high-severity issues. Naming/location/name-fit scores are 4-5 across the
board except FutureExecutionTest (naming 3 — its PascalCase test methods break
the `testXxx` camelCase convention every sibling uses) and PrePeekPostProcessTest (naming 3, for `checkWithoutThis`).

<!-- ROLLUP
theme: cluster-integration tests grouped by shared "cluster test verifying X" phrasing, not by a common feature — actual subjects span compression, cluster-membership/priority control, command-execution edge cases, the Jobs plugin, and a SQLite constraint check
exports: [BedrockClusterTester-driven multi-node integration test pattern, cluster-membership/priority command coverage (SetPriority, DoubleDetach, Permafollower, ControlCommand attach-prevention), command execution-edge-case coverage (timeouts, future/scheduled execution, HTTPS-thread interaction, prePeek/postProcess hooks, worker-thread exceptions, per-command timing), fast leader stand-down/failover under load, Jobs plugin coverage (FinishJob, job-ID reinitialization), journal compression verification, SQLite unique-constraint check]
depends_on_dirs: [libstuff, test/clustertest, test/lib, test/tests/jobs]
depended_on_by: []
misfit_count: {high: 0, med: 1, low: 5}
resolved_locally: 6
escalate: []
-->
