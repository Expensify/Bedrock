# Cluster: cluster-verifies-through

## What these units have in common

Five tests, all `tpunit` fixtures built on `BedrockClusterTester`, each standing up
a live multi-node (3- or 5-node) Bedrock cluster and driving it through one
specific adversarial or edge condition, then asserting a single correctness
invariant survives it:

- a rate limiter's threshold state (`BlockingQueueRateLimitTest`)
- a bound SQL parameter's value, as it replicates through the journal and
  through `RemoteSQLite` (`BoundParametersTest`)
- data/journal/row-count convergence under a flood of conflicting writes
  (`ConflictSpamTest`)
- a manufactured journal fork, expected to trip a hard abort rather than
  silently rejoin (`ForkCheckTest`)
- the SYNCHRONIZING state transition after repeated leader death/restart
  (`MultipleLeaderSyncTest`)

That is a real, substantive theme -- not just shared vocabulary -- every unit
here provisions a real cluster, forces a stress or failure condition, and
checks a specific downstream invariant across nodes. That said, the cluster's
*label* (`cluster-verifies-through`) is only weakly diagnostic on its own:
"verifies" and "cluster" are generic words that most of `test/clustertest/tests`
would match (this is exactly the uniform-vocabulary case the task brief warned
about), and "through" only literally appears in two of the five intents
(`BlockingQueueRateLimitTest`'s "round-trip through Status",
`BoundParametersTest`'s "through RemoteSQLite"). The grouping happens to be
coherent, but that coherence comes from the shared `BedrockClusterTester`
multi-node/failure-injection pattern, not from the token overlap that formed
it -- a different cluster elsewhere in this directory could easily match the
same label tokens without sharing this actual pattern.

## Units

| Unit | Lines | What it verifies |
|---|---|---|
| `BlockingQueueRateLimitTest` | 143 | Time-based rate-limit window/threshold settings round-trip through `Status`; concurrent conflicting commands on a shared identifier trip per-identifier blocking once the threshold is exceeded. |
| `BoundParametersTest` | 141 | Regression test (PR #2600): a leader write with a named bound SQL parameter replicates correctly to followers, with the journal holding the inlined literal (not the raw placeholder); same path re-checked through `RemoteSQLite`. |
| `ConflictSpamTest` | 295 | Stress test firing high volumes of conflicting writes at all three nodes serially then concurrently; cluster must converge to identical data, journals, and row counts with zero request failures. |
| `ForkCheckTest` | 139 | Manufactures a fork by advancing a follower's journal past a stopped leader and hand-corrupting the leader's last journal-entry hash via raw `sqlite3`; restarted leader must `SIGABRT` rather than rejoin. |
| `MultipleLeaderSyncTest` | 187 | Kills the leader twice on a 5-node cluster with writes between failures; both returning nodes must pass through SYNCHRONIZING before re-leading or following. |

## Misfits

One unit-level (not cluster-fit) misfit was flagged in the input and is worth
carrying forward:

- **`ForkCheckTest`** includes `<sqlitecluster/SQLite.h>` and
  `<sqlitecluster/SQLiteNode.h>` but references neither class -- only the raw
  `sqlite3` C API (`sqlite3_open_v2`/`sqlite3_exec`/`sqlite3_close_v2`), which
  those headers only supply transitively. Severity low. This is an in-file
  hygiene issue, not a cluster-membership problem -- `ForkCheckTest` fits this
  cluster's theme (multi-node failure injection) as well as any other member.
  **Resolvable here**: swap the two includes for `libstuff/sqlite3.h` directly
  in `test/clustertest/tests/ForkCheckTest.cpp`. Marked `resolved-locally`.

No unit in this cluster fails to fit the *cluster's* theme (all five build a
multi-node cluster via `BedrockClusterTester` and assert one failure/stress
invariant). One naming-quality soft spot: `ConflictSpamTest`'s test methods are
named generically (`slow`, `spam`) and only read clearly via the class name and
header comment -- noted, not a misfit.

## What this input cannot tell me

This is a partial view of `test/clustertest/tests` (5 of ~36 children per the
spec's own count). I can't say whether the *directory's* clustering as a whole
is sound, whether other clusters overlap in theme with this one (e.g. another
group of leader-failure or replication tests), or whether the label
`cluster-verifies-through` was applied consistently elsewhere. That judgment
belongs to whoever assembles the directory-level Pass A from all cluster
summaries.

<!-- ROLLUP
theme: cluster-level BedrockClusterTester scenarios that inject a specific stress or failure condition (rate-limit pressure, bound-param replication, write-conflict spam, journal-hash corruption, repeated leader death) and assert one correctness invariant survives it
exports: [rate-limit-window-status-roundtrip, bound-param-journal-replication, conflict-spam-convergence, fork-corruption-abort, multi-leader-failure-synchronizing-transition]
depends_on_dirs: [libstuff, test/clustertest, test/lib, sqlitecluster]
depended_on_by: []
misfit_count: {high: 0, med: 0, low: 1}
resolved_locally: 1
escalate: []
-->
