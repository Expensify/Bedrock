# 99 — Synthesis: WAL2 vs HC-Tree for Expensify

**Status:** covers units 0, 3–8 and the four standing priorities. Units 1 (write path) and
2 (locking) remain as separate files, but their substance is largely absorbed into
`10-conflict-investigation.md`.
**Evidence rule:** every claim below is traced to source in
`hctree-bedrock-lcd-ex @ eedd80c1a9749300`, or to Bedrock at commit `3dc7b9f`. Claims that
could **not** be verified in source are marked **UNVERIFIED** and collected in §6.
**Drift:** production runs check-in `bf843733…` (2026-08-28). The only functional
difference from the check-in analysed is the `OP_IdxDelete` no-op skip
(`00-provenance.md`). Nothing else in this document is affected.

---

## 1. The direct answer to "why doesn't row-level locking help?"

**Because the premise is wrong, and because two other things are true.**

WAL2's `BEGIN CONCURRENT` is **not** page-level. It records reads as key ranges
(`BtReadIntkey{iRoot, iMin, iMax}`) and detects conflicts against write **keys**
(`btreeBcDetectIntkeyConflict()`, `btree.c:1361`). Both engines conflict at row/key
granularity. So the expected win never existed at the engine level.

On top of that:

**(a) A large class of Bedrock's conflicts is engine-independent.** Every write transaction
runs `SELECT MIN(id)` at the head of its journal shard (`SQLite.cpp:952`), recording the
read range `[SMALLEST_INT64, K]`; journal trimming deletes the ten lowest rows starting at
exactly `K` (`SQLite.cpp:969`). That is a genuine same-row read/write conflict, detected
identically by both engines. **Row-level locking cannot reduce it by one.**

**(b) Bedrock's own conflict-damping mechanism is defeated by HC-Tree's precision.**
`ConflictLockGuard` makes a retrying command take a mutex keyed on where it last conflicted
(`BedrockServer.cpp:686`). Under WAL2 that key is a **page number**, which groups every
command touching that page — the coarseness is what makes the mitigation work. Under
HC-Tree it is `hash(table + row key)` (`SQLite.cpp:432`), which groups almost nothing and
thrashes the 500-entry mutex LRU. **A more precise engine made the mitigation useless.**

And HC-Tree pays more per conflict: WAL2 validates with an in-memory sorted merge join,
touching no pages and no shared state; HC-Tree physically re-traverses the B-tree over
every read range with a shared transaction-map lookup per row (`hct_database.c:8191`).
Slower validation widens the window for invalidation, which feeds back into more conflicts.

**Confidence:** high on the mechanism for all three. **The magnitudes are unmeasured**, and
(b) is gated on whether `-enableConflictPageLocks` is on in production (§6, Q1).

## 2. Where each engine is genuinely stronger

### HC-Tree is better at

| | Evidence |
|---|---|
| **No checkpointing at all** — the entire starvation failure class disappears | `sqlite3HctBtreeCheckpoint()` body is `return SQLITE_OK;` (`hctree.c:3753`) |
| **Idle connections are free.** Only *actively reading* clients pin the GC horizon; idle ones are CAS-advanced automatically | `sqlite3HctTMapScan()`, `hct_tmap.c:592`. Under WAL2 any reader holding a read mark blocks the file switch (`wal.c:5010`) — a serious difference for a large connection pool |
| **Single-layer memory model** — the mapping *is* the cache, so the OS uses all 6 TB without SQLite second-guessing it | `hct_file.c:713`; no pager (`hctree.c:761`). Removes a tuning parameter currently set wrong |
| **Growth is more benign** under a long reader — an under-recycled file costs disk; a long WAL costs disk *and* read latency | `04-checkpointing.md` §1 vs `06-readers-snapshots.md` §2 |
| **Cleaner recovery model** — per-row TIDs mean a partial transaction is never visible; no undo needed | `hct_database.c:8092`, `hct_tmap.c:634` |
| **Fails safe on multi-process** — exclusive `fcntl` lock, `SQLITE_BUSY`, log names the PID | `hctFileLock()`, `hct_file.c:638` |

### WAL2 is better at

| | Evidence |
|---|---|
| **Validation cost** — merge join vs physical re-traversal. This is the single biggest engine-level advantage | `btree.c:1361` vs `hct_database.c:8191` |
| **Read-set coalescing** — sorts and merges adjacent ranges; HC-Tree never coalesces index ops | `btree.c:1050` vs `hct_database.c:2242` |
| **Durability is at least configurable** — `synchronous` works. HC-Tree issues **no fsync anywhere** and offers no control | `05-recovery-durability.md` §3 |
| **Defensive recovery** — checksum chain bounds the parse; HC-Tree's log parser validates neither offset nor length | `wal.c:1331` vs `12-bugs.md` #7 |
| **Multi-process capable** (with a flag-matched binary) — enables out-of-process tooling | `09-multiprocess.md` §4 |
| **Maturity** — `btree.c` is decades-hardened; the HC-Tree files carry stale docs (`hct_tmap.c:46` "TODO: This all needs updating!!!"), disabled code paths, and describe structures that do not exist (`HctTMapRef`) |

## 3. Concrete risks at 6 TB / 384 CPUs

Ordered by expected severity.

1. **No durability control on HC-Tree, and a recovery parser that doesn't validate input.**
   The combination is the real concern: no fsync means torn logs are the *expected* input
   after power loss, and `hctLogReaderNext()` trusts an unvalidated offset and length
   (`12-bugs.md` #7). The uncovered scenario is a **correlated** power event, where
   replication provides no clean peer. **Fixable** — items 1–3 in that entry are a few lines
   each.
2. **Shared-structure contention at 384 CPUs.** `hctDbTMapLookup()` runs per validated row
   with its `iLocalMinTid` fast path **commented out** (`hct_database.c:995`); the page
   allocator has one global mutex (`hct_pman.c:103`); `sqlite3HctTMapScan()` walks all
   clients under a global mutex (`hct_tmap.c:596`). **All three are already instrumented**
   and measurable today (§4).
3. **Conflict-rate regression**, per §1 — with the good news that the largest identified
   contributors are in Bedrock's own code, not SQLite's.
4. **No out-of-process tooling.** On HC-Tree the in-process virtual tables are the *only*
   way to inspect a live database (`09-multiprocess.md` §5.3). Backups need a volume
   snapshot or a stopped node.
5. **Disk-space leak.** HC-Tree log files are never unlinked at runtime — both `unlink()`
   calls are disabled (`hct_log.c:268`, `:444`) — and the leak is active in exactly
   Bedrock's NORMAL-mode configuration (`12-bugs.md` #4).
6. **Everything runs at upstream defaults.** Bedrock sets none of HC-Tree's ten tunable
   pragmas (`06-readers-snapshots.md` §5). Not a risk so much as unclaimed headroom.
7. **Mapping limit is *not* a near-term risk** — ~5–9 % of `vm.max_map_count` at 6 TB,
   ~10× headroom to the ~64 TiB ceiling (`07-mmap.md` §2). Recorded to close it off.

## 4. Recommended verification, in order

**Everything in tier 1 costs nothing and reorders everything below it. Do it before
changing any code.**

### Tier 1 — no rebuild, no deploy

1. **Search existing logs for `slow HC-Tree commit`.** Bedrock already dumps the full
   `hctstats` counter set for every HC-Tree commit over 100 ms
   (`SQLite.cpp:1181-1188`). The data to settle risk #2 may already be in the archive.
   Compute `tmap.mutex_block / tmap.mutex_attempt`, `pman.mutex_block / pman.mutex_attempt`,
   `file.cas_fail / file.cas_attempt`, and `db.tmap_lookup` per transaction.
2. **Grep conflict logs for `journal*`.** Both engines log conflicts with the table name
   resolved and Bedrock already parses them (`SQLite.cpp:411-435`). The share naming a
   journal table directly sizes §1(a). *Trap:* the engines log under different result codes
   (WAL2 `SQLITE_OK`, HC-Tree `SQLITE_BUSY_SNAPSHOT`), so filter on message text.
3. **Answer Q1–Q4 in §6.** Configuration questions, not investigations.
4. **Check `vm.max_map_count` and `wc -l /proc/<pid>/maps`** on a production node — closes
   risk #7 with one command.

### Tier 2 — cheap experiments

5. **Set HC-Tree pragmas and measure.** `hct_npageset` (default 256) is the allocator batch
   size and the direct answer to allocator-mutex contention; `hct_npagescan` (1024) trades
   scan frequency against reuse latency. Both are runtime pragmas.
6. **Run a write workload under `PRAGMA noop_update`.** It rewrites every `SET` to `+column`
   (`update.c:468`), so nothing changes value. Any remaining conflicts and index
   delete/insert traffic are pure write amplification — exactly what optimization A2 would
   remove, measured without writing code.
7. **Set `journal_size_limit`** — currently unset, leaving the WAL2 switch threshold at 1000
   frames / ~4 MB (`wal.c:534`). One line, reversible. *(WAL2 only; diagnostic value for
   understanding the starvation history rather than a P2 goal.)*

### Tier 3 — rebuild

8. **`-DHCT_VALIDATE_TIMERS`** on one node. Logs per-op validation time and `nStep` — the
   row count re-walked during validation, which is precisely the quantity §1 claims is
   HC-Tree's structural cost. If `nStep` is large, A1 and B1 are confirmed; if small, §1's
   third mechanism is wrong and attention moves to §1(a)/(b).

### Tier 4 — code changes, once measured

9. Fix `ConflictLockGuard`'s HC-Tree identifier (A4) — likely the largest single win, and
   entirely in Bedrock's code.
10. Take journal housekeeping off the write path (B7) — cache the per-shard min-id, move
    trimming to a background sweep.
11. Sort and coalesce HC-Tree read ranges (A1) — port of `btree.c:1050`, no semantic change.
12. Fix `12-bugs.md` #7 (recovery input validation) — small, local, and worth doing
    independent of any benchmark.

## 5. A recommendation

**On the evidence so far, HC-Tree is the right long-term direction, and the conflict
problem that prompted this analysis is mostly not HC-Tree's fault.**

The reasoning: HC-Tree eliminates checkpoint starvation outright, makes idle pooled
connections free, and fits a 6 TB-RAM host better with its single-layer memory model. Those
are structural advantages that tuning cannot give WAL2. Meanwhile the largest identified
contributors to the conflict regression — the journal-head pattern and the
`ConflictLockGuard` identifier — are **in Bedrock's own code and fixable without upstream
involvement**.

The genuine reservations are not about the concurrency design but about **maturity**:
no fsync and no way to enable one, a recovery parser that trusts its input, disabled
`unlink()` calls, stale documentation describing structures that do not exist, and
commented-out fast paths. These read as a system that is architecturally further along than
it is operationally hardened.

**So: continue with HC-Tree, but treat §4 tiers 1–2 as prerequisites rather than
follow-ups, and put the durability question (§6 Q5) to a decision before scaling further.**

**Confidence:** moderate. The mechanisms are verified in source; the magnitudes are not.
Tier 1 could plausibly overturn the ranking in §4 — that is exactly why it comes first.

## 6. Open questions

**For Dan (configuration — these gate findings):**

1. **Is `-enableConflictPageLocks` enabled in production?** Gates §1(b), the strongest
   single finding. Defaults to `false` (`BedrockServer.h:393`).
2. Where does our production conflict metric come from? The engines log under different
   result codes and HC-Tree additionally dooms transactions *eagerly* mid-scan
   (`hct_database.c:5905`) with no WAL2 equivalent — so the two may not be counting the
   same event.
3. Does production pass an explicit `-cacheSize`? (WAL2 nodes only — it is a no-op on
   HC-Tree.)
4. Does the production schema use expression indexes on hot tables? Determines whether the
   newer check-in's `OP_IdxDelete` fix is worth anything to us.
5. **Which durability position are we taking** — hardware-backed writes, geographic
   separation, or restore-from-backup? (`05-recovery-durability.md` §4.)
6. How are backups of a live HC-Tree node taken today?
7. Was there ever a local mutex-alert patch to `sqlite3.c`? `SQLITE_MUTEX_ALERT_MILLISECONDS`
   is dead (`12-bugs.md` #1) and the amalgamation carries no local patches, so if it existed
   it is gone.

**For Dan Kennedy (upstream):**

8. Why is `iLocalMinTid` commented out in `hctDbTidIsConflict()` (`hct_database.c:995`)?
   Highest-ceiling contention fix, blocked on this.
9. Are the two disabled `unlink()` calls in `hct_log.c` intentional?
10. Is the absence of any `fsync` in HC-Tree deliberate and permanent?
    `sqlite3HctBtreeSetPagerFlags()`'s own comment asks the same question.
11. The unvalidated `iFile` / `nByte` in log recovery (`12-bugs.md` #7).
12. Could the `OP_IdxDelete` no-op skip be generalised beyond expression indexes (A2)?
13. `hct_tmap.c:59-64` describes an `HctTMapRef` revocable-reference design that does not
    exist in the code. Is `sqlite3HctTMapScan()` the intended replacement?
14. What is the intent of `bReadOnlyMap`, whose only enabler is commented out
    (`hct_file.c:1417`)? Is out-of-process read-only access planned?

## 7. UNVERIFIED claims

Stated for completeness — these are the places this analysis reasons beyond what source
proves:

- **Every magnitude.** No production measurement was available. All rankings in §4 are by
  expected value, not observation.
- **That §1's three mechanisms account for the *observed* conflict rate.** Each is verified
  as a mechanism; their relative contribution is not.
- **That validation cost feeds back into conflict rate.** The causal chain is plausible and
  the components are verified, but the loop itself is inferred, not measured.
- **Shard-collision frequency for the journal-head conflict.** Round-robin over ~384 shards
  means transactions *i* and *i+384* collide; whether they overlap in time is a throughput
  question not settled here.
- **Whether `sqlite3HctPManStats()` aggregates across clients** or reports only the querying
  connection's counters (`13-instrumentation.md` §1). Changes how to read the numbers.
- **Reachability of the POSIX-lock footgun** in `09-multiprocess.md` §2 — noted as a
  pattern, not demonstrated in Bedrock.
- **That `pFakePager` is never dereferenced** (`12-bugs.md` #6). No misuse found, but the
  search was not exhaustive.
- **Production configuration generally.** The config in this repo is a sample
  (`configs/bedrock.conf`); what production passes is unknown, which is why Q1–Q4 exist.
