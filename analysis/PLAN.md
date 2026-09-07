# PLAN — Line-by-line comparative analysis of WAL2 vs HC-Tree

**Status:** active
**Branch:** `claude/optimistic-albattani-auhvnk`
**Started:** 2026-09-07

---

## 0. Scope and ground truth

We are comparing the two storage/concurrency engines that coexist in the single SQLite
source tree Expensify runs in production via Bedrock:

- **WAL2** — the classic B-tree pager (`btree.c` / `pager.c`) with the two-WAL journal mode
  (`wal.c`), plus Expensify's `SQLITE_ENABLE_WAL_BIGHASH` / `SQLITE_ENABLE_WAL2NOCKSUM`
  modifications.
- **HC-Tree** — the alternative storage engine (`src/hct*`) dispatched through
  `src/btwrapper.c` / `src/btreeModules.h`, selected at runtime per database.

Both engines live in one amalgamation, built once with one flag set
(Makefile `AMALGAMATION_FLAGS`, line 18). Engine selection is at **runtime**, per database:
`-newDBsUseHctree` / URI `hctree=1` on first open, header sniff ("Hctree database version")
thereafter (`SQLite::validateDBFormat`); non-hctree DBs get `PRAGMA journal_mode = WAL2`
(`sqlitecluster/SQLite.cpp`).

### Verified provenance (Phase 0, confirmed)

| Item | Value |
|---|---|
| Upstream repo | `https://sqlite.org/hctree` (fossil) |
| Branch | `hctree-bedrock-lcd-ex` |
| Check-in | `eedd80c1a974930025bf713a44457f51e1f18998d371f6920f97220e20b561fb` |
| Check-in date | 2026-09-07T15:24:37Z, "Merge latest changes from branch hctree-bedrock." |
| Upstream VERSION | 3.54.0 |
| Bedrock vendored drop | commit `b655fd9`, 2026-08-31, "Latest SQLite from Dan" |
| Vendored file | `libstuff/sqlite3.c`, 10,467,329 bytes |

The check-in **post-dates** Bedrock's vendored drop by ~7 days, so drift is expected and
must be classified, not assumed away.

### Sandbox capabilities (confirmed)

- `sqlite.org` is reachable; tarball fetched (13.5 MB) and extracted. Fossil is not
  installed, so the tarball path was used; `manifest.uuid` confirms the exact check-in.
- `tclsh` was not present but installs from apt — required to run `tool/mksqlite3c.tcl`
  for amalgamation generation.

### Source size baseline (this check-in)

| WAL2 side | lines | HC-Tree side | lines |
|---|---|---|---|
| `src/btree.c` | 13,977 | `src/hct_database.c` | 9,978 |
| `src/os_unix.c` | 8,770 | `src/hctree.c` | 3,984 |
| `src/pager.c` | 8,119 | `src/hct_file.c` | 2,993 |
| `src/wal.c` | 5,978 | `src/hct_tree.c` | 1,430 |
| `src/btreeInt.h` | 984 | `src/hct_pman.c` | 1,103 |
| `src/wal.h` | 185 | `src/hct_journal.c` | 966 |
| | | `src/hct_tmap.c` | 984 |
| | | `src/btwrapper.c` | 719 |
| | | (+ hct_log/journalhash/record/stats, headers) | ~1,700 |
| **total** | **38,013** | **total** | **24,509** |

---

## 0b. Standing investigation priorities (from Dan, 2026-09-07)

These override the neutral "compare everything evenly" framing. Every subsystem unit
below must be read with these three questions in hand, and anything relevant routed into
the dedicated files named here.

### P1 — Why does HC-Tree produce *the same or more* conflicts than WAL2?

This is the headline question. HC-Tree advertises row-level locking and should therefore
conflict *less* than WAL2's page-level granularity; production says otherwise. Treat this
as an active investigation, not a comparison. Candidate hypotheses to confirm or kill in
source, each of which would explain "row-level locking that doesn't buy you anything":

- **H1 — Granularity is not actually row-level on the paths Bedrock uses.** Conflict
  detection may fall back to page/range granularity for index scans, `OP_IdxDelete`,
  overflow cells, or interior-node splits, so the effective granularity is coarser than
  advertised for real workloads.
- **H2 — Validation scope is wider than the write set.** If the commit-time validation
  compares against a *read* set, a scanned key range, or a whole-table sequence number,
  then read-mostly transactions collide even when their writes are disjoint.
- **H3 — Shared hot structures.** Free-page/pointer-map equivalents, the journal/log
  append point, `sqlite_sequence`-style counters, root-page metadata, or Bedrock's own
  journal tables may be written by *every* transaction, giving a guaranteed conflict edge
  regardless of engine granularity.
- **H4 — Bedrock's journal tables are the true conflict set.** Every Bedrock commit writes
  the journal; if those rows/pages are adjacent or monotonically appended, row-level
  locking on a hot tail is worth little.
- **H5 — False conflicts from hash/bitmap aliasing.** If validation uses a hashed or
  bitmap-summarized representation of the write set, collisions produce false positives
  whose rate grows with transaction size and concurrency — and would get *worse* at 384
  CPUs, not better.
- **H6 — Retry/abort accounting differs.** HC-Tree may report as a conflict what WAL2
  reports as a busy/blocked wait, so the two engines' "conflict" counters are not
  measuring the same event. This would make the comparison itself partly an artifact.
- **H7 — Interaction with Bedrock's own layer.** `BedrockConflictManager` /
  `ConflictLockGuard` may serialize or retry differently depending on the error code the
  engine returns, amplifying engine-level conflicts into observed ones.

Deliverable: **`analysis/10-conflict-investigation.md`** — each hypothesis stated,
evidence for and against with `file:line`, and a verdict of confirmed / killed /
UNVERIFIED-needs-experiment. Where source cannot settle it, specify the exact experiment
or instrumentation that would.

### P2 — WAL2 optimizations portable to HC-Tree

One-directional: we care about improving HC-Tree, not WAL2. As each subsystem is read,
log anything WAL2 does that HC-Tree does not, and that HC-Tree could adopt — including
Expensify's own WAL2-side modifications (`WAL_BIGHASH`, `WAL2NOCKSUM` are the obvious
candidates: a bigger hash table and a skipped checksum are both engine-agnostic ideas).
Note where HC-Tree deliberately does not need the optimization.

Deliverable: **`analysis/11-portable-optimizations.md`** — running list, each entry with
the WAL2 mechanism (`file:line`), the HC-Tree gap (`file:line`), estimated benefit at our
scale, and implementation risk.

### P3 — Bugs and vulnerabilities

Anything found in passing, either engine, either side of the vendor boundary. Includes
correctness bugs, races, unchecked arithmetic on sizes that matter at 6 TB / 16 TB mmap,
and anything reachable from untrusted input.

Deliverable: **`analysis/12-bugs.md`** — appended to continuously, each entry with
`file:line`, the failure scenario, severity, and whether it affects the vendored Aug-31
drop, the newer check-in, or both.

### P4 — Any optimization the hardware makes possible

The target machine is **384 CPUs and 6 TB of RAM**, which is far outside the envelope
stock SQLite is tuned for and changes which trade-offs are correct. Not limited to ideas
already present in either engine — invent where warranted. Angles to work deliberately:

- **The whole database may fit in RAM.** At 6 TB of memory against a 6 TB database, the
  page cache / mmap can plausibly hold everything. Anything that exists to economize on
  memory (cache eviction, spill thresholds, `SQLITE_DEFAULT_CACHE_SIZE=-51200` — only
  50 MB, which is almost certainly wrong here) is a candidate for retuning or removal.
- **384-way contention is a different regime.** Any single mutex, atomic counter, or
  cache line touched on every transaction becomes the bottleneck long before I/O does.
  Look for shared-state hot spots and per-CPU / sharded / striped alternatives.
  `SQLITE_MUTEX_ALERT_MILLISECONDS=20` exists because someone already hit this.
- **NUMA.** A 384-CPU host is many sockets; a single shared mapping and a single hot
  allocator have NUMA locality consequences that neither engine models at all.
- **Trading memory for concurrency.** Bigger hash tables, more aggressive precomputation,
  wider striping, per-connection arenas — all cheap at 6 TB RAM and all normally rejected
  upstream as wasteful.

Deliverable: folded into **`analysis/11-portable-optimizations.md`**, in a separate
"beyond-WAL2" section so ported-from-WAL2 and invented ideas stay distinguishable. Each
entry gets an expected-benefit rationale tied to the hardware, not a generic one.

These four priorities produce **living documents**, appended during every subsequent unit,
not written once at the end.

> Note on hardware figures: the original brief said "6 TB database"; Dan's follow-up says
> "384 CPU, 6 TB RAM machine". Both are recorded; where a claim depends on which, it says
> so. To confirm: whether the working set is ~6 TB *and* RAM is ~6 TB (i.e. fully
> cacheable), which would make P4's first bullet the single highest-leverage item.

---

## 1. Method

For every subsystem, the deliverable is a findings file that answers the same five
questions, with **exact `file:line` references on both sides**:

1. **Mechanism on the WAL2 side** — what code actually runs.
2. **Mechanism on the HC-Tree side** — what code actually runs.
3. **Behavioral differences** — what an application observes differently.
4. **Failure modes** — what breaks, under what conditions.
5. **Implications at Expensify scale** — 6 TB DB, 384 CPUs, Bedrock's usage pattern.

Rules of evidence:

- Every claim is traced to source in the `eedd80c1a9749300` checkout, or to Bedrock source
  in this repo. Claims that could not be verified in source are marked **UNVERIFIED**
  inline, and collected in the synthesis.
- Any claim that touches code identified as drifted in `00-provenance.md` says so
  explicitly, because production runs the *older* Aug-31 drop, not this check-in.
- Code is quoted sparingly — short excerpts with `file:line`, prose summary in our words.
- Docs (`doc/`, `README`, sqlite.org prose) are **corroboration only, never evidence**.
  Where the task says "verify in code, not docs", the finding must cite implementation.

---

## 2. Session breakdown

Each numbered item is one work unit producing one committed file. Units are sized so a
single session can finish one (sometimes two of the small ones). Every unit is
independently valuable — the analysis is useful even if it stops after any unit.

| # | File | Unit | Status |
|---|---|---|---|
| — | `analysis/PLAN.md` | This plan | **done** |
| 0a | `analysis/00-provenance.md` | Amalgamation regenerated and diffed; all 12 hunks classified; **zero Bedrock-only patches** | **done** |
| 0b | `analysis/01-source-map.md` | Both sides' files, line counts, roles, vtable dispatch | **done** |
| P1 | `analysis/10-conflict-investigation.md` | Dan's headline question. All seven hypotheses have verdicts | **first pass done**, write/write path outstanding |
| P2/P4 | `analysis/11-portable-optimizations.md` | A1–A4 ported, B1–B8 invented, provisionally ranked | **living** |
| P3 | `analysis/12-bugs.md` | 5 entries so far | **living** |
| — | `analysis/13-instrumentation.md` | *(unplanned, high value)* the diagnostic surface already shipping in production | **done** |
| 3 | `analysis/04-checkpointing.md` | WAL2 dual-WAL + starvation vs HC-Tree's absence of checkpointing — **verified in code** | **done** |
| 7 | `analysis/08-custom-flags.md` | All Expensify flags: what each gates, engines affected, risk | **done** |
| 1+2 | `analysis/02-write-path.md` | Write path, commit protocol and locking granularity — combined, since on HC-Tree the write path *is* the locking mechanism. Includes the isolated July `08c755b` fix | **done** |
| 4 | `analysis/05-recovery-durability.md` | Crash recovery; no fsync anywhere in HC-Tree; **bug #7** | **done** |
| 5 | `analysis/06-readers-snapshots.md` | MVCC / visibility; long-reader behaviour; GC horizon | **done** |
| 6 | `analysis/07-mmap.md` | mmap per engine; three pragmas that are no-ops on HC-Tree | **done** |
| 8 | `analysis/09-multiprocess.md` | HC-Tree single-process constraint; tooling consequences | **done** |
| 9 | `analysis/99-synthesis.md` | Decision-oriented comparison, risks, verification plan, open questions | **done** (revisit as units 1–2 land) |

**All planned units are complete.** The July `08c755b` fix was isolated after all — not by
fetching upstream tarballs (sqlite.org returned 503 throughout) but by diffing the two
vendored amalgamations out of Bedrock's own git history and classifying all 390 hunks by
source file. See `02-write-path.md` §5; it produced the Q0 that now heads both the conflict
investigation and the synthesis.

**What is left is measurement, not reading.** `99-synthesis.md` §4 lists it in four tiers,
the first of which needs no rebuild and no deploy. Two source-level areas were deliberately
not pursued because nothing suggested they mattered: page balancing/splitting under
concurrency (instrumented via the nine `balance_*` counters if it ever does), and the
FOLLOWER-mode replication paths, which are dead in Bedrock's configuration
(`09-multiprocess.md` §5.2).

**Deviation from the original ordering, and why.** Dan's four priorities arrived after
Phase 0 and reordered the work: the conflict investigation (P1) was promoted ahead of the
neutral subsystem sweep, and units 3 and 7 were done next because both fed it directly —
checkpointing because the production starvation history motivates the whole comparison, and
the flags unit because `WAL_BIGHASH` corroborates it and `NOOP_UPDATE` turned out to be a
ready-made P1 experiment. `13-instrumentation.md` was unplanned and is now the top
recommendation, because the measurements that would settle most open questions are already
being logged.

Ordering rationale: 0a/0b first because every later claim depends on knowing what code
production actually runs. Then units 1–3, which carry the most decision weight (write
path, locking, checkpointing — the subsystems behind the observed production stalls).
Units 4–8 fill in durability, visibility, and the Expensify-specific surface. Synthesis
last, once there is something to synthesize.

---

## 3. Wait points

Points where the analysis may need input rather than more reading:

- **W1 (hard stop, resolved):** sqlite.org unreachable → stop, do not simulate.
  *Resolved: reachable, tarball verified against `manifest.uuid`.*
- **W2 (soft):** If the amalgamation diff in 0a shows Bedrock-only patches to `sqlite3.c`
  that are not present upstream, that is a maintenance finding worth surfacing
  immediately rather than at synthesis — production would be running a locally-patched
  amalgamation that a re-vendor would silently revert.
- **W3 (soft):** Unit 2 wants the upstream check-in diffs for `1035b1143f` and
  `eedd80c1a9`. The tarball is a snapshot with no history. Without fossil we can fetch
  per-check-in diffs over HTTP from sqlite.org, or diff two tarballs. If neither works,
  that unit reports the *current* behavior and marks the *change* UNVERIFIED.
- **W4 (soft):** Questions that source cannot answer — design intent, roadmap, whether a
  limitation is fundamental or merely unimplemented — are not guessed. They accumulate in
  the "open questions for Dan" section of `99-synthesis.md`.

## 4. Working discipline

- Findings files are committed as they are finished; nothing of value stays only in
  context.
- If context runs low mid-unit, partial findings are written to the file with a
  `**RESUME POINT:**` note at the top saying exactly what was covered and what is next.
- The upstream checkout lives in the session scratchpad and is **not** committed to
  Bedrock. It is re-fetchable from the recorded URL + check-in hash; `00-provenance.md`
  records everything needed to reproduce it.
