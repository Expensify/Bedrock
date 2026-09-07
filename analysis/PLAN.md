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

| # | File | Unit | Notes |
|---|---|---|---|
| — | `analysis/PLAN.md` | This plan | done |
| 0a | `analysis/00-provenance.md` | Generate amalgamation from checkout, diff vs `libstuff/sqlite3.c`, classify every hunk as (a) upstream-newer, (b) Bedrock-only patch, (c) generation noise | Gate for everything else |
| 0b | `analysis/01-source-map.md` | Enumerate both sides' files, line counts, role paragraphs, dispatch mechanism | Cheap, high leverage |
| 1 | `analysis/02-write-path.md` | Write path & commit protocol: BEGIN CONCURRENT validation, transaction lifecycle | Largest unit; may split |
| 2 | `analysis/03-locking.md` | Locking granularity: page vs row; July 2026 row-lock fix (Bedrock `08c755b`); `OP_IdxDelete` change in `1035b1143f` / `eedd80c1a9` | Needs fossil check-in diffs |
| 3 | `analysis/04-checkpointing.md` | WAL2 dual-WAL scheme + starvation risk vs HC-Tree's claimed elimination of checkpointing — **verified in code** | Directly tied to the prod incident history |
| 4 | `analysis/05-recovery-durability.md` | Crash recovery paths; what `SYNCHRONOUS=0` means on each side | |
| 5 | `analysis/06-readers-snapshots.md` | MVCC / visibility; long-reader behavior | |
| 6 | `analysis/07-mmap.md` | mmap + `SQLITE_SHARED_MAPPING` at 16 TB map size, per engine | Small unit |
| 7 | `analysis/08-custom-flags.md` | `WAL_BIGHASH`, `WAL2NOCKSUM`, `MUTEX_ALERT_MILLISECONDS`, `NOOP_UPDATE`, `PERCENTILE`: code gated, engines affected, risk | Mechanical; do by grep over checkout |
| 8 | `analysis/09-multiprocess.md` | HC-Tree same-process constraint vs WAL2 multi-process; consequences for Bedrock architecture and out-of-process tooling | |
| 9 | `analysis/99-synthesis.md` | Decision-oriented comparison, risks at scale, open questions for Dan, verification/benchmark list | Last |

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
