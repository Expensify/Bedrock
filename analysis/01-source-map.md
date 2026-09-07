# 01 — Source map: the two engines side by side

**Unit:** Phase 0b
**Status:** COMPLETE
**Source:** `hctree-bedrock-lcd-ex @ eedd80c1a9749300`; line counts from that checkout.
**Drift:** none of the files below differ from the vendored Aug-28 drop except
`hctree.c`, `btwrapper.c`, `vdbe.c` and two headers, all confined to `IdxDelete`
(see `00-provenance.md`).

---

## How one binary runs two engines

There is no `#ifdef` separating the engines. **Both are compiled in, always**, and every
`Btree`/`BtCursor` operation goes through a **vtable of function pointers** chosen when
the database is opened.

The mechanism, in three pieces:

1. **`src/btreeModules.h`** (162 lines) — declares two parallel API surfaces over the same
   opaque `Btree*`/`BtCursor*` types: `sqlite3HctBtree*` (HC-Tree) and `sqlite3StockBtree*`
   (classic B-tree/WAL2). Every entry point exists in both families with identical
   signatures, e.g.
   `sqlite3HctBtreeInsert(BtCursor*, const BtreePayload*, int, int)` vs
   `sqlite3StockBtreeInsert(...)`.

2. **`src/btwrapper.c`** (719 lines) — defines the method-table struct (the
   `xBtree*` / `xBtreeCursor*` slots) and the thin dispatch thunks the rest of SQLite
   actually calls. Every `sqlite3BtreeXxx()` in the codebase is one line here:

   ```c
   SQLITE_PRIVATE int sqlite3BtreeDelete(BtCursor *p, u8 a){
     return p->pMethods->xBtreeDelete(p, a);
   }
   ```

   It also holds `sqlite3StockBtree*` shims for the handful of APIs where the stock engine
   needs adapting rather than direct forwarding — including
   `sqlite3StockBtreeIdxDelete()`, which is `assert(0)`-unreachable because stock builds
   take the non-Hct branch in `OP_IdxDelete` instead.

3. **`tool/hct_mkbtreewrapper.tcl`** — generates `btwrapper.c` and `btreeModules.h` from
   declaration lists in the script itself, so the two families cannot drift apart by
   accident. Notably it carries an explicit `unused_apis` list of stock-engine APIs with
   **no HC-Tree equivalent**:

   ```tcl
   set unused_apis {
     int sqlite3BtreeLockTable(Btree*, int, u8);
     int sqlite3BtreeSharable(Btree*);
     int sqlite3BtreeConnectionCount(Btree*);
     # Probably need this one...
     int sqlite3BtreeCheckpoint(Btree*, int, int*, int*);
   }
   ```

   This list is evidence for later units: `BtreeSharable` / `BtreeConnectionCount` absent
   → the shared-cache/multi-connection model differs (unit 8).

   **Correction (made while reading unit 3):** the `BtreeCheckpoint` entry in this list is
   now stale. `sqlite3HctBtreeCheckpoint()` *does* exist (`hctree.c:3753`) and *is* wired
   into the HC-Tree method table (`btwrapper.c:491`). But its entire body is
   `return SQLITE_OK;` — a no-op stub. So the conclusion "HC-Tree has no checkpointing"
   still holds and is now established from the implementation rather than from the
   generator's TODO list; see `04-checkpointing.md`.

**Runtime selection** happens above this layer, in Bedrock:
`-newDBsUseHctree` / URI `hctree=1` on first open, header sniff for
`"Hctree database version"` thereafter (`SQLite::validateDBFormat`), and
`PRAGMA journal_mode = wal2` for everything else
(`sqlitecluster/SQLite.cpp:284`, guarded by `if (!hctree)`).

**Implication worth stating early:** because dispatch is indirect through a function
pointer on every btree call, HC-Tree and WAL2 both pay an unconditional
non-inlinable-call cost that upstream stock SQLite does not. On hot cursor paths
(`Next`, `PayloadFetch`, `IntegerKey`) this is a real per-row cost. Whether it is
*measurable* against everything else is an open question — logged as a P4 candidate in
`11-portable-optimizations.md`, not asserted here.

---

## WAL2 side (classic pager + two-WAL journal)

| File | Lines | Role |
|---|---:|---|
| `src/btree.c` | 13,977 | The classic B-tree. Page-oriented: cells, cell arrays, overflow chains, balancing/splitting, freelist, pointer maps. Also holds the **BEGIN CONCURRENT conflict-detection logic** — `btree.c:1358` onward compares a transaction's read/write page sets against pages committed since its snapshot and returns `SQLITE_BUSY_SNAPSHOT` (`btree.c:1401`, `btree.c:1472`, `btree.c:2218`). This is the WAL2 half of the P1 comparison. |
| `src/os_unix.c` | 8,770 | VFS: file handles, `fcntl`/POSIX advisory locking, shared-memory (`wal-index`) via `mmap`, and the **`SQLITE_SHARED_MAPPING`** modification (unit 6). Multi-process coordination lives here. |
| `src/pager.c` | 8,119 | Between btree and VFS. Page cache, journal/WAL mode selection, savepoints, transaction state machine, `sqlite3PagerCommitPhaseOne/Two`. `pager.c:6620` documents the CONCURRENT commit path returning `SQLITE_BUSY_SNAPSHOT`. |
| `src/wal.c` | 5,978 | The write-ahead log, in its two-WAL (WAL2) form. Frame append, the wal-index hash lookup, reader marks, and checkpointing. Home of **`SQLITE_ENABLE_WAL_BIGHASH`** and **`SQLITE_ENABLE_WAL2NOCKSUM`** (unit 7) and of the snapshot-conflict returns at `wal.c:4558-4737`. |
| `src/btreeInt.h` | 984 | Internal B-tree structures: `BtShared`, `MemPage`, `BtCursor`, `CellInfo`. Defines where page-level granularity is baked in. |
| `src/wal.h` | 185 | WAL interface seen by `pager.c`. |
| **total** | **38,013** | |

## HC-Tree side

| File | Lines | Role |
|---|---:|---|
| `src/hct_database.c` | 9,978 | The core. Transaction lifecycle, snapshot/visibility, and **commit-time validation** — the HC-Tree half of P1. Defines `HCT_SQLITE_BUSY` (`hct_database.c:695-700`), which logs and returns `SQLITE_BUSY_SNAPSHOT`, and sets `pDb->rcCommit = SQLITE_BUSY_SNAPSHOT` at `hct_database.c:5938`. Also carries a `zCommitMsg` diagnostic (`hct_database.c:8364`) that may be directly useful for instrumenting the conflict question. |
| `src/hctree.c` | 3,984 | The `sqlite3HctBtree*` API surface — the HC-Tree implementation of the btree vtable. Translates cursor/insert/delete calls into tree and database operations. Contains the schema-op table `aSchemaOp[]` and the changed `sqlite3HctBtreeIdxDelete()`. |
| `src/hct_file.c` | 2,993 | File layout and mapping. The database is grown in **chunks** of `HCT_DEFAULT_PAGEPERCHUNK` pages (2 MiB at 4 KiB pages) and mapped `HCT_MMAP_QUANTA` chunks at a time — an explicit workaround for Linux `vm.max_map_count` (65530 default), since mapping 2 MiB at a time would exhaust the limit. Directly relevant to unit 6 and to 6 TB sizing: the header comment computes the maximum mappable database from these constants. |
| `src/hct_tree.c` | 1,430 | The in-memory per-transaction tree holding a writer's uncommitted changes, keyed by index key. `pReseek` handling: a cursor disrupted by a write is re-seeked rather than invalidated. This structure is where a transaction's **write set** materializes, so it is the first place to look for P1 hypotheses H1/H2. |
| `src/hct_pman.c` | 1,103 | Page manager — "baskets" (pagesets) of free page ids. The allocator. A shared allocator touched by every writing transaction is a classic contention point at 384 CPUs (P4) and a candidate for P1 hypothesis H3. |
| `src/hct_tmap.c` | 984 | Transaction map. Carries the locking notes (`iMinTid`, `iMinCid`) that define which transactions and snapshots are still live — i.e. the garbage-collection horizon. Central to unit 5 (readers/snapshots) and to long-reader behaviour. |
| `src/hct_journal.c` | 966 | Shared journal object, one per database across all connections, obtained via `sqlite3HctFileGetJrnlPtr()`. Has an `eMode` of `SQLITE_HCT_NORMAL` / `FOLLOWER` / `LEADER` — HC-Tree has its own replication-shaped concept, which overlaps conceptually with Bedrock's. Returns `SQLITE_BUSY_SNAPSHOT` at `hct_journal.c:732` and, notably, `"snapshot too old"` at `hct_journal.c:922`. |
| `src/hct_log.c` | 451 | Per-connection log files. Documents FOLLOWER-mode ordering: a log file cannot be `unlink()`ed until all transactions with smaller CID have reached the journal. |
| `src/hct_journalhash.c` | 323 | Hash structure over journal entries. **A hash used in conflict-relevant lookup is exactly the shape of P1 hypothesis H5 (aliasing → false conflicts)** — to be read closely in unit 1. |
| `src/hct_record.c` | 217 | Record serialization (`sqlite3HctSerializeRecord`) — turns an `UnpackedRecord` into the byte form used as the tree key. The `memcmp` in the new `IdxDelete` skip compares these bytes. |
| `src/hct_stats.c` | 226 | The `hctstats` eponymous virtual table. **Likely the cheapest instrumentation route for P1** — needs enumerating in unit 1. |
| `src/btwrapper.c` | 719 | Generated dispatch (described above). |
| headers | ~1,000 | `hctInt.h` (256), `hctFileInt.h` (208), `hctTMapInt.h` (142), `hctTreeInt.h` (111), `hctPManInt.h` (103), `hctJrnlInt.h` (83), `hctLogInt.h` (70), `btreeModules.h` (162). |
| **total** | **24,509** | |

---

## Structural observations to carry forward

1. **The engines are not symmetric in maturity of surface.** WAL2's conflict logic sits in
   `btree.c` beside the page code it protects; HC-Tree's sits in `hct_database.c`, a file
   two-and-a-half times the size of `hctree.c` that also owns snapshots, commit, and
   validation. The concentration suggests reading `hct_database.c` first in unit 1.

2. **Three distinct HC-Tree structures could each be the real conflict unit**: the
   per-transaction `hct_tree.c` write tree, the `hct_journalhash.c` hash, and the
   `hct_pman.c` free-page baskets. P1 cannot be answered without establishing which of
   these participates in validation. That is the first task of unit 1.

3. **HC-Tree has no checkpoint API at all** — evidenced by the generator's `unused_apis`
   list, not by documentation. Unit 3 can start from that fact rather than establishing it.

4. **`hct_journal.c` has LEADER/FOLLOWER modes.** Bedrock also has leader/follower.
   Whether these interact, duplicate, or conflict is an open question worth putting to Dan
   — it is not answerable from the SQLite source alone.

---

## Next unit

`10-conflict-investigation.md` (P1) — read `hct_database.c` commit/validation path and
`hct_tree.c` write-set construction line by line, then test hypotheses H1–H7 against
`btree.c:1358-1472`'s page-set comparison on the WAL2 side.
