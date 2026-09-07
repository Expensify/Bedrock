# 08 — The Expensify-specific compile flags: what each one actually gates

**Unit:** 7
**Status:** COMPLETE
**Drift:** none. Every flag behaves identically in the vendored Aug-28 drop and check-in
`eedd80c1a9749300`.
**Scope note:** `AMALGAMATION_FLAGS` (`Makefile:18`) mixes three different kinds of option
— amalgamation-*generation* options, real compile-time options, and one flag that does
nothing at all. The name is misleading; see `12-bugs.md` #2.

---

## Summary table

| Flag | Engine affected | What it gates | Risk |
|---|---|---|---|
| `SQLITE_ENABLE_WAL_BIGHASH` | **WAL2 only** | wal-index hash geometry: `u16`→`u32` slots, 4K→128K frames per hash segment | **Changes the wal-index format.** Necessary; keep. |
| `SQLITE_ENABLE_WAL2NOCKSUM` | **WAL2 only** | drops the page-body checksum; inverts frame write order to compensate | **Changes the WAL format.** Interacts badly with `synchronous=0` — `12-bugs.md` #5 |
| `SQLITE_MUTEX_ALERT_MILLISECONDS=20` | **neither** | **nothing — the macro is never tested** | Dead flag; lost diagnostic — `12-bugs.md` #1 |
| `SQLITE_ENABLE_NOOP_UPDATE` | both | `PRAGMA noop_update`, which rewrites every `SET` expression to `+column` | Low. A real Bedrock feature (mock requests) |
| `SQLITE_SHARED_MAPPING` | both | moves the mmap region from per-file to per-**inode**, shared across connections | Low-medium; makes `mmap_size` effectively immutable after first map |
| `SQLITE_ENABLE_PERCENTILE` | both | adds `percentile()`, `median()`, `percentile_cont/disc()` SQL functions | Negligible; purely additive |
| `SQLITE_DEFAULT_WAL_SYNCHRONOUS=0` | WAL2 | removes **all** WAL fsyncs | High in combination with `WAL2NOCKSUM` |
| `SQLITE_MAX_MMAP_SIZE=17592186044416` (16 TiB) | both | raises the mmap ceiling | See unit 6 |
| `SQLITE_DEFAULT_CACHE_SIZE=-51200` (50 MiB) | both | default page cache — **and it is the effective one**, see `12-bugs.md` #3 | Medium (performance) |

---

## 1. `SQLITE_ENABLE_WAL_BIGHASH` — WAL2 only

**What it changes** (`wal.c:855-859`, `wal.c:900-903`):

```c
#ifdef SQLITE_ENABLE_WAL_BIGHASH
 typedef u32 ht_slot;
#else
 typedef u16 ht_slot;
#endif
...
#ifdef SQLITE_ENABLE_WAL_BIGHASH
# define HASHTABLE_BITS       17                   /* 128K frames per hash */
#else
# define HASHTABLE_BITS       12                   /* 4K frames per hash */
#endif
```

`WALINDEX_PGSZ` is derived from both (`wal.c:917`), so the wal-index page grows from
~32 KB to ~1.5 MB. `WAL_VERSION2` becomes `3021001` instead of `3021000` (`wal.c:463`).

**Why it exists here.** A `u16` slot cannot hold a frame index above 65,535. Choosing this
flag is only necessary if WAL files actually reach that length — which is exactly the
WAL2 growth failure mode documented in `04-checkpointing.md` §1. Beyond raising the
ceiling, it cuts frame-lookup work: lookups walk hash segments, so a 500,000-frame WAL
needs ~125 segment searches at 4K frames/segment versus ~4 at 128K. The 48× larger
wal-index is irrelevant at 6 TB RAM.

**Assessment: correct flag for the problem, keep it.** It is a direct, well-targeted
mitigation and the memory cost is free on this hardware.

**Risk:** it is a **format change**, stated in the source (`wal.c:897`): "Changing any of
these constants will alter the wal-index format and create incompatibilities." Any tool
built without the flag cannot share a database with Bedrock. See §7.

**Not applicable to HC-Tree** — there is no WAL and no wal-index.

## 2. `SQLITE_ENABLE_WAL2NOCKSUM` — WAL2 only

**What it changes.** `isNocksum(pWal)` becomes `isWalMode2(pWal)` (`wal.c:476`) — true for
every wal2 database. Two effects:

1. **The page body is no longer checksummed.** The running checksum covers only the first
   8 bytes of the frame header, in both `walEncodeFrame()` (`wal.c:1275`) and
   `walDecodeFrame()` (`wal.c:1331`).
2. **Frame write order is inverted** (`wal.c:5217-5228`): data first, header second, so a
   header's presence is meant to imply its data landed.

`WAL_VERSION2` becomes `3048000`, or `3048001` combined with `WAL_BIGHASH`
(`wal.c:469-475`).

**Why it exists:** checksumming every page body on every frame write is real CPU on a
write-heavy 384-CPU system. Skipping it is a meaningful saving.

**Risk — this is the one to look at.** The write-ordering substitute is only sound if the
ordering reaches durable storage, and Bedrock also sets
`SQLITE_DEFAULT_WAL_SYNCHRONOUS=0`, which removes every WAL fsync
(`wal.c:5405`, `wal.c:5471`). Nothing then orders the two writes. Full analysis and
qualifications in **`12-bugs.md` #5** — the exposure is machine-crash/power-loss only, not
process crash, and Bedrock's journal hash chain is a plausible detection layer above it.

**Portability note for P2:** the *idea* is engine-agnostic. HC-Tree does its own
integrity work; whether it spends CPU on checksums that could be similarly traded is an
open question for unit 4.

## 3. `SQLITE_MUTEX_ALERT_MILLISECONDS=20` — affects nothing

The macro is **never tested anywhere**: zero matches across the entire upstream tree, zero
in `libstuff/sqlite3.c`, and the only occurrence in Bedrock is the `Makefile` line defining
it. Full write-up in **`12-bugs.md` #1**.

Worth restating here because of what it implies: someone once cared enough about mutex hold
times to add alerting — the precise diagnostic that every 384-CPU hypothesis in
`11-portable-optimizations.md` Part B would want — and it is gone. Since
`00-provenance.md` establishes the amalgamation carries no local patches at all, if such a
patch ever existed it did not survive a re-vendor.

## 4. `SQLITE_ENABLE_NOOP_UPDATE` — both engines, and it is a real feature

**What it gates** (`update.c:468-477`): when `db->flags & SQLITE_NoopUpdate` is set, every
expression in the `SET` list is discarded and replaced with `+<column>`:

```c
    if( db->flags & SQLITE_NoopUpdate ){
      Token x;
      sqlite3ExprDelete(db, pChanges->a[i].pExpr);
      x.z = pChanges->a[i].zEName;
      x.n = sqlite3Strlen30(x.z);
      pChanges->a[i].pExpr =
         sqlite3PExpr(pParse, TK_UPLUS, sqlite3ExprAlloc(db, TK_ID, &x, 0), 0);
    }
```

So `UPDATE t SET x = <anything>` becomes `UPDATE t SET x = +x`. The statement runs the full
update machinery — parsing, constraint checks, index maintenance, journalling — while
writing each column its existing value. The flag is exposed as `PRAGMA noop_update`
(`tool/mkpragmatab.tcl:118`, `sqliteInt.h:1951`).

**Bedrock uses this in production.** `SQLite::setUpdateNoopMode()`
(`sqlitecluster/SQLite.cpp:1601`) issues the pragma, and it is driven by
`BedrockCore.cpp:283`:

```cpp
_db.setUpdateNoopMode(command->request.isSet("mockRequest"));
```

reset to `false` at `BedrockCore.cpp:360`. So this is **mock-request / dry-run support**: a
command can be executed end-to-end against live data — including its HTTPS calls and
validation logic — without mutating anything. `SQLite.cpp:770` and `:787` raise
`SALERT("Non-idempotent write in _noopUpdateMode")` when a write that is not idempotent
slips through. The pragma is also appended to `_uncommittedQuery` so it replicates
(`SQLite.cpp:1614`).

**Assessment: low risk, correctly used.** Note it is a *code-generation* change, so a mock
request and a real request do not execute identical VDBE programs — a mock request cannot
be relied on to reproduce a real one's conflict behaviour.

**Connection to P1:** `UPDATE t SET x=x` is exactly the pathological shape identified in
`11-portable-optimizations.md` A2 — an update that writes identical values yet still
deletes and reinserts index entries, manufacturing write-set entries out of a semantic
no-op. **`PRAGMA noop_update` is therefore a ready-made experiment for A2:** run a
representative write workload with it on, and any conflicts still observed are pure
write-amplification artifacts, since no value changes. That is a clean way to size the A2
prize without writing any code.

## 5. `SQLITE_SHARED_MAPPING` — both engines

**What it changes.** The memory-mapped region moves from the per-file `unixFile` onto the
per-inode `unixInodeInfo` (`os_unix.c:1352`):

```c
#ifdef SQLITE_SHARED_MAPPING
  sqlite3_int64 nSharedMapping;   /* Size of mapped region in bytes */
  void *pSharedMapping;           /* Memory mapped region */
#endif
```

`unixInodeInfo` is shared by all file descriptors on the same inode within the process, so
**every Bedrock connection to the same database shares one mapping** instead of each
creating its own. Consequences in code:

- `unixUnmapfile()` early-returns when `pFd->pInode` is set (`os_unix.c:6311`) — individual
  connections never unmap.
- The mapping is released only when the inode refcount reaches zero
  (`releaseInodeInfo()`, `os_unix.c:1512-1518`).
- Inode tracking is extended to `nolockIoMethods` (`os_unix.c:6311`), which normally does
  not need it.
- `nolockClose()` gains a `releaseInodeInfo()` call (`os_unix.c:2432`).

**Why it exists:** with a 16 TiB `SQLITE_MAX_MMAP_SIZE` and a connection pool sized to the
core count, per-connection mappings would multiply address-space consumption and `mmap()`
churn by the pool size. Sharing is clearly the right call at this scale.

**Behavioural consequence worth knowing** (`os_unix.c:4274-4279`): the
`SQLITE_FCNTL_MMAP_SIZE` handler still records the new `mmapSizeMax`, but the actual
remap is skipped when `pFile->pInode` is set:

```c
#ifdef SQLITE_SHARED_MAPPING
        if( pFile->pInode==0 )
#endif
        if( pFile->mmapSize>0 ){
          unixUnmapfile(pFile);
          rc = unixMapfile(pFile, -1);
        }
```

So **the first connection to map the file fixes the mapping for the whole process**, and a
later `PRAGMA mmap_size` cannot change it. Bedrock issues the same
`PRAGMA mmap_size` on every connection (`sqlitecluster/SQLite.cpp:288`), so this is benign
today — but runtime mmap-size tuning will silently not work, and anyone testing such a
change needs to know that. Carried into unit 6.

## 6. `SQLITE_ENABLE_PERCENTILE` — both engines, negligible

Folds the percentile extension into the core (`func.c:2657-3155`, registered at
`func.c:3443-3456`, with aggregate support at `vdbeaux.c:5771`). Adds `percentile(Y,P)`,
`percentile_cont()`, `percentile_disc()` and `median()`.

Purely additive SQL functions, no storage or concurrency implications, no format change.
**Risk: negligible.** Included here only for completeness.

---

## 7. Cross-cutting risk: three of these change on-disk or shared-memory formats

`WAL_BIGHASH` and `WAL2NOCKSUM` both alter `WAL_VERSION2` (`wal.c:461-479`), and
`WAL_BIGHASH` alters the wal-index layout. Bedrock's WAL2 databases therefore carry
`WAL_VERSION2 == 3048001`, a combination produced only by a build with **both** flags.

**Consequence:** a stock `sqlite3` shell, a backup tool, or any out-of-process reader built
without these exact flags **cannot correctly attach to a live Bedrock WAL2 database**. This
is a real operational constraint that is not recorded anywhere in the repo, and it compounds
the HC-Tree same-process limitation examined in unit 8.

**Recommended:** record the required flag set next to `libstuff/sqlite3.c` alongside the
regeneration recipe (`12-bugs.md` #2), so anyone building a companion tool knows what to
match.

---

## 8. What this unit contributes to the four priorities

- **P1 (conflicts):** `PRAGMA noop_update` is a ready-made experiment for sizing the A2
  no-op-index-rewrite prize (§4) with no code changes.
- **P2 (port to HC-Tree):** `WAL2NOCKSUM`'s CPU-for-integrity trade is engine-agnostic in
  principle; whether HC-Tree has an equivalent cost worth trading is open (unit 4).
- **P3 (bugs):** three entries — `12-bugs.md` #1 (dead flag), #5 (`NOCKSUM` +
  `synchronous=0`), and the undocumented format-compatibility constraint in §7.
- **P4 (hardware):** `WAL_BIGHASH`'s memory cost is free here and its benefit scales with
  WAL length; `SHARED_MAPPING` is well-suited to a large pool; `SQLITE_DEFAULT_CACHE_SIZE`
  is the outlier that looks wrong for the hardware (`12-bugs.md` #3).
