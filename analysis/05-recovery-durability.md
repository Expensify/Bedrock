# 05 — Crash recovery and durability

**Unit:** 4
**Status:** COMPLETE
**Drift:** none. All code cited is identical in the vendored Aug-28 drop.

---

## Summary

**HC-Tree issues no `fsync` of any kind, anywhere, and offers no control to enable one.**
Not in the log writer, not in the page writer, not in the journal. `PRAGMA synchronous` is
a no-op on HC-Tree (`07-mmap.md` §1). This is a stronger statement than "we run with
`synchronous=0`": on WAL2 that is a *setting* that could be changed; on HC-Tree there is
nothing to change.

For a replicated cluster that rebuilds a lost node from peers, this may be an entirely
reasonable trade — but it needs to be a conscious one, because it has a specific failure
mode that replication does not cover (§4).

**Compounding it, the log-recovery parser trusts two values read straight out of the log
file with no bounds validation** — a starting offset and a record length — which on exactly
the input that a power loss produces (a torn, never-synced log file) yields out-of-bounds
reads. That is the most serious defect found in this analysis and is written up as
`12-bugs.md` #7.

---

## 1. WAL2: recovery and what `SYNCHRONOUS=0` means

WAL recovery walks frames from the start of the WAL, validating the running checksum
chain, and stops at the first frame that fails — that frame and everything after it is
discarded (`walDecodeFrame()`, `wal.c:1331`; recovery driver `walIndexRecoverOne()`,
`wal.c:1743`, which handles `*-wal` and `*-wal2` separately).

`SQLITE_DEFAULT_WAL_SYNCHRONOUS=0` sets `synchronous=OFF`, which removes the WAL fsync at
commit (`wal.c:5471`, `if( isCommit && WAL_SYNC_FLAGS(sync_flags)!=0 )`), and
`w.iSyncPoint` stays 0 (`wal.c:5405`) so the mid-write sync in `walWriteToLog()`
(`wal.c:5175-5183`) never fires either. **No fsync is issued on the WAL during normal
operation.**

The consequence for a *process* crash is nil — the page cache retains everything and
recovery sees exactly what was written. The consequence for a *machine* crash is loss of
recent transactions, which is the accepted trade.

**But Bedrock also sets `SQLITE_ENABLE_WAL2NOCKSUM`, which removes the page-body checksum
and substitutes write ordering that nothing then enforces.** So WAL2's normally clean
"truncate at the first bad frame" recovery can, after a power loss, accept a frame whose
header landed but whose body did not. Full analysis in `12-bugs.md` #5.

**Net for WAL2:** recovery is structurally sound and checksum-driven, but Bedrock's flag
combination weakens the integrity check to header-only while removing the barrier the
substitute depends on.

## 2. HC-Tree: recovery is log replay, in two stages

Recovery is staged (`hct_file.c:265-279`):

- `HCT_INIT_RECOVER1` — recover `sqlite_schema` (root page 1) and scan the page-map to
  initialise the page manager
- `HCT_INIT_RECOVER2` — recover all other tables; initialisation complete

Driven by `sqlite3HctFileStartRecovery()` / `sqlite3HctFileFinishRecovery()`
(`hct_file.c:2199`, `:2214`), with free lists rebuilt from the page-map by
`sqlite3HctFileRecoverFreelists()` (`hct_file.c:2225`). Log files are replayed record by
record through `hctRecoverOneLog()` (`hctree.c:618`), which reissues the recorded inserts
and deletes against cursors. The transaction map is reconstructed via
`sqlite3HctTMapRecoverySet()` / `...RecoveryFinish()` (`hct_tmap.c:634`, `:711`).

Because visibility is carried per-row as a TID resolved through the transaction map, a
partially-applied transaction is not "rolled back" so much as never made visible — its TID
never reaches `COMMITTED`. That is a genuinely clean design.

## 3. HC-Tree never syncs — verified exhaustively

A search across every HC-Tree source file for `fsync`, `fdatasync`, `msync`, `xSync`,
`OsSync`, `MS_SYNC`, `O_SYNC` and `O_DSYNC` returns **only virtual-table module slots set
to zero** (`/* xSync */ 0,` in `hct_database.c:9888`, `:9915`, `:9942`, `hct_file.c:2973`,
`hct_pman.c:1067`, `hct_stats.c:212`, `hct_tmap.c:972`) — none of which is a file sync.

All writes are plain, unsynced I/O:

- log records: `write()` (`hct_log.c:141`, `:214`, `:233`)
- database pages: `pwrite()` (`hct_file.c:427`) and the header (`hct_file.c:1154`)
- everything else: stores into `mmap(..., MAP_SHARED, ...)` regions (`hct_file.c:713`)

And there is no way to ask for durability: `sqlite3HctBtreeSetPagerFlags()` — the path
`PRAGMA synchronous` would take — is `return SQLITE_OK;` with the upstream comment
`/* HCT - does this need fixing? */`.

**Also of note: HC-Tree bypasses the SQLite VFS entirely for its own files.** It uses raw
`open`/`read`/`write`/`pwrite`/`mmap` rather than `sqlite3OsWrite()` and friends. Any VFS
shim — instrumentation, encryption, a test harness that injects I/O errors — applies to
WAL2 databases and **not** to HC-Tree ones. Worth knowing before anyone builds tooling that
assumes VFS interception works.

## 4. What this means for Bedrock — the scenario that matters

For a single node, "no fsync" is survivable in the way Bedrock already accepts: a crashed
node is rebuilt from peers, and `synchronous=0` on WAL2 already concedes recent commits on
power loss.

**The scenario replication does not cover is a correlated power event** — a rack, an
availability zone, or a datacenter losing power at once. Every node then recovers from
state that was never forced to disk, using a recovery path that (§5) does not validate its
input. There is no clean peer to rebuild from because every peer took the same hit
simultaneously.

This is not an argument against HC-Tree. It is an argument for knowing which of these is
true:

- we rely on the hardware (battery-backed cache, PLP SSDs) to make un-synced writes durable
  in practice; or
- we rely on geographic separation so a correlated power event cannot hit a quorum; or
- we accept that a correlated power event means restoring from backup.

Any of those is a defensible position. **Not having picked one is not.**

**Question for Dan:** which is it? And relatedly — how do we take backups of a live HC-Tree
node, given no second process may open the file (`09-multiprocess.md` §5.3)?

## 5. The recovery parser does not validate its input

Two values are read directly out of the log file and used without bounds checking.

**(a) The starting offset.** `hctLogReaderOpen()` (`hctree.c:560`) reads the whole file into
`aFile`, then takes the initial read position *from the file's own bytes*:

```c
      memcpy(&pReader->iTid, pReader->aFile, sizeof(i64));
      memcpy(&pReader->iFile, &pReader->aFile[8], sizeof(int));
```

`iFile` is never checked against `[0, nFile]`. `hctLogReaderNext()` guards only the upper
end, and does so in signed arithmetic (`hctree.c:510`):

```c
  if( (pReader->iFile + 12)>pReader->nFile ){
    pReader->bEof = 1;
  }else{
    memcpy(&iRoot, &pReader->aFile[pReader->iFile], sizeof(iRoot));
```

A **negative** `iFile` passes that test and indexes before the start of the buffer.

**(b) The record length.** Still in `hctLogReaderNext()` (`hctree.c:526-534`):

```c
      }else{
        pReader->nKey = nByte;
        pReader->aKey = &pReader->aFile[pReader->iFile];
        pReader->iFile += pReader->nKey;
```

`nByte` is a `u32` taken verbatim from the file. It is not compared against `nFile` or the
remaining bytes. The resulting `(aKey, nKey)` pair — which may describe a region far past
the end of the allocation — is handed to `hctRecoverOneLog()` (`hctree.c:618`) and used as
a key.

The allocation carries 8 bytes of slack (`sqlite3HctMallocRc(&rc, pReader->nFile + 8)`),
which suggests some awareness of small overruns, but it does not bound an arbitrary
`nByte`.

**(c) A secondary issue in the same function:** `pReader->nFile = (int)sStat.st_size;` casts
an `off_t` to `int` with no range check. A log file at or above 2 GiB yields a negative
`nFile`. Log chunks default to 16 KiB (`HCT_DEFAULT_SZLOGCHUNK`, `hctInt.h:57`) so this is
unlikely to be reachable today, but it is unguarded.

**Why this matters more than it would elsewhere:** this is the *crash recovery* path, and
§3 establishes that nothing is ever synced. A torn or partially-written log file after a
power loss is not a hypothetical adversarial input — **it is the expected input to this
code path.** The two facts compound: the design guarantees the parser will sometimes be fed
damaged files, and the parser does not defend against them.

Recorded as **`12-bugs.md` #7**. Not remotely triggerable — it requires local file state —
so it is a robustness and crash-recovery-reliability problem rather than a remote
vulnerability. But the practical failure is a node that cannot restart, or restarts having
read adjacent heap as a key.

**Recommended:** raise with Dan Kennedy. The fixes are small and local — validate `iFile`
against `[0, nFile]` on open, validate `nByte` against `nFile - iFile` before use, and use
`i64` for the file size. A per-record checksum would be a larger change but would also let
recovery *detect* a torn tail rather than parse into it; the log format currently has no
checksum or magic (only an `iRoot == 0` terminator, `hctree.c:522`).

## 6. Comparison

| | WAL2 (as Bedrock builds it) | HC-Tree |
|---|---|---|
| Recovery method | replay WAL frames, stop at checksum failure | replay log files, rebuild tmap and freelists |
| Integrity check on recovery | checksum chain — but **header-only** under `WAL2NOCKSUM` | **none** — no checksum or magic in the log format |
| Input validation on recovery | frame header validated by checksum | **offset and length unvalidated** (§5) |
| fsync during normal operation | none (`synchronous=0`) | **none, and not configurable** |
| Can durability be turned on? | yes — change `synchronous` | **no** — `SetPagerFlags` is a no-op |
| Process-crash safe | yes | yes |
| Machine-crash safe | no (accepted trade) | no, and no path to it |
| Uses the SQLite VFS | yes | **no** — raw syscalls |

**Assessment.** HC-Tree's recovery *design* is sound and arguably cleaner than WAL2's —
per-row TID visibility means a partial transaction is simply never visible, with no undo
needed. But its recovery *implementation* is less defensive than WAL2's on exactly the
input it is most likely to meet, and it removes the durability control that WAL2 at least
still exposes. For a decision at 6 TB, §4's question is the one to settle first.

## 7. Open questions

1. Which durability position are we actually taking (§4) — hardware-backed, geographically
   separated, or restore-from-backup?
2. How are backups of a live HC-Tree node taken today?
3. For Dan Kennedy: is the absence of any `fsync` in HC-Tree deliberate and permanent, or
   pending? `sqlite3HctBtreeSetPagerFlags()`'s own comment asks the same question.
4. For Dan Kennedy: the unvalidated `iFile` and `nByte` in `hctLogReaderOpen` /
   `hctLogReaderNext` (§5).
