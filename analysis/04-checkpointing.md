# 04 — Checkpointing: WAL2's dual-WAL scheme vs HC-Tree's absence of one

**Unit:** 3
**Status:** COMPLETE for the main question; HC-Tree space reclamation partially covered
**Drift:** none. Every line cited here is identical in the vendored Aug-28 drop.
**Method note:** the task asked to verify HC-Tree's elimination of checkpointing *in code,
not docs*. That is done below — the conclusion rests on the body of
`sqlite3HctBtreeCheckpoint()`, not on any prose claim.

---

## Summary

- **WAL2 does not eliminate checkpoint starvation. It converts it from a stall into
  unbounded WAL growth.** When a writer cannot switch WAL files because a reader still
  holds the other one, `walRestartLog()` swallows the `SQLITE_BUSY` and keeps appending to
  the current file (`wal.c:5104-5106`). Writers never block; the WAL just grows.
- **Expensify's `SQLITE_ENABLE_WAL_BIGHASH` is direct evidence that this happened.**
  Without it the wal-index slot type is `u16`, which cannot address beyond 65,535 frames.
  Enabling it is only necessary if WAL files were genuinely growing past that.
- **HC-Tree has no checkpointing at all**, confirmed from the implementation:
  `sqlite3HctBtreeCheckpoint()`'s entire body is `return SQLITE_OK;` (`hctree.c:3753`).
  It is wired into the method table (`btwrapper.c:491`), so calls succeed and do nothing.
- **Bedrock's checkpoint thread is therefore inert on HC-Tree**, and harmlessly so — it is
  gated on a counter only the WAL hook ever sets.
- HC-Tree replaces checkpointing with per-connection log files plus page reclamation, but
  **both `unlink()` calls for those log files are disabled in the source**
  (`hct_log.c:268`, `hct_log.c:444`), so they are only cleaned at process start.

---

## 1. WAL2's mechanism, and where starvation re-enters

### The design (`wal.c:252-345`)

The header comment states the problem WAL2 solves plainly: in legacy WAL mode a writer may
append to the WAL while a checkpoint is in progress, so "in a deployment that features a
high volume of write traffic, this may mean that the wal file is never completely
checkpointed. And so grows indefinitely." Forcing completion with
`PRAGMA wal_checkpoint=RESTART` requires waiting on all readers and blocking all writers —
"in a system with long running readers, such pauses may be for a non-trivial amount of
time."

WAL2's answer: two files, `<db>-wal` and `<db>-wal2`. Writers append to the "current" one;
once it exceeds a threshold they switch to the other, and a checkpointer can then
checkpoint the now-idle one. There is only one kind of checkpoint (no truncate/restart
variants) and it always does a whole file.

### The switch condition (`wal.c:5044`, `walRestartLog`)

Switching from file `iApp` to `!iApp` requires all three:

> (a) wal file `iApp` contains ≥ `nWalSize` frames
> (b) this client is not reading from wal file `!iApp`
> (c) no other client is reading from wal file `!iApp`

Conditions (b) and (c) are enforced by `wal2RestartOk()` (`wal.c:5010`), which takes an
**exclusive** lock over the three read-mark slots for the other file:

```c
int eLock = 1 + (iApp==0);
return walLockExclusive(pWal, WAL_READ_LOCK(eLock), 3);
```

### The starvation path — writers do not block, the WAL grows

```c
      rc = wal2RestartOk(pWal, iApp);
      if( rc==SQLITE_OK ){
        ... switch to the other wal file ...
      }else if( rc==SQLITE_BUSY ){
        rc = SQLITE_OK;                    /* wal.c:5105 */
      }
```

**If any reader still holds the other WAL file, the switch is abandoned and the writer
simply continues appending to the current file.** The `SQLITE_BUSY` is converted to
`SQLITE_OK` and never surfaces. There is no bound on how far past `nWalSize` the current
file may grow.

So WAL2's improvement over legacy WAL is real but narrower than "solves starvation": it
removes the *writer stall* that `wal_checkpoint=RESTART` would cause, and replaces it with
**silent unbounded growth of one WAL file** under exactly the condition legacy WAL also
struggled with — a long-lived reader. At 384 CPUs running `BEGIN CONCURRENT`, the
probability that *no* client is reading the other file is the thing that has to hold, and
it gets harder to satisfy as concurrency rises.

### The threshold is small, and Bedrock does not tune it

```c
#define WAL_DEFAULT_WALSIZE 1000        /* wal.c:534 */
```

`walRestartLog()` uses `WAL_DEFAULT_WALSIZE` unless `pWal->mxWalSize > 0`, which is set
from `PRAGMA journal_size_limit` (`wal.c:5049-5053`). **Bedrock never issues
`journal_size_limit`** — there is no such string in `sqlitecluster/`. So the switch
threshold is 1000 frames, roughly 4 MB at a 4 KiB page size. Writers therefore attempt a
file switch very frequently, and each attempt takes an exclusive lock over three read-mark
slots (`wal2RestartOk`). Whether that lock attempt is itself a contention source at 384
CPUs is **not measured** — but it is on the commit path of every writer once the WAL is
past 1000 frames, which is essentially always.

**Open question for Dan:** was leaving `journal_size_limit` unset deliberate? A larger
threshold means fewer switch attempts (less lock traffic) but a larger floor on WAL size —
trivially affordable at 6 TB RAM. This is a one-line, reversible experiment.

### `SQLITE_ENABLE_WAL_BIGHASH` is the fingerprint of this problem

The flag changes the wal-index hash geometry (`wal.c:855-903`):

| | default | with `WAL_BIGHASH` |
|---|---|---|
| `ht_slot` type | `u16` | `u32` |
| `HASHTABLE_BITS` | 12 → **4K frames/hash** | 17 → **128K frames/hash** |
| `WALINDEX_PGSZ` | ~32 KB | ~1.5 MB |
| `WAL_VERSION2` | 3021000 | 3021001 |

Two things follow. First, a `u16` slot cannot represent a frame index above 65,535, so the
stock build has a hard ceiling on usable WAL length; **choosing this flag is only necessary
if WAL files were actually reaching that scale.** Second, frame lookup walks hash segments,
so at a 500,000-frame WAL the stock build searches ~125 segments where a BIGHASH build
searches ~4 — a 32× reduction in lookup work, at the cost of 48× larger wal-index pages
(irrelevant at 6 TB RAM).

This is a well-chosen flag for the observed problem. It is also a **file-format change**
(the comment at `wal.c:897` says so explicitly: "Changing any of these constants will alter
the wal-index format and create incompatibilities") — see `08-custom-flags.md` and unit 8
for the tooling consequences.

---

## 2. HC-Tree: verified absence of checkpointing

`sqlite3HctBtreeCheckpoint()` (`hctree.c:3753`), in full:

```c
int sqlite3HctBtreeCheckpoint(Btree *p, int eMode, int *pnLog, int *pnCkpt){
  return SQLITE_OK;
}
```

It is registered in the HC-Tree method table (`btwrapper.c:491`,
`.xBtreeCheckpoint = sqlite3HctBtreeCheckpoint`) and reached through the standard dispatch
thunk (`btwrapper.c:404`). So `sqlite3_wal_checkpoint_v2()` on an HC-Tree database returns
success having done nothing.

**This confirms the claim from code rather than documentation, as required.** HC-Tree does
not checkpoint because it has no WAL to checkpoint: it writes pages into the database file
directly, with visibility governed by per-cell TIDs and the transaction map, so there is no
"catch the main file up with the log" step to starve.

Note the out-parameters are not written by the stub. That is harmless only because
`sqlite3_wal_checkpoint_v2()` initialises them itself (`main.c`, "Initialize the output
variables to -1 in case an error occurs"). A caller reading `nLog`/`nCkpt` on HC-Tree gets
`-1`, not garbage.

### Bedrock's checkpoint thread on HC-Tree

`SQLite::commit()` runs a checkpoint after releasing the commit lock
(`sqlitecluster/SQLite.cpp:1203-1215`), guarded by:

```cpp
if (!_sharedData.checkpointInProgress.test_and_set()) {
    if (_sharedData.outstandingFramesToCheckpoint) { … }
}
```

`outstandingFramesToCheckpoint` is set **only** by `_walHookCallback()`
(`sqlitecluster/SQLite.cpp:392-395`), registered via `sqlite3_wal_hook()`
(`sqlitecluster/SQLite.cpp:314`). HC-Tree never invokes a WAL hook, so the counter stays
zero and the checkpoint block never executes. The thread is inert, not merely no-op — it
does not even reach the stub call. **This is correct behaviour, not a bug**, and is
recorded here so nobody spends time "fixing" it.

---

## 3. What HC-Tree does instead — and a leak

Rather than a WAL, each HC-Tree connection owns an `HctLog` holding **two log files**,
created in the Btree open path (`hctree.c:775`, `sqlite3HctLogNew`) and released on close
(`hctree.c:847`, `sqlite3HctLogClose`). Transactions are written to the log and to the
journal; space is reclaimed through the page manager's free-page baskets (`hct_pman.c`)
rather than by folding a log back into the main file.

`sqlite3HctLogClose()` (`hct_log.c:245`) contains careful logic to decide whether a log
file may be removed immediately: in `FOLLOWER` mode it defers if the log holds transactions
newer than the journal's safe CID, otherwise it may delete at once. **And then it does not
delete.** The call is commented out (`hct_log.c:268`):

```c
      if( p->zPath && bDefer==0 ){
        // unlink(p->zPath);
        sqlite3_free(p->zPath);
      }
```

The deferred path is likewise disabled — `sqlite3HctLogFree()` (`hct_log.c:444`):

```c
        if( bUnlink && 0 ) unlink(pFile->zPath);
```

`&& 0` is an unconditional disable. Both look like deliberate temporary suppressions
(perhaps for post-mortem debugging) that were never reverted.

**Consequence:** HC-Tree log files are never removed while the process runs. The only
cleanup is at startup — `hctFileServerInitUnlinkLog()` (`hct_file.c:786`) called from
initialisation (`hct_file.c:1033`) via `hctFileFindLogs()`.

**Severity is bounded by connection churn, not transaction rate**, because logs are
per-connection and Bedrock pools connections (`SQLitePool`, `BedrockServer.cpp:115`). A
stable pool leaks little; a pool that recycles handles, or a process that opens
short-lived `SQLite` copies (the copy constructor builds a fresh handle,
`sqlitecluster/SQLite.cpp:346`), leaks two files per cycle until restart. Tracked as
`12-bugs.md` #4. **Question for Dan Kennedy:** are these two disabled `unlink()` calls
intentional?

---

## 4. Comparison at Expensify's scale

| | WAL2 | HC-Tree |
|---|---|---|
| Catch-up step | checkpoint, whole file at a time | none — writes go to the database file |
| Starvation failure mode | writer never blocks; current WAL grows without bound (`wal.c:5105`) | not applicable |
| Long-reader sensitivity | **high** — a single long reader blocks the file switch for everyone | shifts to snapshot retention / GC horizon (unit 5) |
| Tuning knob | `journal_size_limit` (unset by Bedrock; defaults to 1000 frames) | none for this |
| Space reclamation | checkpoint + WAL reset | free-page baskets (`hct_pman.c`) |
| Known space leak | — | log files never unlinked at runtime (§3) |
| Read-path cost as log grows | grows with WAL length; mitigated 32× by `WAL_BIGHASH` | no equivalent |

**The headline for a decision:** HC-Tree genuinely removes the checkpoint-starvation class
of failure — this is its clearest structural advantage over WAL2 and it is verified in
code. But it does not remove the underlying pressure, it relocates it. WAL2's long-reader
problem becomes HC-Tree's snapshot-retention problem: old versions must be kept while any
reader can still see them, and the transaction map's `iMinTid`/`iMinCid` horizon
(`hct_tmap.c`) is what bounds reclamation. `hct_journal.c:922` returning
`"snapshot too old"` shows there is a retention limit and that exceeding it is a
user-visible error. **Unit 5 must establish whether a long reader that today causes WAL
growth would instead cause snapshot-too-old failures or unbounded version retention.** That
is the real like-for-like comparison, and it is not answered yet.

---

## 5. Open questions

1. Was leaving `journal_size_limit` unset deliberate? (§1 — cheap, reversible experiment.)
2. Are the two disabled `unlink()` calls in `hct_log.c` intentional? (§3, Dan Kennedy.)
3. Do we have historical WAL-size data from the starvation incidents? It would confirm the
   `WAL_BIGHASH` reasoning and size the frame counts actually reached.
