# 06 — Readers, snapshots and long-reader behaviour

**Unit:** 5
**Status:** COMPLETE for the main comparison
**Drift:** none. All code cited is identical in the vendored Aug-28 drop.
**Why this unit mattered:** `04-checkpointing.md` §4 argued that HC-Tree removes
checkpoint starvation but *relocates* the underlying pressure rather than removing it, and
left the like-for-like comparison open. This unit closes it.

---

## Summary

**Both engines convert a long-running reader into unbounded growth, not into a writer
stall. They differ in *what* grows and in *which* readers count.**

| | WAL2 | HC-Tree |
|---|---|---|
| What a long reader pins | the other WAL file — the file switch cannot happen | the GC horizon `iMinMinTid` — freed page ids cannot be reused |
| What grows as a result | the **current WAL file**, without bound (`wal.c:5105`) | the **main database file**, as the allocator cannot recycle |
| Does an **idle** connection pin it? | **Yes** while it holds a read mark | **No** — idle clients are dragged forward automatically (§2) |
| Secondary cost of growth | read lookups slow as the WAL lengthens (mitigated 32× by `WAL_BIGHASH`) | none equivalent — page lookup does not scan a log |
| Writer blocking | never | never |
| Bound on retention | none — grows until disk or performance fails | none — grows until disk fails |

**The material advantage is HC-Tree's**, on two counts. First, an idle connection is
harmless under HC-Tree but harmful under WAL2 — and a pooled server holds many idle
connections. Second, WAL growth degrades the read path, whereas an under-recycled main file
does not.

**But neither engine bounds retention.** Whichever is chosen, a genuinely long-running read
transaction is an operational hazard with no engine-level backstop.

---

## 1. WAL2: read marks pin the file switch

A reader takes a shared lock on one of the read-mark slots, whose WAL2 interpretation is
one of `WAL_LOCK_PART1`, `WAL_LOCK_PART1_FULL2`, `WAL_LOCK_PART2_FULL1`, `WAL_LOCK_PART2`
(`wal.c:518-522`). Switching WAL files requires an **exclusive** lock over the three
read-mark slots for the other file — `wal2RestartOk()` (`wal.c:5010`):

```c
int eLock = 1 + (iApp==0);
return walLockExclusive(pWal, WAL_READ_LOCK(eLock), 3);
```

Any reader holding that file blocks the switch, and `walRestartLog()` then swallows the
`SQLITE_BUSY` and keeps appending (`wal.c:5104-5106`, analysed in `04-checkpointing.md`
§1). The reader does not have to be *doing* anything — merely holding the read mark is
enough. **A pooled connection sitting on an open read transaction is indistinguishable from
a busy one.**

## 2. HC-Tree: the GC horizon, and why idle connections are harmless

Visibility is per-row. Each cell may carry a TID (`HCTDB_HAS_TID`), and
`hctDbTidIsConflict()` (`hct_database.c:994`) resolves it through the transaction map to a
CID and a state (`WRITING` / `VALIDATING` / `ROLLBACK` / `COMMITTED`), comparing against the
reader's `iSnapshotId`. There is no log to consult and no page-version chain to walk for
the common case — visibility is a property of the row itself.

Retention is governed by the transaction map. Each client publishes an `iLockValue`
combining a flag and a "safe TID" (`hct_tmap.c:135-145`):

- bit 56 (`HCT_LOCKVALUE_ACTIVE`, `hct_tmap.c:160`) is set while a read transaction is
  active
- the low bits hold a TID such that pages freed by that transaction and all earlier ones
  may be reused without disturbing this client

`HctTMapServer.iMinMinTid` is the minimum across all clients (`hct_tmap.c:131`) — **the
garbage-collection horizon**. The file header states the consequence plainly
(`hct_tmap.c:33-35`): "So long as this object exists, it is not safe to reuse any page ids
(logical or physical) freed by transactions with TID values > iMinTid."

### The dormant-connection problem, and its actual fix

The header comment names the hazard directly (`hct_tmap.c:59-64`):

> The above creates a problem - a single dormant connection can prevent all reuse of freed
> logical and physical pages. This is addressed by using smart reference objects of type
> `HctTMapRef` that support the reference being revoked by the server at any time.

**`HctTMapRef` does not exist.** The identifier appears exactly twice in the tree, both
inside that comment; there is no such struct. The comment is stale — the same block carries
`TODO: This all needs updating!!!` (`hct_tmap.c:46`).

The mechanism that *does* solve it is `sqlite3HctTMapScan()` (`hct_tmap.c:592`), which
walks every client and **advances the horizon of any client that is not actively reading**:

```c
  for(pClient=p->pServer->pClientList; pClient; pClient=pClient->pNextClient){
    u64 iVal = HctAtomicLoad(&pClient->iLockValue);
    u64 iTid = (iVal & HCT_TMAP_CID_MASK);

    if( (iVal & HCT_LOCKVALUE_ACTIVE)==0 && iTid<iSafe ){
      hctTMapBoolCAS64(&pClient->iLockValue, iVal, iSafe);
      ...
    }
    iSafe = MIN(iSafe, iTid);
  }
  HctAtomicStore(&p->pServer->iMinMinTid, iSafe);
```

An idle client's lock value is CAS-advanced by whichever client happens to run the scan, so
**a dormant connection cannot pin the horizon.** Only a client with
`HCT_LOCKVALUE_ACTIVE` set — an actually-open read transaction — holds it back.

**This is a real and material advantage over WAL2** for a server that maintains a
connection pool: under WAL2 every pooled connection holding a read mark is a potential
blocker; under HC-Tree only genuinely active readers are.

### When the scan runs — and a 384-CPU cost

The scan is not run per commit. `sqlite3HctDbTMapScan()` (`hct_database.c:8340`) is called
after each commit or write-rollback, but throttles by a global page-write counter:

```c
  nFinalWrite = sqlite3HctFileIncrWriteCount(pDb->pFile, nWrite);
  if( (nFinalWrite / nPageScan)!=((nFinalWrite-nWrite) / nPageScan) ){
    sqlite3HctTMapScan(sqlite3HctFileTMapClient(pDb->pFile));
  }
```

So roughly once per `nPageScan` pages written across the whole database
(`HCT_DEFAULT_NPAGESCAN = 1024`, `hctInt.h:56`).

Two consequences worth noting:

1. **The horizon only advances when writes happen.** Benign — if nothing is being written,
   nothing is being freed.
2. **The scan is O(number of clients) under a single global mutex**
   (`ENTER_TMAP_MUTEX`, `hct_tmap.c:596`). At a 384-connection pool that is a 384-iteration
   walk holding the transaction-map mutex, once per 1024 pages written. Whether that is
   material is measurable today — `tmap.mutex_block / tmap.mutex_attempt` in `hctstats`
   (`13-instrumentation.md`). `hct_npagescan` is a tunable pragma, so the frequency is
   adjustable without a rebuild.

## 3. `"snapshot too old"` is narrower than it looked

`04-checkpointing.md` §4 speculated that HC-Tree's retention limit might surface to readers
as `"snapshot too old"` (`hct_journal.c:922`). **That was wrong, and is corrected here.**

The message is raised in the FOLLOWER-mode replication path, checking that an incoming
replicated transaction was prepared against a snapshot the follower has already reached
(`hct_journal.c:920-924`):

```c
  if( sqlite3HctDbSnapshotId(pJrnl->pDb)<(iSnapshot*HCT_CID_INCREMENT) ){
    hctJournalSetDbError(db, SQLITE_BUSY_SNAPSHOT, "snapshot too old");
    return SQLITE_BUSY_SNAPSHOT;
  }
```

It is a replication-ordering check, guarded by an explicit FOLLOWER-mode test immediately
above it — **not** a reader-side retention limit. So HC-Tree has **no "snapshot too old"
backstop for long readers at all**: a long reader is not evicted or errored, it simply
holds the horizon and the file grows. That is a cleaner failure mode than an unexpected
error mid-query, but it also means nothing bounds the growth.

## 4. Implications at Expensify's scale

- **A pooled server favours HC-Tree here.** Idle pooled connections are free under HC-Tree
  and costly under WAL2 (§1, §2). With a pool sized near 384, this is not a marginal
  difference.
- **Neither engine protects against a genuinely long read transaction.** Under WAL2 it
  grows the WAL and slows every reader; under HC-Tree it stops page recycling and grows the
  file. **Recommendation: bound read-transaction lifetime at the application layer
  regardless of engine choice** — this is not something either engine will do for you.
- **HC-Tree's growth is the more benign of the two.** An under-recycled database file costs
  disk; a long WAL costs disk *and* read latency on every lookup, which is why
  `WAL_BIGHASH` was needed at all (`08-custom-flags.md` §1).
- **`hct_npagescan` is an untouched knob** with a direct contention/latency trade: lower
  means the horizon advances sooner (better reuse) at the cost of more O(nClient) scans
  under a global mutex; higher means the reverse. Bedrock sets neither it nor any other
  HC-Tree pragma (§5).

## 5. HC-Tree exposes tunable pragmas — Bedrock sets none of them

Discovered while tracing `nPageScan`. `sqlite3HctBtreePragma()` (`hctree.c`) implements ten
HC-Tree-specific pragmas:

| Pragma | Default | Notes |
|---|---|---|
| `hct_npagescan` | 1024 (`hctInt.h:56`) | TMap scan interval, this unit §2 |
| `hct_npageset` | 256 (`hctInt.h:54`) | **Page-manager batch size — the knob for the allocator-mutex contention in `11-portable-optimizations.md` B8** |
| `hct_ndbfile` | 1 (`hctInt.h:53`) | Number of database files |
| `hct_try_before_unevict` | 100 (`hctInt.h:55`) | Eviction retry threshold |
| `hct_prefault` | — | **Launches N threads to fault the mapping into RAM** (`hct_file.c:2505-2530`); negative N means minor faults only |
| `hct_log` | — | Logging control |
| `hct_extra_logging`, `hct_extra_write_logging` | — | Diagnostics |
| `hct_quiescent_integrity_check` | — | Integrity check |
| `hct_create_table_no_cookie` | — | Schema-cookie behaviour |

**Bedrock issues none of these** — a grep for `hct_` across the repo's C++ and configs
returns nothing but the `hctree` identifier itself. So the entire HC-Tree tuning surface is
at upstream defaults, chosen without reference to a 384-CPU / 6 TB-RAM host.

Two stand out and are logged as B9 and B10 in `11-portable-optimizations.md`:

- **`hct_npageset` (256)** directly sets how many free page ids a client batches before
  touching the shared allocator mutex. Raising it trades memory — free at 6 TB — for
  proportionally less mutex traffic. This is the cheapest available response to B8.
- **`hct_prefault`** is a multi-threaded mapping warmer that Bedrock does not use.
  Bedrock's own `VMTouch` is multi-threaded (`VMTouch.cpp:118`,
  `thread::hardware_concurrency()`) but runs **only as a standalone utility mode** that
  exits the process immediately afterwards (`main.cpp:351-355`):

  ```cpp
  if (args.isSet("-checkDBMemoryMapping") || args.isSet("-setDBMemoryMapping")) {
      VMTouch::check(args["-db"].c_str(), args.isSet("-setDBMemoryMapping"), true);
      SStopSignalThread();
      return 0;
  }
  ```

  So **nothing warms the mapping during normal server startup.** On a host where the
  database may fit in RAM, a cold start pays page-fault latency on every first touch.

## 6. Open questions

1. What is the longest-lived read transaction in production? Both engines are exposed to it
   and neither bounds it (§4).
2. Is there a reason Bedrock leaves every HC-Tree pragma at default (§5), or has the tuning
   surface simply not been examined?
3. Was the standalone-only use of `VMTouch` deliberate, or is warming at server startup
   simply missing?
4. For Dan Kennedy: the `HctTMapRef` design described in `hct_tmap.c:59-64` does not exist
   in the code. Is `sqlite3HctTMapScan()` the intended replacement, or is the revocable-
   reference design still pending?
