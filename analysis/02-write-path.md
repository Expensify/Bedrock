# 02 — Write path, commit protocol and locking granularity

**Units:** 1 and 2, combined — they overlap almost completely on the HC-Tree side, where
the write path *is* the locking mechanism (per-row TIDs rather than page locks).
**Status:** COMPLETE for the write/write path. The `08c755b` re-vendor is tracked in §5.
**Drift:** none, except `OP_IdxDelete` (`00-provenance.md`).
**Note:** the read/write half of the commit protocol — read-set representation and
validation — is in `10-conflict-investigation.md` §1–2 and not repeated here. This file
covers the *write* side.

---

## Headline

**The single most important finding in this unit: `iLocalMinTid` is already load-bearing
in the write path, which materially strengthens the case for re-enabling it in
`hctDbTidIsConflict` (optimization B1).**

`hctDbFollowRangeOld()` (`hct_database.c`) uses `iLocalMinTid` to decide when to *stop*
descending a history chain — a correctness-critical decision, since descending too little
would miss a conflict. Yet the same variable is commented out in `hctDbTidIsConflict()`
(`hct_database.c:995`), where it would be a pure optimization. The two uses assert the same
proposition. Details in §3.

**Second finding: inserting a new key is not O(1) on HC-Tree, and its cost is coupled to
long readers** — which links unit 5's GC horizon directly to write-path cost and conflict
probability (§2.2).

---

## 1. Locking granularity: there are no locks

Neither engine takes row or page *locks* in the write path. Both are optimistic:

- **WAL2 / `BEGIN CONCURRENT`** accumulates read ranges and write keys, then validates at
  commit by merge join (`btree.c:1361`). No lock is held across the transaction.
- **HC-Tree** carries visibility on the data itself. Each cell may hold a TID
  (`HCTDB_HAS_TID`); whether a reader sees it is resolved through the transaction map
  against the reader's snapshot (`hctDbTidIsConflict()`, `hct_database.c:994`).

So "row-level locking" is a slight misnomer for both. The accurate statement is **row-level
conflict *detection*, optimistically, at commit** — for both engines. This is why the
granularity framing in P1 did not survive contact with the source
(`10-conflict-investigation.md` §1).

The real granularity difference is not in *what* is protected but in *how the check is
performed*, which is the subject of §2 and of `10-conflict-investigation.md` §2.

## 2. HC-Tree's write/write conflict detection

`hctDbWriteWriteConflict()` (`hct_database.c:5945`) has two distinct cases with very
different costs.

### 2.1 Overwriting an existing entry — O(1), genuinely row-level

When `pOp->nClobber` is set, the insert replaces a cell that is already there. The check
reads that cell's TID and asks whether it conflicts (`hct_database.c:5966-5975`):

```c
    if( pE->flags & HCTDB_HAS_TID ){
      u64 iTid;
      hctMemcpy(&iTid, &aTarget[pE->iOff], sizeof(u64));
      if( hctDbTidIsConflict(pDb, iTid) ){
        hctDbLogWriteConflict(&p->writecsr, iTid, pKey, iKey, 0);
        rc = HCT_SQLITE_BUSY;
      }
    }
```

One cell, one TID, one transaction-map lookup. This is exactly the row-level precision
HC-Tree advertises, and it is cheap.

### 2.2 Inserting a *new* key — a history walk, and it is coupled to long readers

When there is nothing to clobber (`pOp->iInsert > 0`), correctness requires proving that no
concurrent transaction *deleted* a row at this key that this connection cannot see. A
deleted row is not present in the current page, so the check must walk **backwards through
history pages** via range pointers (`hct_database.c:5977-6031`):

```c
    hctDbGetRange(aTarget, iCell, &ptr);
    while( hctDbFollowRangeOld(pDb, &ptr, &bMerge) ){
      ...
      nDescend++;
      ...
      rc = hctDbLeafSearch(pDb, aOld, iKey, pKey, &iCell, &bExact);
      if( rc==SQLITE_OK && bExact ){
        if( bMerge ){
          /* If bMerge is true, then the connection could not see the
          ** delete operation that removed this entry. It is therefore
          ** a write-write conflict. Return HCT_SQLITE_BUSY.  */
```

Each descent loads a page (`hctDbGetPhysical()`) and searches it. The descent count is
accumulated into a counter that is **queryable in production**
(`hct_database.c:6033`):

```c
  pDb->stats.nDescendInWriteWrite += nDescend;
```

— surfaced as `db.descend_in_writewrite` in `hctstats` (`13-instrumentation.md`).

**The coupling that matters:** the descent terminates when the range TID falls to or below
`pDb->iLocalMinTid` (§3). `iLocalMinTid` tracks the settled-transaction frontier, which is
bounded by the global GC horizon `iMinMinTid` — **held back by any actively reading client**
(`06-readers-snapshots.md` §2).

So:

> **A long-running read transaction holds back the GC horizon → `iLocalMinTid` lags →
> history chains are longer → every *insert of a new key* pays more page loads, and has more
> old versions in which to find a conflicting delete.**

This is a write-path cost and a conflict-probability increase that **WAL2 does not have** —
a WAL2 insert does not walk version history. It is also a second, independent mechanism by
which long readers hurt HC-Tree, complementing the space-reclamation effect in unit 5.

**Measurable today:** `db.descend_in_writewrite` divided by write volume. If it is large or
grows over an uptime window, this path is active and long readers are the first thing to
look at. **Magnitude UNVERIFIED.**

## 3. `iLocalMinTid` is already trusted for a stronger purpose than B1 asks for

This sharpens `11-portable-optimizations.md` B1 considerably.

`hctDbFollowRangeOld()` decides whether to keep descending:

```c
  if( iRangeTidValue>pDb->iLocalMinTid ){
    bRet = 1;                       /* keep descending */
    ...
  }else if( (pPtr->iFollowTid & HCT_TID_MASK)>pDb->iLocalMinTid ){
    bRet = 1;                       /* keep descending */
  }
```

Descent **stops** once the range TID is `<= iLocalMinTid`. That is a *correctness*
decision: stopping too early would fail to find a concurrent delete and would let a
conflicting insert commit. The code therefore already asserts:

> **nothing at or below `iLocalMinTid` can be a conflict.**

That is *exactly* the proposition the commented-out clause in `hctDbTidIsConflict()`
(`hct_database.c:995`) would assert:

```c
  if( iTid==pDb->iTid /* || iTid<=pDb->iLocalMinTid */ || iTid==LARGEST_TID ){
    return 0;
```

**So the disabled line is not asserting anything new.** The same claim is already relied on
a few hundred lines away, for a stronger purpose (correctness rather than optimization),
and `iLocalMinTid` is documented as the frontier below which all transactions are "fully
committed or rolled back" (`hct_database.c:332-336`).

**This does not make it safe to uncomment** — there may be a subtlety, and the neighbouring
dead code (`hct_database.c:1013-1018`) discusses a rollback case where a rolled-back
version can mask an older write/write conflict. But it turns the question to Dan Kennedy
from "is this safe?" into a much sharper one:

> `hctDbFollowRangeOld()` already treats `iLocalMinTid` as a correctness bound for history
> descent. Why is the same bound not safe as a short-circuit in `hctDbTidIsConflict()`?

**Do not act on this without an answer.** But it is now a well-founded question rather than
a speculative one, and B1 remains the highest-ceiling contention fix identified
(it would remove a shared transaction-map lookup from the inner loop of every commit
validation, across 384 cores).

## 4. Structural write cost: balancing and defragmentation

HC-Tree instruments its structural write amplification thoroughly — nine `balance_*`
counters plus `defragment`, `update_in_place` and `load_physical_to_free_ovfl`
(`13-instrumentation.md`). `hctDbDefragment()` (`hct_database.c:6049`) rewrites a page in
place, optionally omitting one cell.

`update_in_place` is the one to watch alongside optimization A2: it counts updates that
avoid a structural change. If A2 (skipping no-op index rewrites) works, `update_in_place`
should rise and the `balance_*` counters should fall. **That gives A2 a measurable success
criterion before and after**, which it previously lacked.

## 5. The July 2026 re-vendor (`08c755b`) — provenance recovered, content pending

Bedrock commit `08c755b` ("Fix row-level locking in WAL2 DBs on HC-Tree branches",
2026-07-15) is a whole-amalgamation re-vendor: 4,787 lines changed across
`libstuff/sqlite3.c` and `sqlite3.h`, with the semantic change buried among the mechanical
diff. Reading the Bedrock commit alone cannot isolate it.

The two upstream check-ins involved are recoverable from the vendored headers:

| | `SQLITE_SOURCE_ID` | Branch |
|---|---|---|
| before (`08c755b^`) | `2026-07-09 16:12:25 45f6fccc2ed3e0b6d6f72cf3aeb849a77eace5f8675f40d3fff-experimental` | `unknown` |
| after (`08c755b`) | `2026-07-14 21:08:02 485f67e6c9dd4dbf13e9475f9b0b98df6006742dcf91ce730a5b14c83e3ef634` | `hctree-bedrock-lcd-ex` |

**Two observations that stand on their own:**

1. **The July commit is when Bedrock moved onto the `hctree-bedrock-lcd-ex` branch.**
   `SQLITE_SCM_BRANCH` went from `"unknown"` to `hctree-bedrock-lcd-ex`. So the "fix" was
   at least partly a *branch switch*, not only a patch.

2. **The previous drop was built from a tree that was not a clean check-in.** Its source ID
   is 51 hex characters (not 64) with an `-experimental` suffix, and its branch is
   `"unknown"` — the signature of an amalgamation generated from a working tree with
   uncommitted state rather than from a committed check-in. **That drop is therefore not
   reproducible from the fossil repository**, which is a provenance gap worth recording:
   for that period we cannot say exactly what shipped. It is also a useful contrast with
   today's position, where `00-provenance.md` shows the current drop is a byte-clean
   generated amalgamation of a named check-in.

**Isolating the semantic change requires diffing the two upstream trees.** Both tarball
fetches are currently returning HTTP 503 "Server Overload" from `sqlite.org` — the fossil
server throttles tarball generation. Retries are in progress; if they continue to fail this
is an availability limitation, not an analysis one, and the comparison can be redone at any
time from the two IDs above. **Marked UNVERIFIED pending that fetch.**

## 6. Comparison

| | WAL2 | HC-Tree |
|---|---|---|
| Write-path locks | none (optimistic) | none (optimistic) |
| Conflict carrier | read/write sets validated at commit | per-cell TID + transaction map |
| Overwrite an existing row | write key recorded, merge-joined at commit | O(1) TID check (§2.1) |
| Insert a *new* key | write key recorded; no history walk | **history descent** bounded by `iLocalMinTid` (§2.2) |
| Cost coupled to long readers | via WAL growth only | **also via history-chain length** (§2.2) |
| Structural amplification visible | limited | nine `balance_*` counters + `defragment` (§4) |

**Assessment.** HC-Tree's overwrite path is genuinely cheap and genuinely row-level. Its
new-key insert path is where the hidden cost sits, and that cost is not constant — it grows
with the amount of unreclaimed history, which long readers control. Combined with unit 5's
finding that long readers also block space reuse, **the case for bounding read-transaction
lifetime at the application layer is now supported by two independent mechanisms.**

## 7. Open questions

1. **For Dan Kennedy (sharpened):** `hctDbFollowRangeOld()` already uses `iLocalMinTid` as
   a *correctness* bound on history descent. Why is the same bound not safe as a
   short-circuit in `hctDbTidIsConflict()` (§3)?
2. What is `db.descend_in_writewrite` per write in production? Settles §2.2.
3. Does anyone have the working tree that produced the `-experimental` July-09 drop, or is
   that period simply unreproducible (§5)?
