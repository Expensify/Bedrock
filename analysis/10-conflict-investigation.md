# 10 — Why does HC-Tree conflict as much as or more than WAL2?

**Priority:** P1 (Dan, 2026-09-07) — the headline question
**Status:** IN PROGRESS — first pass complete, verdicts below. Living document.
**Drift:** none of the code in this file differs between the vendored Aug-28 drop and
check-in `eedd80c1a9749300`, **except** the `IdxDelete` path noted in §6. Everything else
here describes exactly what production runs.

**RESUME POINT:** covered — read-set representation, validation algorithms on both sides,
`hctDbTidIsConflict`, range coalescing. Next — (i) the write/write path
(`hctDbWriteWriteConflict`, `hct_database.c:5945+`), (ii) `hct_journalhash.c` for H5,
(iii) `hct_pman.c` free-page baskets for H3, (iv) Bedrock's journal tables for H4,
(v) `BedrockConflictManager` for H7.

---

## Headline

**Both engines track reads as key ranges and detect conflicts at row/key granularity. The
granularity is not the difference. The difference is the validation algorithm.**

- **WAL2** validates with an **in-memory sorted merge join** between its read ranges and
  the write keys of concurrently-committed transactions. Cost is
  O(*n_read* + *n_write*) over two sorted arrays. It touches no pages and no shared state.
- **HC-Tree** validates by **physically re-traversing the B-tree** over every recorded read
  range at commit time, loading pages and performing a shared transaction-map lookup **per
  row visited**. Cost is O(*rows currently in the read range*).

So the "row-level locking" claim is true and is *not* where the problem lies. HC-Tree's
conflict *predicate* is at least as precise as WAL2's. What is worse is the *cost of
evaluating it*, and cost converts into conflicts through a feedback loop: validation is
performed while the transaction is still live, so a slower validation widens the window in
which another transaction can invalidate this one, which produces more retries, which
produces more concurrent transactions, which lengthens validation further.

This reframes the question from "why is the locking coarser?" (it isn't) to **"why is
validation so expensive, and what does that cost us in conflicts?"**

---

## 1. How each engine represents the read set

### WAL2 — `btree.c`

Reads accumulate into `BtShared.conc.aReadIntkey[]` as
`BtReadIntkey{ iRoot, iMin, iMax }` — a root page plus an inclusive key range. Index
reads use the parallel `aReadIndex[]`. Range ends are set at `btree.c:1806-1834`;
notably `p->iMax = bEof ? LARGEST_INT64 : iKey;` (`btree.c:1807`).

Before validation, `btreeBcReadIntkeySort()` (`btree.c:1050`) **sorts the ranges and
coalesces overlapping or adjacent ones**:

```c
if( pIn->iRoot==pOut->iRoot
 && (pIn->iMin<=pOut->iMax || (pIn->iMin==pOut->iMax+1))
){ if( pIn->iMax>pOut->iMax ) pOut->iMax = pIn->iMax; }
```

(`btree.c:1070-1077`; the `iMin==iMax+1` form is deliberate overflow avoidance.)

### HC-Tree — `hct_database.c`

Reads accumulate per cursor as a linked list of `HctCsrIntkeyOp{ iFirst, iLast, iLogical,
iPhysical }` (`hct_database.c:48`) or `HctCsrIndexOp{ pFirst, nFirst, pLast, nLast, … }`
(`hct_database.c:61`), built by `hctDbCsrScanStart()` (`hct_database.c:2166`) and closed by
`hctDbCsrScanFinish()` (`hct_database.c:2198`).

The same EOF extension exists: at `hct_database.c:2210-2216`, a forward scan that reaches
EOF sets `iVal = LARGEST_INT64`; a reverse scan sets `SMALLEST_INT64`.

**This is the same representation and the same EOF over-approximation as WAL2.** An early
hypothesis that HC-Tree uniquely inflates ranges to infinity is **killed** — `btree.c:1807`
does exactly the same thing.

---

## 2. How each engine validates — the actual difference

### WAL2: merge join, no I/O

`btreeBcDetectIntkeyConflict()` (`btree.c:1361`) walks the sorted read array and the
sorted write array in lockstep:

```c
if( iRootWrite < iRootRead ){ iWrite++; }
else if( iRootWrite > iRootRead ){ iRead++; }
else if( iKey < aRead[iRead].iMin ){ iWrite++; }
else if( iKey > aRead[iRead].iMax ){ iRead++; }
else { /* conflict */ return SQLITE_BUSY_SNAPSHOT; }
```

`btreeBcDetectIndexConflict()` (`btree.c:1417`) is the index analogue. Both are pure
in-memory comparisons of two arrays. **No page is read. No lock is taken. No shared
structure is consulted.** The cost is bounded by how much the transaction read and how
much concurrent transactions wrote — not by how much data exists.

### HC-Tree: physical re-traversal with a shared lookup per row

`hctDbValidateIntkey()` (`hct_database.c:8132`) walks each recorded op and, for each,
re-seeks the tree and steps every key in the range:

```c
while( rc==SQLITE_OK && !sqlite3HctDbCsrEof(pCsr) ){
  nStep++;
  sqlite3HctDbCsrKey(pCsr, &iKey);
  if( iKey>=pOp->iFirst && iKey<=pOp->iLast ){
    rc = hctDbValidateEntry(pDb, pCsr);
  }
  if( rc!=SQLITE_OK || iKey>=pOp->iLast ) break;
  rc = hctDbCsrNext(pCsr);
}
```

(`hct_database.c:8191-8200`.) `hctDbValidateIndex()` (`hct_database.c:8232`) is the index
analogue.

`hctDbValidateEntry()` (`hct_database.c:8092`) then reads the cell's TID and asks
`hctDbTidIsConflict()` (`hct_database.c:994`), which performs
`hctDbTMapLookup(pDb, iTid & HCT_TID_MASK, &eState)` — **a lookup in the shared
transaction map, per row visited.**

There is a fast path, but it is narrow. At `hct_database.c:8150-8157`:

```c
u32 iPhys = sqlite3HctFilePageMapping(pDb->pFile, pOp->iLogical, &bEvict);
if( pOp->iPhysical==iPhys && bEvict==0 ) continue;
```

If the logical page holding the read still maps to the same physical page and has not been
evicted, the whole op is skipped. **But `iLogical` is only non-zero for reads confined to a
single page** — `hctDbCsrScanFinish()` zeroes it the moment the scan crosses a page
boundary (`hct_database.c:2219-2221`: `if( pCsr->pg.iPg!=pOp->iLogical ){ pOp->iLogical =
pOp->iPhysical = 0; }`). So point lookups get the fast path; **any multi-page scan always
pays the full re-traversal.**

Note also what the fast path is: a *page-mapping* comparison. HC-Tree's cheap check is
page-granular; only its expensive check is row-granular. That is the grain of truth behind
"row-level locking isn't helping" — the row-level precision is real, but it is only reached
by paying the expensive path.

---

## 3. Hypothesis verdicts (first pass)

| # | Hypothesis | Verdict |
|---|---|---|
| H1 | Granularity is not actually row-level on Bedrock's paths | **Killed as stated.** The conflict predicate is genuinely per-row (`hctDbValidateEntry`, `hct_database.c:8092`). But *reworded and confirmed*: the cheap path is page-granular, the row-granular path is the expensive one. |
| H2 | Validation scope is wider than the write set — read sets conflict | **Confirmed, but not a differentiator.** HC-Tree does conflict on reads (`rcCommit` doc, `hct_database.c:343-358`; `hctDbLogReadConflict`, `hct_database.c:5883`). So does WAL2 (`btreeBcDetectIntkeyConflict`). Both are read/write-conflicting optimistic schemes. |
| H3 | Shared hot structures serialize every transaction | **Confirmed as a mechanism, magnitude unmeasured.** `hctDbTMapLookup()` is called per validated row, and the fast path that would avoid it is **commented out** — see §4. This is the strongest 384-CPU concern. |
| H4 | Bedrock's journal tables are the true conflict set | **Partially confirmed — sharding is good, but the head of each shard is a contended maintenance hot spot.** See §4.5. |
| H5 | Hash/bitmap aliasing produces false conflicts | **Not yet examined** (`hct_journalhash.c`). Note: HC-Tree validation compares real keys and real TIDs, so aliasing would have to enter via the journal hash, not via validation. |
| H6 | The two engines count different events as "conflicts" | **Partially confirmed — important for interpreting our metrics.** See §5. |
| H7 | Bedrock's own layer amplifies engine conflicts | **Killed.** `BedrockConflictManager` (`BedrockConflictManager.cpp`, 66 lines) is *purely* a profiling counter: `recordTables()` increments per-command/per-table use counts under a mutex, and `generateReport()` prints them. It explicitly skips journal tables. It makes no retry, ordering, or locking decision and cannot amplify anything. |

---

## 4. Specific defects and missed optimizations found (the actionable list)

### 4.1 The `iLocalMinTid` fast path is commented out — `hct_database.c:995`

```c
static int hctDbTidIsConflict(HctDatabase *pDb, u64 iTid){
  if( iTid==pDb->iTid /* || iTid<=pDb->iLocalMinTid */ || iTid==LARGEST_TID ){
```

`iLocalMinTid` is documented (`hct_database.c:332-336`) as the TID below which all
transactions "have been fully committed or rolled back" — precisely the condition under
which a row cannot be a conflict and no map lookup is needed. It is disabled.

**Consequence:** every validated row against an old, long-settled row still performs a
shared `hctDbTMapLookup`. On a 6 TB database where the overwhelming majority of rows were
written long ago, this is the common case, not the rare one. At 384 CPUs this is a shared
structure touched once per row per validation across every core.

**This is the single highest-value lead in the investigation so far.** It is also the one
most likely to have a good reason for being disabled — the adjacent dead code (§4.2)
discusses a rollback subtlety that may be why. **Question for Dan / Dan Kennedy:** why is
`iLocalMinTid` commented out, and what would it take to re-enable it? If it is the
rollback case, is a narrower condition safe?

### 4.2 Dead code in `hctDbTidIsConflict` — `hct_database.c:1008-1021`

```c
    if( eState==HCT_TMAP_COMMITTED && iCid<=pDb->iSnapshotId ) return 0;
    return 1;                                              /* <-- unconditional */
    if( eState==HCT_TMAP_WRITING || eState==HCT_TMAP_VALIDATING ) return 1;   /* dead */
    ...
    if( eState==HCT_TMAP_ROLLBACK ) return 1;                                 /* dead */
    assert( eState==HCT_TMAP_COMMITTED );
    return (iCid > pDb->iSnapshotId);                                         /* dead */
```

**Behaviourally this is a no-op** — every dead branch also returns 1, so the short-circuit
is equivalent. Recorded because (a) it hides a comment explaining a real subtlety
("a key that has been rolled back … the previous version … may be a write/write
conflict"), and (b) it signals that the rollback case is knowingly **over-conservative**:
a rolled-back transaction's row is treated as a conflict even though the write never
happened. That is a genuine source of *false* conflicts, acknowledged in the comment as
something "ideally, this code would check".

**Under retry storms this compounds.** Every aborted transaction leaves rows whose TIDs
now cause *other* transactions to abort. That is a plausible positive-feedback mechanism
for exactly the symptom Dan reports: conflicts that do not fall as concurrency rises.
Ranked as the second-highest lead. Not yet quantified — needs the write/write path read
(next session) to confirm the loop closes.

### 4.3 Index read ops are never coalesced — `hct_database.c:2242`

Intkey ops get a containment check before being added to the list
(`hct_database.c:2231`): if the new op lies entirely within the previous one, it is
discarded. Index ops get no such check — `hctDbCsrScanFinish()` unconditionally does:

```c
pOp->pNextOp = pCsr->index.pOpList;
pCsr->index.pOpList = pOp;
```

So a transaction performing *n* index seeks accumulates *n* ops, each re-validated by full
re-traversal at commit. Even the intkey check is weaker than WAL2's: it only compares
against the *immediately previous* op, not the whole set, and only detects containment,
not overlap or adjacency.

**Port target (P2):** WAL2's `btreeBcReadIntkeySort()` (`btree.c:1050`) — sort all ranges,
then coalesce overlapping *and adjacent* ones. Applying this to HC-Tree's op lists before
validation would cut both the number of re-traversals and the number of rows revisited,
with no change to the conflict predicate. See `11-portable-optimizations.md` #1.

### 4.5 The journal head is a maintenance-induced hot spot — `SQLite::prepare()`

Bedrock shards journals well: `journalTables` defaults to `workerThreads`
(`BedrockServer.cpp:97`), i.e. one per worker (~384), and each transaction takes the next
shard round-robin via a global counter,
`_journalName = _journalNames[journalID % _journalNames.size()]`
(`sqlitecluster/SQLite.cpp:948`, `journalID = _sharedData.nextJournalCount++`). So
consecutive transactions land on different shards, and the *append* point — the natural
hot spot of an append-only journal — is spread ~384 ways. **The obvious version of H4 is
therefore killed: journal appends are not the conflict set.**

But every writing transaction also performs two *maintenance* operations at the **head** of
its shard, inside the transaction:

```cpp
SASSERT(!SQuery(_db, "SELECT MIN(id) FROM " + _journalName, journalLookupResult));
```
(`sqlitecluster/SQLite.cpp:952` — runs unconditionally on every `prepare()`), and, when
trimming is due:
```cpp
string query = "DELETE FROM " + _journalName + " WHERE id < " + SQ(oldestCommitToKeep)
             + " LIMIT " + SQ(deleteLimit);   /* deleteLimit == 10 */
```
(`sqlitecluster/SQLite.cpp:969`, under `shared_lock(_sharedData.writeLock)`).

So on each shard the pattern is: **read the head on every transaction; delete the head
periodically; append at the tail.** The head is the one region where reads and writes
coincide, and neither touch comes from application logic — both are journal housekeeping.
A transaction whose `SELECT MIN(id)` read the very rows a concurrent transaction on the
same shard is deleting is a textbook read/write conflict, and under HC-Tree it is detected
per-row by exactly the machinery in §2.

**How bad is it?** Not established. With ~384 shards and round-robin assignment, two
transactions share a shard only if ~384 transactions are in flight within one transaction's
lifetime — plausible at 384 CPUs, but this is arithmetic, not measurement. Two things make
it worth pursuing anyway:

- The `SELECT MIN(id)` is **unconditional** — it runs on every single transaction even when
  no trimming will occur. It exists only to decide whether to trim.
- Both touches are *avoidable*. The min-id could be cached in `_sharedData` per shard
  rather than re-read from the database inside every transaction, and trimming could be
  moved out of the write path entirely (a background sweep). Either change removes the read
  from the transaction's read set, and with it the conflict edge.

**UNVERIFIED and needs checking before acting:** what read range HC-Tree actually records
for `SELECT MIN(id)` on an intkey table. If the planner emits a `First()` seek,
`hctDbCsrScanStart`/`Finish` may record `[SMALLEST_INT64, min_id]` rather than a point —
which would make the read range cover exactly the deleted region and turn an occasional
overlap into a systematic one. This is directly testable and is the first thing to check
next session.

**Cheap win regardless of the above:** make the `SELECT MIN(id)` conditional, or cache it.
It is a per-transaction database read on the write path whose result is usually
"nothing to do".

### 4.4 Validation cost is unbounded by the transaction's own size

WAL2's validation cost depends on what *this* transaction read and what *others* wrote.
HC-Tree's depends on **how many rows currently exist in the read range** — which
concurrent inserts can grow. A transaction that scanned to EOF revalidates the tail
*including rows inserted since*. This makes validation cost adversarial under exactly the
append-heavy, high-concurrency conditions Bedrock creates.

---

## 5. H6: the two engines may not be counting the same thing

`hctDbSetCannotCommit()` (`hct_database.c:5905`) sets `rcCommit = SQLITE_BUSY_SNAPSHOT`
**eagerly, during the scan**, the moment a cursor steps onto a history entry — not at
commit time. The transaction is doomed from that point and fails at `COMMIT` with the
stored code. WAL2 has no equivalent early-doom path; it discovers conflicts only in
`btreeBcDetectIntkeyConflict()` at commit.

Two consequences:

1. **HC-Tree can doom a transaction that would never have reached a conflicting commit** —
   e.g. one that would have been rolled back anyway, or whose later logic would not have
   committed. Whether that shows up as a counted "conflict" depends on Bedrock's
   accounting.
2. **Any comparison of conflict counters between the two engines is suspect** until we
   confirm both are counting the same event. HC-Tree logs conflicts through
   `sqlite3_log(SQLITE_BUSY_SNAPSHOT, …)` in three distinct places
   (`hctDbLogWriteConflict` `hct_database.c:5856`, `hctDbLogReadConflict`
   `hct_database.c:5883`, `hctDbSetCannotCommit` `hct_database.c:5905`); WAL2 logs with
   `sqlite3_log(SQLITE_OK, …)` — **a different result code**, at `btree.c:1391`.

**Action:** before drawing conclusions from production conflict rates, confirm how
Bedrock's counters are derived. If they come from log scraping or from the returned error
code, the two engines are not directly comparable — WAL2's conflict log line is emitted
with `SQLITE_OK` as its code, HC-Tree's with `SQLITE_BUSY_SNAPSHOT`. **Question for Dan:**
where does our conflict metric come from?

---

## 6. What the newer check-in changes (drifted code)

The `OP_IdxDelete` no-op skip (see `00-provenance.md`) removes a delete+reinsert of a
byte-identical index entry, which removes both a write and its conflict footprint. It
engages **only for expression indexes** (`delete.c:926`, gated on `pIdx->bHasExpr`).

The equivalent gap for **ordinary** indexes remains open in both versions: if an indexed
column appears in the `SET` list but is assigned its existing value, `update.c`'s static
test (`indexColumnIsBeingUpdated()`, `update.c:97`) says "index affected" and HC-Tree
performs a real delete + insert of an identical entry — manufacturing a write-set entry,
and a conflict opportunity, from a semantic no-op. ORMs that write back every column hit
this constantly.

**This is a strong P2 candidate** and is logged in `11-portable-optimizations.md` #2.

---

## 7. Instrumentation available today

`HCT_VALIDATE_TIMERS` (`hct_database.c:7998-8090`) is a compile-time option that logs slow
validations at three granularities:

- `hctDbValidateWarning()` — whole transaction: total µs, count of intkey ops, count of
  point lookups, count of index ops
- `hctDbValidateCsrWarning()` — per cursor, with root page
- `hctDbValidateOpWarning()` — per op, with step count

Thresholds `HCT_VALIDATE_THRESHOLD`, `HCT_VALIDATE_CSR_THRESHOLD`,
`HCT_VALIDATE_OP_THRESHOLD`.

**This is very close to exactly the data needed to settle this investigation empirically**
— it directly measures the quantity §2 predicts is the problem (rows stepped per
validation). It is not currently compiled in.

**Recommended immediate experiment:** build a Bedrock with `-DHCT_VALIDATE_TIMERS` and low
thresholds, run production-shaped load, and collect the distribution of `nStep` per op and
µs per validation. If validation times are large and dominated by high step counts, §2 is
confirmed and §4.1/§4.3 become the fix list. This requires only a compile flag, no source
change.

---

## 8. Open questions for Dan

1. Why is `iLocalMinTid` commented out in `hctDbTidIsConflict` (`hct_database.c:995`)?
2. Where does our production conflict metric come from, and does it count HC-Tree's eager
   `hctDbSetCannotCommit` dooming the same way it counts WAL2's commit-time detection?
3. Does the production schema use expression indexes on hot tables? (Determines whether
   the newer check-in's `IdxDelete` fix is worth anything to us.)
4. Has anyone measured validation time separately from total transaction time on HC-Tree?
5. Confirm the rollback over-conservatism in `hctDbTidIsConflict` is intentional and known.
