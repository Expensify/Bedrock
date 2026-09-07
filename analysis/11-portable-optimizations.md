# 11 — Optimizations for HC-Tree (living document)

**Priority:** P2 (port from WAL2) and P4 (invent for 384 CPU / 6 TB RAM), Dan 2026-09-07
**Status:** open, appended during every unit
**Direction:** one-way — improving HC-Tree only. WAL2 improvements are out of scope
except as a source of ideas.

Each entry: the mechanism, the HC-Tree gap, expected benefit at our scale, and
implementation risk. Entries are **not** ordered by value yet; see the ranking at the end,
which is provisional.

---

# Part A — Portable from WAL2

## A1 — Sort and coalesce read ranges before validation

**WAL2 mechanism:** `btreeBcReadIntkeySort()` (`btree.c:1050`) merge-sorts the accumulated
read ranges by `(iRoot, iMin)` and then coalesces every pair that overlaps *or is
adjacent*:

```c
if( pIn->iRoot==pOut->iRoot
 && (pIn->iMin<=pOut->iMax || (pIn->iMin==pOut->iMax+1)) ){
  if( pIn->iMax>pOut->iMax ) pOut->iMax = pIn->iMax;
}
```
(`btree.c:1070-1077`)

**HC-Tree gap:** read ops accumulate as an unsorted per-cursor linked list. Intkey ops get
only a weak dedup — containment against the *immediately preceding* op only
(`hct_database.c:2231`). **Index ops get nothing at all**; `hctDbCsrScanFinish()` blindly
prepends (`hct_database.c:2242`).

**Benefit:** HC-Tree validation cost is O(rows in the read ranges), re-traversed
physically (`hct_database.c:8191`). Coalescing reduces both the number of tree re-seeks
(one per op) and the number of rows revisited where ranges overlap. Two seeks over
adjacent ranges become one seek and one contiguous walk. For a transaction doing many
index probes into the same region — the Bedrock pattern — this could be a large constant
factor.

**Risk:** low. It is a pure reduction of the op set that preserves the union of ranges, so
the conflict predicate is unchanged. The `iMin==iMax+1` overflow-avoidance form should be
copied verbatim. Index ops need a comparison function over serialized records rather than
integers, which is the only real work — `sqlite3HctSerializeRecord` output is already
memcmp-comparable for this purpose (it is compared with `memcmp` in the new `IdxDelete`
path).

**Confidence:** high that it helps; magnitude unmeasured.

## A2 — Skip no-op index entry rewrites for *ordinary* indexes

**Upstream mechanism (newer check-in only):** `OP_IdxDelete` now passes the
about-to-be-inserted record to `sqlite3HctBtreeIdxDelete()`, which skips the delete when
it is byte-identical, and the VDBE then nulls P3 so the paired insert is skipped too
(`hctree.c` / `vdbe.c`, see `00-provenance.md`).

**Gap:** this only engages for expression indexes — `delete.c:926` sets P3 only when
`pIdx->bHasExpr`. For ordinary indexes, `update.c`'s static per-column test
(`indexColumnIsBeingUpdated()`, `update.c:97`) decides whether the index is touched. If an
indexed column is in the `SET` list, the index entry is deleted and reinserted **even when
the assigned value equals the existing one**.

**Benefit:** removes a write, a journal entry, and a conflict opportunity per unchanged
index entry per updated row. The pattern "ORM writes back all columns" makes this the
common case, not an edge case. This is conflict reduction, not just I/O reduction, so it
bears directly on P1.

**Implementation:** the machinery already exists — populate P3 unconditionally (or
whenever `aRegIdx` is available) rather than only under `bHasExpr`. The runtime `memcmp`
then does the work. Cost is one `memcmp` per index per updated row against a saving of a
delete + insert.

**Risk:** low-to-medium. The correctness argument is that a byte-identical index record
means an identical entry, which is the same argument the expression-index case already
relies on. Needs care that `aRegIdx[i]` genuinely holds the new record for the ordinary
path. **Worth raising with Dan Kennedy directly** — he wrote the expression-index version
ten days ago and will know immediately whether the general case was omitted deliberately or
simply not yet done.

**Confidence:** high that the gap is real; medium that generalizing is as easy as it looks.

**Ready-made experiment, no code required:** Bedrock already ships `PRAGMA noop_update`
(`SQLITE_ENABLE_NOOP_UPDATE`, `update.c:468`), which rewrites every `SET` expression to
`+column` so an `UPDATE` writes each column its existing value — see
`08-custom-flags.md` §4. Running a representative write workload with it enabled produces
*only* the write amplification, since no value changes. Any conflicts and any index
delete/insert traffic observed under it are exactly what A2 would remove. This sizes the
prize before anyone writes a line of code.

## A4 — Give `ConflictLockGuard` an identifier with grouping power on HC-Tree

**Not a port from WAL2 — a repair of a Bedrock mechanism that WAL2's coarseness made work
and HC-Tree's precision broke.** Full analysis in `10-conflict-investigation.md` §6.

Bedrock damps conflict storms by making a retrying command take a mutex keyed on where it
last conflicted (`ConflictLockGuard.cpp`, `BedrockServer.cpp:686`). Under WAL2 that key is
a **page number**, which groups every command touching that page. Under HC-Tree it is
`hash(table + row key)` (`sqlitecluster/SQLite.cpp:432`), which groups essentially nothing
— and churns the 500-entry mutex LRU.

**Fix options, cheapest first:**

1. Key on `_conflictLocation` (table/index name) alone — one line, maximally coarse, may
   over-serialize hot tables.
2. **Key on `root=` plus a bucketed row key.** HC-Tree's conflict message already carries
   `(root=%lld)` (`hct_database.c:5869`); bucketing the key gives a tunable granularity
   between row and table, needs no upstream change, and the bucket width becomes a
   measurable knob. **Recommended.**
3. Key on a logical page — reproduces WAL2 exactly, but HC-Tree does not log a page number,
   so it needs an upstream message change.

**Gate:** confirm `-enableConflictPageLocks` is on in production first
(`BedrockServer.h:393` defaults it to false). If it is off, this becomes new headroom
rather than a regression to fix.

**Risk:** low — the change is confined to how one identifier is computed, and over-grouping
costs throughput rather than correctness. Needs measurement to tune bucket width.

**Confidence:** high that the asymmetry is real and material; the magnitude depends
entirely on the answer to the gate question.

## A3 — Cheap-first conflict detection

**WAL2 mechanism:** validation is an in-memory merge join over two sorted arrays
(`btreeBcDetectIntkeyConflict`, `btree.c:1361`) — no page loads, no shared-state lookups.

**HC-Tree gap:** validation physically re-traverses the tree and does a shared
`hctDbTMapLookup` per row (`hct_database.c:8191`, `hct_database.c:1002`).

**Idea:** HC-Tree could maintain, alongside its ranges, a compact sorted summary of *keys
written by transactions that committed since our snapshot*, and merge-join against that
first — falling back to physical re-traversal only when the cheap test says "possible
conflict". Most transactions would then validate without touching a page.

**Benefit:** potentially removes the dominant validation cost entirely for the common
no-conflict case, which is the case that matters (it is on the critical path of every
successful commit).

**Risk:** high — this is a design change, not a port. It requires a shared, concurrently
readable structure of recent commits, which is itself a 384-CPU contention question, and
HC-Tree's whole design avoids exactly such a structure. **Recorded as a direction, not a
recommendation.** Raise with Dan Kennedy before investing.

**Confidence:** low. Do not act on this without upstream buy-in.

---

# Part B — Beyond WAL2: ideas for 384 CPUs / 6 TB RAM

## B1 — Re-enable the `iLocalMinTid` fast path *(highest value if viable)*

**Current state:** commented out in the hot path (`hct_database.c:995`):

```c
if( iTid==pDb->iTid /* || iTid<=pDb->iLocalMinTid */ || iTid==LARGEST_TID ){
```

`iLocalMinTid` is documented (`hct_database.c:332-336`) as the TID below which every
transaction is known settled. Rows older than it cannot be conflicts and need no map
lookup.

**Benefit:** on a 6 TB database, nearly every row a transaction reads was written long
ago. Re-enabling this would eliminate the shared `hctDbTMapLookup` for the overwhelming
majority of validated rows — removing a shared-structure access from the per-row inner
loop of every commit across 384 cores. This is the single most promising contention fix
identified so far.

**Risk:** unknown, and the fact that it is commented out rather than absent means someone
disabled it deliberately. The neighbouring dead code (`hct_database.c:1013-1018`) discusses
a rollback subtlety that is the most likely reason. **Must ask Dan Kennedy before
touching.** Do not simply uncomment it.

**Confidence:** high value if safe; safety entirely unestablished.

## B2 — Raise the page cache to match the hardware

`SQLITE_DEFAULT_CACHE_SIZE=-51200` (`Makefile:18`) is 50 MiB, and Bedrock's `-cacheSize`
default is `0`, which means the runtime `PRAGMA cache_size` is never issued at all
(`main.cpp:329`, `sqlitecluster/SQLite.cpp:295`). See `12-bugs.md` #3 — the help text
claims 1 GB.

On a 6 TB-RAM host this is very likely leaving the working set out of cache. Cheapest
possible experiment: set `-cacheSize` explicitly and measure. **Pending confirmation of
what production actually passes.**

## B3 — Retune the HC-Tree mmap chunking for a 6 TB database

`hct_file.c`'s header comment explains that the file is grown in
`HCT_DEFAULT_PAGEPERCHUNK`-page chunks (2 MiB at 4 KiB pages) and mapped
`HCT_MMAP_QUANTA` chunks at a time, specifically to avoid exhausting Linux
`vm.max_map_count` (65530 default) — and it computes the maximum mappable database size
from those constants.

**At 6 TB this arithmetic needs checking directly**, both for whether we are near the
mapping limit and for whether the quanta are well-chosen when memory is abundant. Deferred
to unit 6 (`07-mmap.md`), flagged here so the optimization angle is not lost: raising
`vm.max_map_count` is a free sysctl change if the constants are the binding constraint.

## B4 — NUMA locality of the shared transaction map

A 384-CPU host is many sockets. `hctDbTMapLookup` is consulted per validated row by every
committing transaction (§B1), so the transaction map is both the hottest shared structure
and a single memory region with no NUMA awareness anywhere in `hct_tmap.c`.

If B1 cannot be re-enabled, sharding or replicating the map becomes the alternative.
**Speculative** — no measurement yet, and `hct_tmap.c` has not been read line by line
(unit 5). Recorded so it is not lost.

## B5 — Recover the mutex contention diagnostic

`SQLITE_MUTEX_ALERT_MILLISECONDS=20` is a dead flag (`12-bugs.md` #1) — it appears nowhere
in upstream or the amalgamation. Whatever it once enabled is gone. Given that every item
in Part B is a contention hypothesis, restoring a mutex-hold-time alert would convert
guesswork into measurement. Low effort, high diagnostic leverage.

## B7 — Take journal housekeeping off the write path

`SQLite::prepare()` runs `SELECT MIN(id) FROM <journal shard>` **on every transaction**
(`sqlitecluster/SQLite.cpp:952`) purely to decide whether to trim, and when trimming is due
issues `DELETE … WHERE id < N LIMIT 10` (`sqlitecluster/SQLite.cpp:969`) — both at the
*head* of the shard, both inside the transaction. See `10-conflict-investigation.md` §4.5.

**Two independent changes, both cheap:**

1. **Cache the min-id per shard in `_sharedData`** instead of reading it from the database
   inside every transaction. The value changes only when that shard is trimmed, which the
   process itself does — so it can be maintained in memory. Removes a database read, and
   its read-set entry, from every write transaction.
2. **Move trimming off the write path** into a background sweep. Journal trimming has no
   ordering relationship with the transaction that happens to trigger it; it is bolted onto
   `prepare()` for convenience. A background trimmer removes the head-region write from
   application transactions entirely.

**Benefit:** eliminates the only region of the journal where reads and writes coincide, and
removes one unconditional query from every write transaction. Helps both engines, but helps
HC-Tree more, because HC-Tree's validation cost scales with the read set.

**Risk:** low for (1) — it is a cache of a value this process controls. Medium for (2) —
needs care that trimming cannot race ahead of a reader that still needs those journal rows
(`_getJournalQuery` readers at `sqlitecluster/SQLite.cpp:1346`, `:1371`), and the existing
`shared_lock(_sharedData.writeLock)` suggests the locking is already considered.

**Confidence:** high that both are safe wins in principle; the magnitude depends on the
shard-collision rate, which is unmeasured.

## B6 — Compile in `HCT_VALIDATE_TIMERS`

Not an optimization but the measurement that ranks all of the above. See
`10-conflict-investigation.md` §7: HC-Tree already contains per-transaction, per-cursor and
per-op validation timing with step counts, behind `-DHCT_VALIDATE_TIMERS`. It directly
measures the quantity suspected to be the problem. **Do this first** — it costs a compile
flag and turns Part A/B from ranked guesses into ranked measurements.

---

## B8 — The page allocator is behind one global mutex

`hct_pman.c:103` gives `HctPManServer` a single `sqlite3_mutex` guarding the free-page
"baskets", entered via `hctPManMutexEnter()` (`hct_pman.c:144`) from every allocation path
(`hct_pman.c:420`, `:624`, `:723`).

The design already mitigates this: each client keeps local `aPgSet[2]` pools of
immediately-reusable physical and logical page ids and only takes the server mutex when
refilling or handing back a batch (`hct_pman.c:124-141`). So the mutex is amortized, not
per-page.

**Whether the amortization is sufficient at 384 CPUs is exactly what
`pman.mutex_block / pman.mutex_attempt` measures** — and that counter already exists
(`13-instrumentation.md`). If the ratio is high, the fixes are conventional: larger
per-client batches (cheap at 6 TB RAM), or sharding the server pool per NUMA node or per
worker.

**Do not act before measuring** — the batching may already make this a non-issue, and the
counter will say so in minutes.

## B9 — Raise `hct_npageset` (the allocator batch size)

**Default 256** (`HCT_DEFAULT_NPAGESET`, `hctInt.h:54`), settable at runtime via
`PRAGMA hct_npageset` (`hctree.c:3550`). Bedrock sets it nowhere.

This is how many free page ids a client batches locally before it must take the **single
global page-allocator mutex** (`hct_pman.c:103`) to refill or hand back — i.e. it is the
amortization factor for B8. Raising it reduces mutex traffic proportionally, at a cost of
memory that is free at 6 TB RAM.

**This is the cheapest possible response to B8** — a runtime pragma, no rebuild, trivially
reversible. Measure `pman.mutex_block / pman.mutex_attempt` from `hctstats` before and
after (`13-instrumentation.md`).

**Risk:** low. Larger batches mean more free page ids held per client and slightly lazier
reuse. **Confidence:** high that the mechanism works as described; the right value needs
measurement.

## B10 — Warm the mapping at server startup (`hct_prefault`)

`PRAGMA hct_prefault = N` launches N threads to fault the database mapping into RAM
(`hct_file.c:2505-2530`, `hctree.c`); a negative N restricts it to minor faults.

**Nothing currently warms the mapping during normal Bedrock startup.** Bedrock's `VMTouch`
is multi-threaded (`VMTouch.cpp:118`) but runs only in a standalone utility mode that
exits the process straight afterwards (`main.cpp:351-355`). So a freshly started node pays
page-fault latency on first touch of every page.

On a host where the database plausibly fits in RAM, warming it deliberately — with as many
threads as the box has — converts a long tail of cold-start latency into a bounded startup
cost. `hct_prefault` does exactly that, in-process, and is unused.

**Risk:** low, but it is startup-time and I/O-heavy; on a 6 TB file with cold cache it will
take a while and should be sized/measured rather than switched on blind. Note it applies to
HC-Tree only.

**Open question:** is the standalone-only use of `VMTouch` deliberate?

## Provisional ranking

Ordered by (value × confidence) ÷ risk, with the honest caveat that nothing here is
measured yet:

0. **Query `hctstats` in production and grep conflict logs for `journal*`.** Zero rebuild,
   zero risk, and it reorders everything below. See `13-instrumentation.md`.
1. **B6** — compile in `HCT_VALIDATE_TIMERS` and measure. Prerequisite for the code changes.
2. **B2** — check and fix the page cache size. Trivial, possibly large.
3. **A4** — fix the `ConflictLockGuard` identifier on HC-Tree, *if*
   `-enableConflictPageLocks` is on in production. Potentially the largest single win, and
   entirely in Bedrock's own code.
4. **A1** — sort/coalesce read ranges. Low risk, clear mechanism, no semantic change.
5. **B1** — re-enable `iLocalMinTid`. Highest ceiling, but blocked on a question to Dan
   Kennedy.
6. **A2** — generalize the no-op index rewrite skip. Directly reduces conflicts; needs
   upstream input.
7. **B7** — take journal housekeeping off the write path. Two cheap, self-contained
   changes in Bedrock's own code, no SQLite change needed.
8. **B9** — raise `hct_npageset`. Runtime pragma, no rebuild; the cheap answer to B8.
9. **B10** — warm the mapping at startup via `hct_prefault`.
10. **B5** — restore mutex alerting. Diagnostic.
11. **B8** — page-allocator mutex *sharding*, only if `hctstats` says it is contended
    **and** B9 does not resolve it.
12. **B3 / B4** — B3 deferred pending unit 6; B4 (NUMA) still speculative.
13. **A3** — design change; do not pursue without upstream agreement.

**Cross-cutting note added after unit 5:** Bedrock sets **none** of HC-Tree's ten tunable
pragmas (`06-readers-snapshots.md` §5), so the whole engine runs at upstream defaults
chosen without reference to a 384-CPU / 6 TB-RAM host. B9 and B10 are the two with the
clearest rationale, but the surface as a whole deserves a deliberate pass.
