# 13 — Instrumentation already available (no code changes needed)

**Status:** COMPLETE for the discovery pass
**Why this file exists:** several findings in `10-conflict-investigation.md` and
`11-portable-optimizations.md` are ranked guesses that could be turned into measurements
cheaply. It turns out HC-Tree ships a substantial diagnostic surface that is **already
compiled into production and queryable today**. This is the fastest route from "plausible"
to "known", so it is written up separately and ranked first.

**Drift:** none. All of this is identical in the vendored Aug-28 drop.

---

## 1. `SELECT * FROM hctstats` — available right now, no rebuild

`hct_stats.c` implements an eponymous virtual table with schema:

```sql
CREATE TABLE x(subsys, stat, val)      -- hct_stats.c:47
```

It is registered unconditionally for every HC-Tree connection, alongside `hctvalid`,
`hctfile`, `hctpman`, `hctjrnl` and `hcttmap` (`hct_database.c:9968`,
`sqlite3HctStatsInit(db)`). **No compile flag gates it.** Any HC-Tree database Bedrock has
open can be queried today.

Five subsystems report (`aHctStatGlobal[]`, `hct_stats.c:39`): `file`, `db`, `tmap`,
`pman`, `hct`.

### The complete counter list, and what each one settles

| subsys | stat | Why it matters here |
|---|---|---|
| `pman` | `mutex_attempt`, `mutex_block` | **H3 directly.** The page allocator has a *single global mutex* (`hct_pman.c:103`) and `hctPManMutexEnter()` (`hct_pman.c:144`) does `sqlite3_mutex_try()` first, counting a block only when it fails. `mutex_block / mutex_attempt` is therefore a clean contention ratio for the allocator. |
| `tmap` | `mutex_attempt`, `mutex_block` | Same ratio for the **transaction map** — the structure §4.1 says is consulted per validated row. If this is contended, B1 (re-enabling `iLocalMinTid`) is confirmed as high-value. |
| `file` | `mutex_attempt`, `mutex_block` | Third mutex contention ratio, file layer. |
| `file` | `cas_attempt`, `cas_fail` | **Lock-free contention.** Failed compare-and-swaps are retried spins; `cas_fail / cas_attempt` rising with core count is the classic 384-CPU signature that a mutex ratio would miss. |
| `file` | `incr_attempt`, `incr_fail` | Same for atomic increments. |
| `file` | `get_page`, `get_physical` | Page-fetch volume — denominator for the above. |
| `file` | `growmapping_cycles`, `truncatedb_cycles`, `truncatepagemap_cycles` | Spin cycles in mapping growth and truncation. Relevant to the 16 TB mmap question (unit 6). |
| `db` | **`tmap_lookup`** | **The single most valuable counter for P1.** This counts exactly the calls that `hctDbTidIsConflict()` makes into the shared transaction map — the work that the commented-out `iLocalMinTid` fast path (`hct_database.c:995`) would eliminate. A high value directly sizes the prize for B1. |
| `db` | `internal_retry`, `descend_in_writewrite` | Internal retry volume; `descend_in_writewrite` counts descents in the write/write conflict path (`hct_database.c:5975+`). |
| `db` | `update_in_place` | How often an update avoids a structural change. Relevant to A2 (no-op index rewrites). |
| `db` | `balance_*` (9 counters) | Page balance/split/merge volume: `balance_intkey_leaf`, `balance_index_leaf`, `balance_intkey_node`, `balance_index_node`, `balance_single`, `balance_deleteleftkey`, `balance_underfull`, `balance_overfull`, `defragment`. Structural write amplification. |
| `db` | `load_physical_to_free_ovfl` | Overflow-page handling volume. |
| `hct` | **`nretry`, `nretrykey`, `nkeyop`** | Transaction-level retry counts — the closest thing to a native "conflict rate" from inside the engine, and therefore the right cross-check against Bedrock's own conflict metric (see `10-conflict-investigation.md` §5, where the two engines are shown to log conflicts under *different* result codes). |

### Bedrock already logs this — check existing logs before instrumenting anything

`SQLite::commit()` already dumps the whole table on slow HC-Tree commits
(`sqlitecluster/SQLite.cpp:1181-1188`):

```cpp
if (_commitElapsed > 100'000 && _hctree) {
    SQResult stats;
    if (read("SELECT * FROM hctstats", stats)) {
        for (const auto& row : stats) {
            SINFO("slow HC-Tree commit", {{"hctstats", SComposeList(row)}});
        }
    }
}
```

So for every HC-Tree commit taking over 100 ms, production logs already contain a full
counter snapshot tagged `slow HC-Tree commit`. **The data needed to settle H3 and to size
B1 may already be sitting in the log archive.** Searching for that tag is the single
cheapest next step in this entire analysis — no query, no rebuild, no deploy.

Two caveats: the sample is biased to slow commits by construction (which is arguably the
population of interest), and the counters are cumulative, so deltas between successive
snapshots are what carry meaning.

### Recommended first query

```sql
SELECT subsys, stat, val FROM hctstats ORDER BY subsys, stat;
```

Sample it twice, some minutes apart, under production load and take deltas — the counters
are cumulative. The three ratios to compute:

1. `pman.mutex_block / pman.mutex_attempt`
2. `tmap.mutex_block / tmap.mutex_attempt`
3. `file.cas_fail / file.cas_attempt`

Any of these materially above zero on a 384-CPU host confirms H3 and reorders the whole
optimization list. `db.tmap_lookup` per transaction sizes B1.

**Caveat to check before trusting absolute numbers:** `HctPManStats` lives on
`HctPManClient` (`hct_pman.c:141`), i.e. **per connection**, not global. Whether
`sqlite3HctPManStats()` aggregates across clients or reports only the querying connection's
own counters is **not yet verified** — it changes how to read the numbers, not whether they
are useful. Verify before publishing figures.

---

## 2. `HCT_VALIDATE_TIMERS` — needs a rebuild, settles §2 directly

Compile-time only (`hct_database.c:7998-8090`). Logs slow validations at three
granularities via `sqlite3_log()`:

- `hctDbValidateWarning()` — per transaction: total µs, intkey op count, point-lookup
  count, index op count
- `hctDbValidateCsrWarning()` — per cursor, with root page
- `hctDbValidateOpWarning()` — per op, with **`nStep`** — the number of rows actually
  stepped during re-validation

Thresholds: `HCT_VALIDATE_THRESHOLD`, `HCT_VALIDATE_CSR_THRESHOLD`,
`HCT_VALIDATE_OP_THRESHOLD`.

`nStep` is precisely the quantity `10-conflict-investigation.md` §2 argues is HC-Tree's
structural disadvantage against WAL2's merge join. If validation times are large **and**
dominated by high `nStep`, §2 is confirmed and A1 (range coalescing) plus B1 become the fix
list. If `nStep` is small, §2 is wrong and attention should move to the journal-head
conflict (§4.5) instead.

Add `-DHCT_VALIDATE_TIMERS` to `AMALGAMATION_FLAGS`. **Note:** this is a genuine
compile-time flag, unlike `SQLITE_MUTEX_ALERT_MILLISECONDS`, which is dead
(`12-bugs.md` #1).

---

## 3. Conflict log messages already carry the answer to "which table?"

Both engines already log every conflict with the object name resolved, so the *distribution*
of conflicts across tables is obtainable from logs alone — no code change:

- HC-Tree write/write: `hctDbLogWriteConflict()` (`hct_database.c:5856`) —
  `"write/write conflict on %s %s%s%s (root=%lld), key=%s, conflicting=(%lld)%s (mytid=%lld)"`
- HC-Tree read/write: `hctDbLogReadConflict()` (`hct_database.c:5883`) and
  `hctDbSetCannotCommit()` (`hct_database.c:5905`), same shape
- WAL2: `btree.c:1391` —
  `"cannot commit CONCURRENT transaction - conflict in table %s - range (%lld,%lld) conflicts with write to rowid %lld"`

All resolve the root page to a table (and index) name — HC-Tree via `hctDbFindObject()`,
WAL2 via `btreeBcRootToObject()`.

**This is the direct test of the §4.5 journal-head finding:** count what fraction of
conflict log lines name a `journal*` table. If it is large, B7 (take journal housekeeping
off the write path) is the highest-value fix available and needs no upstream involvement.

**Bedrock already parses these messages.** `SQLite::_sqliteLogCallback()`
(`sqlitecluster/SQLite.cpp:411-435`) extracts the table/index name into
`_conflictLocation` for *both* engines and exposes it via `getLastConflictLocation()`. So
the conflict-by-table distribution is available from existing production logs — see
`10-conflict-investigation.md` §6, which also shows the identifier derived alongside it
means different things on the two engines.

**Two traps when doing this:**

1. The engines log under **different result codes** — HC-Tree uses
   `sqlite3_log(SQLITE_BUSY_SNAPSHOT, …)` (517), WAL2 uses `sqlite3_log(SQLITE_OK, …)` (0).
   Bedrock's own parser keys on the message prefix rather than the code and so handles
   both, but any *manual* log filter keyed on the code will silently see only one engine.
2. HC-Tree's messages are also emitted from the *eager* dooming path
   (`hctDbSetCannotCommit`), which has no WAL2 equivalent, so raw counts are not
   like-for-like. See `10-conflict-investigation.md` §5.

---

## 4. Other virtual tables registered alongside `hctstats`

Registered in the same block (`hct_database.c:9955-9976`) and not yet explored — noted so
later units know they exist: `hctvalid`, `hctfile` (`sqlite3HctFileVtabInit`), `hctpman`
(`sqlite3HctPManVtabInit`), `hctjrnl` (`sqlite3HctJrnlInit`), `hcttmap`
(`sqlite3HctTmapVtabInit`). The `hcttmap` table in particular is likely to expose the
snapshot/GC horizon that unit 5 (readers and snapshots) needs.

---

## Recommended order of operations

1. **Query `hctstats` in production.** Zero risk, zero rebuild, immediately reorders the
   optimization list. Do this first.
2. **Grep conflict logs for `journal*`.** Also zero-cost, and directly tests the
   highest-confidence finding (§4.5).
3. **Rebuild with `-DHCT_VALIDATE_TIMERS`** on one node and collect `nStep` distributions.
4. Only then start changing code.
