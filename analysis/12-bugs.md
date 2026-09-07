# 12 — Bugs, vulnerabilities and maintenance hazards (living document)

**Priority:** P3 (Dan, 2026-09-07) — "any other bug or vulnerability you come across"
**Status:** open, appended during every unit

Each entry records: what it is, `file:line`, the failure scenario, severity, and which
build it affects (vendored Aug-28 drop / check-in `eedd80c1a9749300` / both / Bedrock
itself).

Severity scale: **high** = silent wrong behaviour or data risk; **medium** = lost
capability, performance cliff, or a trap that will bite on the next change; **low** =
cosmetic or defensive.

---

## #1 — `SQLITE_MUTEX_ALERT_MILLISECONDS=20` is a dead flag

**Severity:** medium
**Affects:** Bedrock build configuration
**Location:** `Makefile:18` (`AMALGAMATION_FLAGS`)

The flag is defined in Bedrock's build and referenced **nowhere else in existence**:

- Not in the upstream checkout: `grep -rn MUTEX_ALERT` over the whole
  `hctree-bedrock-lcd-ex @ eedd80c1a9749300` tree → **0 matches**.
- Not in the vendored amalgamation: `grep -c MUTEX_ALERT libstuff/sqlite3.c` → **0**.
- Not in Bedrock's own C++: the only hit anywhere in the repo is the `Makefile` line that
  defines it.

So the compiler defines a macro that no `#ifdef` ever tests. Whatever mutex-contention
alerting this once switched on is **not compiled into production**.

**Why it matters beyond tidiness:** this flag exists because someone previously cared
about mutex hold times — exactly the failure mode expected on a 384-CPU host (P4). The
diagnostic that would tell us which mutex is hot has been silently lost, probably during
an upstream re-vendor that dropped the patch implementing it. We are flying blind on the
single most likely scaling bottleneck.

**Action:** decide whether to (a) delete the flag as dead, or (b) recover the original
mutex-alert patch and re-apply it. Given P4, (b) is more valuable. **Question for Dan:**
was there ever a local mutex-alert patch to `sqlite3.c`, and does anyone have it? It is
not in the current amalgamation, and `00-provenance.md` establishes the amalgamation
carries no local patches at all — so if it existed, it is already gone.

---

## #2 — Amalgamation regeneration procedure is unrecorded, and one option is load-bearing

**Severity:** medium
**Affects:** Bedrock maintenance
**Location:** `Makefile:18`; no documented regeneration step anywhere in the repo

`SQLITE_ENABLE_UPDATE_DELETE_LIMIT` must be passed at **amalgamation-generation** time
(`./configure --enable-update-limit`), because Lemon bakes the grammar into `parse.c`.
Bedrock lists it among `AMALGAMATION_FLAGS`, which are otherwise `-D` compile flags — so
the name suggests it is sufficient to pass it to the compiler. It is not.

**Failure scenario:** an engineer re-vendors SQLite from a fresh checkout, runs
`./configure && make sqlite3.c` (the obvious commands), and commits the result. The build
succeeds with no warning. `-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT` is still on the compile
line and still does nothing useful, because the generated parser no longer has the rules.
Every `UPDATE … LIMIT` / `DELETE … LIMIT` statement in Bedrock and its plugins begins
failing as a **syntax error** at runtime — not at build time.

Detected here because the first generation attempt produced 13 spurious `parse.c` hunks
and a dropped `#define SQLITE_UDL_CAPABLE_PARSER 1`.

**Action:** commit the exact regeneration recipe (see `00-provenance.md`) into the repo
next to `libstuff/sqlite3.c`, and/or add a build-time assertion that
`SQLITE_UDL_CAPABLE_PARSER` is defined whenever `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` is.
The latter turns a silent runtime regression into a compile error and costs three lines.

---

## #3 — `-cacheSize` help text contradicts its actual default; real default is 50 MiB on a 6 TB-RAM host

**Severity:** low-medium — **WAL2 only** (see the correction at the end of this entry)
**Affects:** Bedrock; WAL2 databases only
**Location:** `main.cpp:250`, `main.cpp:329`, `sqlitecluster/SQLite.cpp:295-297`,
`Makefile:18`, `configs/bedrock.conf:12`

The documented and actual defaults disagree, and the actual one is very small for the
hardware.

- `main.cpp:250` — help text: `-cacheSize <kb>  number of KB to allocate for a page cache
  (defaults to 1GB)`
- `main.cpp:329` — `SETDEFAULT("-cacheSize", SToStr(0));` — the real default is **0**
- `sqlitecluster/SQLite.cpp:295` — `if (_cacheSize) {` … so **0 means the `PRAGMA
  cache_size` is never issued at all**
- therefore the effective default is the compile-time
  `SQLITE_DEFAULT_CACHE_SIZE=-51200` (`Makefile:18`), i.e. **50 MiB** per connection

So the help promises 1 GB, the code delivers 50 MiB, and nothing reconciles them. The
shipped sample config makes it smaller still: `configs/bedrock.conf:12` sets
`CACHE_SIZE="-cacheSize 10000"` → **10 MiB**.

**Why it matters:** on a 384-CPU / 6 TB-RAM host this is the difference between a page
cache that holds the working set and one that thrashes. It is also multiplied by the
connection-pool size (`_dbPoolSize`, `BedrockServer.cpp:115`), so the right number is not
obvious — but 10–50 MiB per connection is almost certainly far below optimum here.

**Not yet verified:** the production config is not in this repo, so production may pass an
explicit `-cacheSize`. **Question for Dan:** what does production actually set? If it
inherits the default, this is likely the cheapest available win (P4).

**CORRECTION (unit 6): this affects WAL2 only.** `PRAGMA cache_size` is a **no-op on
HC-Tree** — `sqlite3HctBtreeSetCacheSize()` is `/* no-op in hct */ return SQLITE_OK;`
(`hctree.c`). HC-Tree has no pager and no page cache; it maps the file directly
(`hct_file.c:713`) and lets the OS page cache do the work. See `07-mmap.md` §1.

So on HC-Tree nodes this setting is inert and the severity drops to a documentation bug.
On WAL2 nodes it remains a real performance question. Given Dan's P2 framing — improve
HC-Tree, not WAL2 — the *performance* half of this is out of scope; the misleading help
text is still worth a one-line fix.

**Action:** fix the help text, or better, make the default match it. Note the same
no-op applies to `PRAGMA mmap_size` and `PRAGMA synchronous` on HC-Tree.

---

## #6 — HC-Tree's `pFakePager` is a raw zeroed buffer cast to `Pager*`

**Severity:** low (latent; no known reachable path)
**Affects:** both builds; HC-Tree only
**Location:** `src/hctree.c:761`, returned by `sqlite3HctBtreePager()`

HC-Tree has no pager, but the btree API requires `sqlite3BtreePager()` to return one. The
implementation returns a 4096-byte zeroed heap allocation cast to `Pager*`:

```c
    pNew->pFakePager = (Pager*)sqlite3HctMallocRc(&rc, 4096);
```

Any caller that dereferences a field of the returned pointer reads zeros rather than
crashing on NULL — which is arguably worse, since a NULL would fail loudly. The 4096-byte
size appears to be "comfortably larger than `sizeof(Pager)`" rather than a computed bound;
if `Pager` ever grew past 4096 bytes, a field read would run off the allocation.

**No reachable misuse identified** — this is recorded as a latent hazard, not a live bug.
Two cheap hardenings if upstream is amenable: use `sizeof(Pager)` instead of a literal, or
return NULL and fix the callers that cannot accept it.

---

---

## #4 — HC-Tree log files are never unlinked at runtime (both delete paths disabled)

**Severity:** medium (disk-space leak)
**Affects:** both the vendored Aug-28 drop and check-in `eedd80c1a9749300`; HC-Tree only
**Location:** `src/hct_log.c:268`, `src/hct_log.c:444` (upstream)

Both code paths that would remove an HC-Tree log file are disabled in the source.

`sqlite3HctLogClose()` (`hct_log.c:245`) computes `bDefer` to decide whether removal is
safe now — in `FOLLOWER` mode it defers while the log holds transactions newer than the
journal's safe CID — and then never removes anything (`hct_log.c:266-269`):

```c
      if( p->zPath && bDefer==0 ){
        // unlink(p->zPath);
        sqlite3_free(p->zPath);
      }
```

The deferred path is disabled too, by an unconditional `&& 0` (`hct_log.c:444`):

```c
        if( bUnlink && 0 ) unlink(pFile->zPath);
```

**Failure scenario:** each HC-Tree connection creates an `HctLog` holding two log files
(`hctree.c:775`) and releases it on close (`hctree.c:847`). Because neither release path
deletes, the files persist. The only cleanup is at process start —
`hctFileServerInitUnlinkLog()` (`hct_file.c:786`) via `hctFileFindLogs()`
(`hct_file.c:1033`). A long-running node therefore accumulates two files per
connection-close for its entire uptime, and only reclaims them on restart.

**Magnitude depends on connection churn, not transaction rate.** Bedrock pools connections
(`SQLitePool`, `BedrockServer.cpp:115`), so a steady pool leaks little; churn — including
`SQLite`'s copy constructor, which opens a fresh handle
(`sqlitecluster/SQLite.cpp:346`) — leaks proportionally. **Not yet measured against
production behaviour**; the check is simply counting log files in the database directory on
a long-uptime node.

Both suppressions have the shape of deliberate temporary debugging changes that were never
reverted (a commented-out call and an `&& 0`). **Question for Dan Kennedy:** intentional?

**Update (unit 8): the leak is active in exactly Bedrock's configuration, not merely
theoretical.** The `bDefer` branch is only taken in `FOLLOWER` mode, and Bedrock does not
use HC-Tree's replication at all — a grep for `SQLITE_HCT_` / `sqlite_hct_journal` across
Bedrock's C++ returns nothing, so HC-Tree runs in `NORMAL` mode
(`09-multiprocess.md` §5.2). Every close therefore takes the `bDefer==0` path — the one
whose `unlink()` is commented out. Note also that Bedrock's `-clean` / `-bootstrap` reset
removes the database, `-pagemap`, `-wal`, `-wal2` and `-shm` (`main.cpp:340-347`) but
**not** HC-Tree log files.

---

## #5 — `WAL2NOCKSUM` trades data checksums for write ordering, but `synchronous=0` removes the ordering guarantee

**Severity:** medium — silent corruption *only* on machine crash or power loss, and
partially mitigated by Bedrock's own hash chain. Not a process-crash risk.
**Affects:** both builds; WAL2 databases only
**Location:** `Makefile:18` (`-DSQLITE_ENABLE_WAL2NOCKSUM`, `-DSQLITE_DEFAULT_WAL_SYNCHRONOUS=0`),
`src/wal.c:1275`, `:1331`, `:5217-5228`, `:5405`, `:5471`

With `SQLITE_ENABLE_WAL2NOCKSUM`, `isNocksum(pWal)` is true for every wal2-mode database
(`wal.c:476`), and the running frame checksum then covers **only the first 8 bytes of the
frame header** — the page number and truncate size. The page body is excluded, in both the
encoder (`wal.c:1275`) and the validator (`wal.c:1331`):

```c
  walChecksumBytes(nativeCksum, aFrame, 8, aCksum, aCksum);
  if( isNocksum(pWal)==0 ){
    walChecksumBytes(nativeCksum, aData, pWal->szPage, aCksum, aCksum);
  }
```

The integrity substitute is **write ordering**: in nocksum mode `walWriteOneFrame()` writes
the page data *first* and the frame header *afterwards* (`wal.c:5217-5228`), inverting the
normal order, so that a header's presence is meant to imply its data was already written.

**The problem:** nothing enforces that ordering to durable media. `walWriteToLog()` syncs
only when a write crosses `iSyncPoint` (`wal.c:5175-5183`), `w.iSyncPoint` is initialised
to `0` (`wal.c:5405`), and the only place it is set to something meaningful is the commit
path, guarded by `if( isCommit && WAL_SYNC_FLAGS(sync_flags)!=0 )` (`wal.c:5471`). With
`SQLITE_DEFAULT_WAL_SYNCHRONOUS=0` those sync flags are absent, so **no fsync is issued on
the WAL at all** and the kernel may write back the header and the data in either order.

**Failure scenario:** power loss or kernel panic after the frame header has reached storage
but before (or during) the page body. On restart, `walDecodeFrame()` validates the header
chain, finds it consistent, and — because the body is not covered by any checksum —
**accepts the frame and applies whatever bytes are in the page slot**. Instead of a clean
truncation at the last good frame, recovery silently applies a partially-written or stale
page.

**Qualifications, which matter:**

- This is **not** a process-crash risk. If only `bedrock` dies, the page cache retains the
  writes and recovery reads exactly what was written.
- `synchronous=0` already means recent transactions are lost on power loss; that is an
  accepted, deliberate trade. The *additional* exposure from `WAL2NOCKSUM` is that the loss
  boundary may be **silently wrong** rather than cleanly detected.
- Bedrock maintains its own hash chain in the journal (`INSERT INTO <journal> VALUES
  (:commitID, :query, :hash)`, `sqlitecluster/SQLite.cpp:1070`) and is a replicated
  cluster, so a corrupted node has a plausible detection path above SQLite. **Whether that
  detection actually runs on recovery is not verified here** and is the thing worth
  checking.

**Action:** confirm the intended durability contract. If the answer is "we accept losing
recent commits but must never apply a corrupt page", the combination of `NOCKSUM` +
`synchronous=0` does not deliver that, and either the checksum or a barrier before the
header write is needed. Worth putting to Dan Kennedy, since the write-inversion in
`wal.c:5217-5228` is clearly a designed mechanism and he will know what durability level it
assumes.

---

*(further entries appended as units proceed)*
