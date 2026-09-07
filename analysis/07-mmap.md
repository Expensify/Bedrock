# 07 — mmap, `SQLITE_SHARED_MAPPING`, and 16 TiB map sizes per engine

**Unit:** 6
**Status:** COMPLETE
**Drift:** none. All code cited is identical in the vendored Aug-28 drop.

---

## Summary — and a correction to earlier units

**The two engines use entirely separate memory-mapping subsystems, and three pragmas
Bedrock issues on every connection are silent no-ops on HC-Tree.**

WAL2 goes through SQLite's pager, `os_unix.c`'s `unixMapfile()`, `SQLITE_MAX_MMAP_SIZE` and
`SQLITE_SHARED_MAPPING`. HC-Tree has **no pager at all** and calls `mmap()` directly with
its own chunking constants.

| Pragma Bedrock issues | WAL2 | HC-Tree |
|---|---|---|
| `PRAGMA mmap_size` (`SQLite.cpp:288`) | effective | **no-op** — `sqlite3HctBtreeSetMmapLimit()` is `return SQLITE_OK;` (`hctree.c`) |
| `PRAGMA cache_size` (`SQLite.cpp:297`) | effective | **no-op** — `sqlite3HctBtreeSetCacheSize()` is `/* no-op in hct */ return SQLITE_OK;` |
| `PRAGMA synchronous` / `SQLITE_DEFAULT_WAL_SYNCHRONOUS=0` | effective | **no-op** — `sqlite3HctBtreeSetPagerFlags()` is `/* HCT - does this need fixing? */ return SQLITE_OK;` |

The third carries the upstream author's own uncertainty in a comment, which is worth
flagging rather than glossing.

**This forces corrections to two earlier conclusions:**

- **`12-bugs.md` #3** (the 50 MiB page cache) is **WAL2-only**. HC-Tree has no page cache to
  size. Corrected in that file.
- **`11-portable-optimizations.md` B2** ("raise the page cache to match the hardware") is
  therefore **out of scope** under Dan's P2 framing — it would improve WAL2, which we
  explicitly do not care about. Downgraded there.

The good news is that the corresponding HC-Tree knobs *do* exist and are untouched — they
are just different ones (`hct_npageset`, `hct_prefault`, `hct_npagescan`; see
`06-readers-snapshots.md` §5).

---

## 1. WAL2 side: the pager's mapping, shared per inode

Bedrock sets `SQLITE_MAX_MMAP_SIZE=17592186044416` (16 TiB, `Makefile:18`) and issues
`PRAGMA mmap_size=<mmapSizeGB>GiB` per connection
(`sqlitecluster/SQLite.cpp:288`). This drives `unixMapfile()` in `os_unix.c`.

`SQLITE_SHARED_MAPPING` (analysed in `08-custom-flags.md` §5) moves the mapped region from
the per-file `unixFile` onto the per-inode `unixInodeInfo` (`os_unix.c:1352`), so **all
connections in the process share one mapping** rather than one each. Released only when the
inode refcount hits zero (`os_unix.c:1512-1518`); `unixUnmapfile()` early-returns while
`pInode` is set (`os_unix.c:6311`).

**At Bedrock's scale this is the difference between one 6 TB mapping and ~384 of them.**
Address space is not the binding constraint on 64-bit, but page-table setup, `mmap`/`munmap`
churn, and per-mapping kernel bookkeeping all are. Sharing is clearly correct here.

**The catch already noted:** the `SQLITE_FCNTL_MMAP_SIZE` handler skips the remap once an
inode mapping exists (`os_unix.c:4274-4279`), so the **first** connection to map the file
fixes the size for the whole process and later `PRAGMA mmap_size` calls are inert. Benign
today because Bedrock passes the same value everywhere — but runtime mmap-size tuning
experiments will silently do nothing, which is exactly the sort of thing that wastes an
afternoon.

## 2. HC-Tree side: its own mapping, its own arithmetic

HC-Tree has no pager. `sqlite3HctBtreePager()` returns `p->pFakePager`, which is a
**4096-byte zeroed allocation** (`hctree.c:761`):

```c
    pNew->pFakePager = (Pager*)sqlite3HctMallocRc(&rc, 4096);
```

— a placeholder so callers expecting a `Pager*` do not crash. (Any code that actually
dereferenced its fields would read zeros; nothing appears to, but it is a latent hazard
worth knowing about. Logged as `12-bugs.md` #6, low severity.)

Instead, `hct_file.c` calls `mmap()` directly (`hct_file.c:713`, `MAP_SHARED`), mapping
the pagemap file in one call (`hct_file.c:921`) and each data file in one call per file
(`hct_file.c:930`).

### The chunking constants and the mapping-count ceiling

The design is documented at `hct_file.c:31-44` and the reasoning is worth restating because
it is the one place either engine reasons explicitly about very large databases:

```c
#define HCT_DEFAULT_PAGEPERCHUNK  512
#define HCT_MMAP_QUANTA          1024
```

- chunk = 512 pages × 4096 B = **2 MiB** — the unit the file is extended and managed in
- mapping = `HCT_MMAP_QUANTA` × chunk = 1024 × 2 MiB = **2 GiB** per `mmap()` call
- Linux `vm.max_map_count` defaults to **65530** total mappings per process
- mappings are needed for **both** the database and the page-map, so ~32765 are available
  for the data

giving the comment's figure:

```
32765 * 1024*512*4096 bytes   ≈ 64 TiB
```

**The arithmetic checks out.** 32765 × 2 GiB = 65530 GiB ≈ 64 TiB.

### Where a 6 TB database sits

6 TB ÷ 2 GiB per mapping ≈ **3,000 mappings** for the data files. Doubling for the page-map
gives ~6,000 against a 65,530 limit — roughly **5–9 % of the ceiling**, with ~10× headroom
before 64 TiB becomes a concern.

**Conclusion: we are not close to the mapping limit, and this is not a current risk.** Worth
stating plainly because the `hct_file.c` comment reads alarmingly and could easily be
mistaken for a live constraint.

**One thing to actually check:** `vm.max_map_count` is a *tunable* that some environments
lower, and other things in the process consume mappings too (every shared library, thread
stack, and the WAL2 side's mappings if any WAL2 databases are open in the same process).
The 65530 assumption should be verified on the production host rather than assumed —
`cat /proc/sys/vm/max_map_count`, and `wc -l /proc/<pid>/maps` on a running node for the
actual count. It is a one-line check and it converts an assumption into a fact.

### Prefaulting

`hct_file.c:2558` notes the concern about page tables for the mapped region, and
`PRAGMA hct_prefault = N` (`hct_file.c:2505-2530`) spawns N threads to fault the mapping
in. Covered as B10 in `11-portable-optimizations.md` — currently unused, and nothing else
warms the mapping at server startup (`06-readers-snapshots.md` §5).

## 3. Comparison at Expensify's scale

| | WAL2 | HC-Tree |
|---|---|---|
| Mapping owner | SQLite pager via `os_unix.c` | `hct_file.c` directly |
| Shared across connections | yes, via `SQLITE_SHARED_MAPPING` (per inode) | yes, inherently — one `HctFileServer` per database per process |
| Size control | `SQLITE_MAX_MMAP_SIZE` + `PRAGMA mmap_size` | **neither applies**; governed by `PAGEPERCHUNK` × `MMAP_QUANTA` |
| Granularity | one mapping of the whole file | 2 GiB mappings, 2 MiB chunks |
| Ceiling | file size / address space | ~64 TiB from `vm.max_map_count` |
| Page cache above the mapping | yes (`PRAGMA cache_size`) | **none** — the mapping *is* the cache |
| Warming | none in Bedrock (`VMTouch` is a standalone mode) | `hct_prefault`, unused |

**The most consequential difference is the last two rows.** On WAL2 there are two layers —
an mmap and a page cache on top of it — and Bedrock currently sizes the cache at 50 MiB by
default (`12-bugs.md` #3). On HC-Tree there is one layer: the mapping, backed by the OS page
cache, with the kernel doing the eviction. **For a host with 6 TB of RAM and a database of
comparable size, HC-Tree's single-layer model is the better fit** — it lets the OS use all
available memory without SQLite second-guessing it, and removes a tuning parameter that is
currently set wrong.

That is a genuine, if unglamorous, architectural advantage for HC-Tree on this hardware, and
it is independent of the conflict question.

## 4. Open questions

1. What is `vm.max_map_count` on the production hosts, and how many mappings does a running
   node actually hold? (§2 — one-line check.)
2. Since `PRAGMA mmap_size` is inert on HC-Tree, does `-mmapSizeGB` still serve a purpose
   for HC-Tree nodes, or is it dead configuration there?
3. For Dan Kennedy: `sqlite3HctBtreeSetPagerFlags()` carries the comment
   `/* HCT - does this need fixing? */`. What durability control, if any, is intended for
   HC-Tree — i.e. what is HC-Tree's equivalent of `PRAGMA synchronous`?
