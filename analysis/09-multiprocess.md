# 09 — Multi-process access: HC-Tree's single-process constraint

**Unit:** 8
**Status:** COMPLETE
**Drift:** none. All code cited is identical in the vendored Aug-28 drop.

---

## Summary

**HC-Tree is strictly single-process, and it enforces this safely.** All cross-connection
coordination lives in **process-global heap memory**, not in a shared-memory file. A second
process attempting to open the same database is rejected with `SQLITE_BUSY` by an exclusive
`fcntl` lock — it does not silently corrupt.

The operational consequence is absolute: **no out-of-process access to a live HC-Tree
database, not even read-only.** No `sqlite3` shell, no backup tool, no monitoring query, no
ad-hoc investigation — everything must go through the running Bedrock process.

WAL2 keeps genuine multi-process support via the `-shm` wal-index, but Bedrock's custom
compile flags impose a compatibility requirement of their own (`08-custom-flags.md` §7):
any companion tool must be built with the *exact* flag set or it cannot attach.

---

## 1. HC-Tree: coordination lives in a process-global static

The shared object for a database is `HctFileServer`. All instances live on a linked list
hanging off a **C file-scope static** (`hct_file.c:107-120`):

```c
static struct HctFileGlobalVars {
  HctFileServer *pServerList;
  ...
} g;
```

Connections find each other by matching the file's identity in that in-process list
(`hct_file.c:1386-1395`):

```c
  if( 0==stat(zFile, &sStat) ){
    for(pServer=g.pServerList; pServer; pServer=pServer->pServerNext){
      if( pServer->st_ino==(i64)sStat.st_ino
       && pServer->st_dev==(i64)sStat.st_dev
        ){
        break;
      }
    }
  }
```

Everything that makes HC-Tree work hangs off that object: the transaction map
(`HctTMapServer`, whose `pClientList` and `iMinMinTid` are plain pointers and integers,
`hct_tmap.c:126-133`), the page manager (`HctPManServer`, `hct_pman.c:103`), and the
journal (`sqlite3HctFileGetJrnlPtr()`). None of it is in a mapped file; all of it is heap
memory reachable only within one address space.

**So two processes opening the same HC-Tree database would each build a private
`HctFileServer` and be completely blind to one another** — separate transaction maps,
separate page allocators, separate GC horizons. Concurrent writes would destroy the
database.

## 2. The lock that prevents it — clean failure, not corruption

When a process creates the first `HctFileServer` for a database, it takes an **exclusive
POSIX write lock** on a single byte at 1 MiB into the file (`HCT_LOCK_OFFSET = 1024*1024`,
`HCT_LOCK_SIZE = 1`, `hct_file.c:49-50`). `hctFileLock()` (`hct_file.c:638`):

```c
    l.l_type = F_WRLCK;
    l.l_start = HCT_LOCK_OFFSET;
    l.l_len = HCT_LOCK_SIZE;
    res = fcntl(fd, F_SETLK, &l);
    if( res!=0 ){
      fcntl(fd, F_GETLK, &l);
      sqlite3_log(SQLITE_BUSY, "hct file \"%s\" locked by process %lld",
          zFile, (i64)l.l_pid);
      *pRc = SQLITE_BUSY;
    }
```

It is called from exactly one site (`hct_file.c:1401`), unconditionally, on first open in
the process.

**This is good design and worth crediting:** the failure is immediate, explicit, and the
log message names the PID holding the lock. A second Bedrock instance, a stray `sqlite3`
shell, or a backup script gets `SQLITE_BUSY` and a diagnosable message rather than silent
divergence.

**Caveat worth knowing (not verified as a live problem):** POSIX `fcntl` locks are
per-process and are released when the process closes *any* file descriptor referring to
that file. Code that opens the database file for an unrelated purpose (a stat, a copy, a
size check) and then closes that descriptor can drop the lock while the database is still
open. This is the classic POSIX-lock footgun, and it is why SQLite's own `os_unix.c`
maintains `unixInodeInfo` with `closePendingFds()`. HC-Tree's lock does not appear to have
equivalent protection. **Not established as reachable in Bedrock**, but worth a look if
anyone adds file-touching tooling to the process.

## 3. There is a latent read-only mode, and it is switched off

`HctFileServer` carries `bReadOnlyMap` (`hct_file.c:250`, "True for a read-only mapping of
db file"), and it is honoured throughout the mapping and write paths — `hct_file.c:844`,
`:934`, `:1129`, `:1153`, `:1742`, `:1819`. So the infrastructure for opening a database
read-only exists and is wired in.

The only line that would ever set it is **commented out** (`hct_file.c:1417`):

```c
        pServer->pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
        /* pServer->bReadOnlyMap = 1; */
```

So the flag is permanently false and the read-only path is unreachable.

**This matters for tooling.** A read-only mode plus a shared (`F_RDLCK`) rather than
exclusive lock would in principle allow an out-of-process reader against a live database —
the single most useful capability currently missing. Whether that is actually safe is a
different question: a reader would still need a consistent view of the transaction map,
which lives in the *writer's* heap, so a naive read-only open would see rows whose TIDs it
cannot resolve. **This is not a "just uncomment it" fix**, and it should be put to Dan
Kennedy rather than attempted locally.

**Question for Dan Kennedy:** what is the intent of `bReadOnlyMap`? Is out-of-process
read-only access a planned capability, and what would it require beyond the flag?

## 4. WAL2 by contrast — genuinely multi-process, with a Bedrock-specific catch

WAL2 uses SQLite's standard wal-index in a `-shm` file, mapped shared, coordinated by
`fcntl` locks on the WAL-index header and read-mark slots (`wal.c:482-522`,
`os_unix.c` shm methods). Multiple processes can read and write concurrently — this is
ordinary SQLite WAL behaviour, preserved.

**But Bedrock's build changes the on-disk and shared-memory formats.** As established in
`08-custom-flags.md` §7, `SQLITE_ENABLE_WAL_BIGHASH` alters the wal-index layout
(`ht_slot` `u16`→`u32`, `HASHTABLE_BITS` 12→17, hence `WALINDEX_PGSZ`), and both it and
`SQLITE_ENABLE_WAL2NOCKSUM` alter `WAL_VERSION2` (`wal.c:461-479`). Bedrock's WAL2
databases carry `WAL_VERSION2 == 3048001`, produced only by a build with **both** flags.

So the practical multi-process story for WAL2 is: *yes, but only with a binary built from
Bedrock's exact `AMALGAMATION_FLAGS`.* A stock `sqlite3` shell will not do. That constraint
is recorded nowhere in the repository.

## 5. What this means for Bedrock

### 5.1 The architecture already fits

Bedrock is a single multi-threaded process with a connection pool (`SQLitePool`,
`BedrockServer.cpp:115`) and replication over the network between nodes, not between
processes on one host. **HC-Tree's single-process constraint therefore costs Bedrock
nothing architecturally.** This is the right shape of application for this engine.

### 5.2 Bedrock does not use HC-Tree's own replication

HC-Tree has its own `SQLITE_HCT_NORMAL` / `LEADER` / `FOLLOWER` modes and a
`sqlite_hct_journal` table (`hct_journal.c`). A grep for `SQLITE_HCT_` or
`sqlite_hct_journal` across Bedrock's C++ returns **nothing** — Bedrock runs HC-Tree in
NORMAL mode and does its own replication through its journal tables.

This resolves the open question raised in `01-source-map.md` (whether the two
leader/follower notions interact): **they do not.** Two consequences:

- The FOLLOWER-mode code paths are dead in Bedrock's configuration — including the
  `"snapshot too old"` check (`06-readers-snapshots.md` §3) and the *deferred* log-file
  cleanup path.
- Because the deferred path is dead, `12-bugs.md` #4's log-file leak takes the
  `bDefer==0` branch every time — the one whose `unlink()` is commented out. **The leak is
  active in exactly Bedrock's configuration**, not merely theoretically.

### 5.3 The real cost is operational, not architectural

What Bedrock gives up by running HC-Tree is **all out-of-process tooling against a live
database**:

- No `sqlite3` shell against a running node — for either engine in practice: HC-Tree
  refuses with `SQLITE_BUSY`, WAL2 needs a flag-matched binary.
- No out-of-process backup, integrity check, or ad-hoc analytical query.
- Any diagnostic must be reachable *through the Bedrock process*. This raises the value of
  the in-process diagnostic surface catalogued in `13-instrumentation.md` —
  `hctstats`, `hcttmap`, `hctfile`, `hctpman`, `hctjrnl`, `hctvalid` are not conveniences,
  they are the **only** way to inspect a live HC-Tree database.
- Taking a consistent copy requires either stopping the node or a filesystem/volume-level
  snapshot, since no second process may open the file.

**Recommendation:** treat a control-port query path onto the `hct*` virtual tables as
infrastructure rather than a nicety. If HC-Tree becomes the production engine, that is the
entire debugging interface.

## 6. Open questions

1. For Dan Kennedy: intent of `bReadOnlyMap` (§3) — is out-of-process read-only access
   planned, and what would it need beyond enabling the flag?
2. How do we take backups of a live HC-Tree node today — volume snapshot, or stop-and-copy?
3. Is there a control-port route to run arbitrary read queries (e.g. against `hctstats`)
   on a live node? If not, that is the highest-value operational gap.
4. Does anything in the Bedrock process open and close the database file for unrelated
   purposes? If so, the POSIX-lock caveat in §2 deserves a real check.
