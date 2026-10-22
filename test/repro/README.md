# HC-Tree recovery visibility reproducer

`hctree_recovery_visibility.c` calls SQLite and HC-Tree's public native journal
APIs directly. It does not use or link any Bedrock code. It is single-threaded,
uses a fresh database for each case, consumes and finalizes every statement, and
checks that observations start and finish outside a transaction.

The original failure below was observed on the 2026-10-17 HC-Tree revision.
The minimal case passes with the 2026-10-22 revision now included in `main`;
see the retest results at the end of this document.

## Build and run

From the repository root, build directly from the SQLite amalgamation:

```sh
clang-18 -std=gnu23 -O2 -g -Ilibstuff \
    test/repro/hctree_recovery_visibility.c libstuff/sqlite3.c \
    -lpthread -ldl -lm -o /tmp/hctree-recovery-visibility

/tmp/hctree-recovery-visibility --case gap-reopen --prefix 2 --trace
```

No Bedrock-specific compile definitions are needed. To enable SQLite's debug
assertions, replace `-O2` with `-O0 -DSQLITE_DEBUG`.

For a quick run against an already-built Bedrock SQLite object:

```sh
clang-18 -std=gnu23 -g -Wall -Wextra -Werror -Ilibstuff \
    test/repro/hctree_recovery_visibility.c .build/libstuff/sqlite3.o \
    -lpthread -ldl -lm -o /tmp/hctree-recovery-visibility

/tmp/hctree-recovery-visibility --prefix 2
```

Exit codes are **0** for all checks passing, **1** for a recovery/visibility
mismatch, and **2** for an API, setup, or argument error. A reproduced bug is an
intentional test failure, not a passing regression test. The program prints the
SQLite source ID, compile options, and artifact directory, and leaves all database
and journal files there for inspection. `--trace` also prints executed SQL.

## Smallest reproduced sequence

With `--prefix 2`, the failing case does the following:

1. Initialize the native journal and enter follower mode.
2. Commit `CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)` as CID 2,
   depending on snapshot 1.
3. On a separate connection, commit `INSERT INTO t VALUES (1, 'replayed')`
   as CID 4, depending on snapshot 2. CID 3 is deliberately missing. The available
   snapshot stays at 2 and readers correctly do not see the inserted row.
4. Close every connection, then reopen the same database. Recovery removes CID 4.
   Switch the sole open connection into follower mode.
5. Open a writer and a reader. Commit blank CID 3, then replay CID 4 using the
   same insert and dependency snapshot 2.
6. Compare the first reopened connection, writer, reader opened before replay,
   and reader opened after replay. All report available snapshot 4, autocommit
   enabled, no active transaction, and no outstanding prepared statements.
7. On the affected revision, observe that the first reopened connection returns
   no rows, whereas the other three connections return the inserted row. This
   affects `COUNT(*)`, primary-key lookup, and a full table scan, not just the
   count optimization.
8. Close and replace only the first connection, keeping the other connections
   open. The replacement sees the inserted row correctly.

The journal payload is opaque text, with `":"` for a blank entry. No application
hashing, compression, metadata cache, connection pool, or replication scheduler is
involved. The NORMAL-to-FOLLOWER transition happens with only one connection open
and no active transaction.

Expected: after replay, all four connections see the inserted row. This is not a
case of an intentionally retained old read transaction.

## Controls and variants

Omit `--case` to run the failing case and both controls:

- `gap-reopen`: apply CID N+2 before N+1, close/reopen all handles, fill the gap,
  and replay N+2. **Reproduces on the 2026-10-17 revision.**
- `no-tail-reopen`: close/reopen at the contiguous prefix without first applying
  an out-of-order tail, then apply N+1 and N+2. **Passes.**
- `gap-no-reopen`: apply N+2 before N+1 and fill the gap without closing/reopening
  the database. **Passes.**

`--prefix N` selects the contiguous prefix before the gap (default 157, range
2 through 100000). `--plain-begin` uses `BEGIN` instead of `BEGIN CONCURRENT`.
`--bedrock-probes` additionally runs the legacy-file-format, cache-size, and
schema-version pragmas on open, and page-count queries around commits. These
probes are not needed to reproduce the failure.

## Original results (2026-10-17 revision)

Tested on Linux 6.8.0-53-generic, aarch64, using Clang 18.1.8:

```text
SQLite version: 3.54.0
SQLite source ID: 2026-10-17 16:16:09 a1551bc4240f6a265ae1c3c4d4f7860e998d8d05e91954a3d65f2f518658854c
SQLite branch: hctree-bedrock-lcd-ex

READ [after-replay/recovered-base] mode=1 snapshot=4 autocommit=1 txn=0 count=0 lookup=0 scan=0
FAIL after-replay/recovered-base count: actual=0 expected=1
FAIL after-replay/recovered-base lookup: actual=0 expected=1
FAIL after-replay/recovered-base scan: actual=0 expected=1
READ [after-replay/replay-writer] mode=1 snapshot=4 autocommit=1 txn=0 count=1 lookup=1 scan=1
READ [after-replay/reader-before-replay] mode=1 snapshot=4 autocommit=1 txn=0 count=1 lookup=1 scan=1
READ [after-replay/reader-after-replay] mode=1 snapshot=4 autocommit=1 txn=0 count=1 lookup=1 scan=1
READ [after-replacing-base/replaced-base] mode=1 snapshot=4 autocommit=1 txn=0 count=1 lookup=1 scan=1
RESULT gap-reopen: FAIL (3 mismatches)
```

The failure reproduced in all 32 combinations of prefixes
`2, 15, 16, 17, 31, 32, 33, 157`, `BEGIN`/`BEGIN CONCURRENT`, and probes off/on.
Both controls passed in every combination. Ten additional runs at prefix 2
reproduced every time, with both controls passing every time.

Separate builds directly from `libstuff/sqlite3.c`, without Bedrock's compile
definitions, also reproduced at prefix 2: an `-O0 -DSQLITE_DEBUG` build and an
`-O2` build. No SQLite debug assertion fired. This identifies a native HC-Tree
issue on the source revision above; it does not establish the root cause or
whether a newer upstream revision fixes it.

## Retest with main's 2026-10-22 revision

Merged `origin/main` at `5558ac2466fea351cacf4f6e94bb1432991470d9`, which includes
[the HC-Tree update](https://github.com/Expensify/Bedrock/pull/2882). The SQLite source ID is now:

```text
2026-10-22 15:03:14 11f43371b5d66a46008920071c4e707d116b7d4957b42c900b2cbae7aa0115a1
```

The update clears the HC-Tree connection's tree state in `hctRecoverOneLog()`
after recovery writes are flushed. The standalone reproducer itself is unchanged.
Fresh `-O0 -DSQLITE_DEBUG` and `-O2` builds both pass the minimal case and both
controls. The first reopened connection now sees the replayed row:

```text
READ [after-replay/recovered-base] mode=1 snapshot=4 autocommit=1 txn=0 count=1 lookup=1 scan=1
RESULT gap-reopen: PASS (0 mismatches)
RESULT no-tail-reopen: PASS (0 mismatches)
RESULT gap-no-reopen: PASS (0 mismatches)
```

As a positive control, the pre-merge binary built from `a1551bc4` still reproduces
the original three mismatches on a fresh database. The Bedrock recovery fixture
also asserts the base connection's row count, in addition to the pooled reader.
