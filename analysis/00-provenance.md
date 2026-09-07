# 00 — Provenance: what code does production actually run?

**Unit:** Phase 0a — gate for all subsequent analysis
**Status:** COMPLETE
**Date:** 2026-09-07

---

## Summary

**Bedrock's `libstuff/sqlite3.c` is a pristine, unmodified amalgamation of upstream
check-in `bf8437339f31209af779566b1bf6744015a278a9133a260a5c9584ef4a467ad5`
(2026-08-28 11:51:14, branch `hctree-bedrock-lcd-ex`).**

There are **zero Bedrock-only patches** to the amalgamation. Every byte of difference
between the vendored file and a freshly generated one is explained by (a) the ten days of
upstream changes between 2026-08-28 and the requested check-in `eedd80c1a9749300`
(2026-09-07), or (b) version-stamp strings.

The total functional drift across those ten days is **one change**: a new argument triple
on `sqlite3BtreeIdxDelete()` that lets `OP_IdxDelete` skip a no-op index delete. Nothing
else in the engine changed. This is directly relevant to the conflict investigation (P1)
and is tracked there.

This is a good result for maintenance: a re-vendor cannot silently revert a local patch,
because there are no local patches.

---

## How this was established (reproducible)

The sandbox has no `fossil`, so the tarball path was used. `manifest.uuid` in the tarball
is the authoritative check-in identifier and it matches the requested check-in exactly.

```
curl -sSL -o hctree.tar.gz \
  "https://sqlite.org/hctree/tarball/eedd80c1a9749300/hctree.tar.gz"
tar xzf hctree.tar.gz
cat hctree/manifest.uuid
#   eedd80c1a974930025bf713a44457f51e1f18998d371f6920f97220e20b561fb   ✓
cat hctree/manifest.tags
#   branch hctree-bedrock-lcd-ex
#   tag    hctree-bedrock-lcd-ex                                       ✓
head -2 hctree/manifest
#   C Merge\slatest\schanges\sfrom\sbranch\shctree-bedrock.
#   D 2026-09-07T15:24:37.507                                          ✓
```

Amalgamation generation (needs `tclsh`, installable via `apt-get install tcl`):

```
cd hctree
./configure --enable-all --enable-update-limit
make sqlite3.c
diff -u /home/user/Bedrock/libstuff/sqlite3.c hctree/sqlite3.c
```

### The `--enable-update-limit` trap

**This flag is required and is not obvious.** `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` appears
in Bedrock's `Makefile:18` `AMALGAMATION_FLAGS`, which reads like a *compile-time* flag.
It is not only that: the grammar is processed by Lemon at **amalgamation-generation** time,
and the option changes the generated parser tables in `parse.c`.

Generating without it produced a 26-hunk / 1,115-line diff, of which 13 hunks were nothing
but shifted Lemon state-machine constants (`YYNSTATE` 604→600, action tables, etc.) plus
the loss of `#define SQLITE_UDL_CAPABLE_PARSER 1`. With the flag, that noise disappears
entirely and the diff drops to 12 hunks / 158 lines.

**Maintenance risk (see also `12-bugs.md` #2):** if anyone regenerates the vendored
amalgamation without `--enable-update-limit`, the `-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT`
compile flag in the Makefile will **not** restore the capability — the parser tables are
already baked. `UPDATE ... LIMIT` / `DELETE ... LIMIT` would begin failing as syntax
errors, and nothing in the build would warn. The generation procedure is not recorded
anywhere in the Bedrock repo.

---

## Hunk-by-hunk classification

All 12 hunks, complete. Classification: **(a)** upstream-newer, **(b)** Bedrock-only patch,
**(c)** generation noise.

| # | Amalg. line | File region | Change | Class |
|---|---|---|---|---|
| 1 | 18 | header comment | check-in id `bf843733…` → `eedd80c1…` | (c) |
| 2 | 476 | `sqlite3.h` | `SQLITE_SOURCE_ID`, `SQLITE_SCM_DATETIME` | (c) |
| 3 | 17840 | `btree.h` | `sqlite3BtreeIdxDelete` proto +3 args | (a) |
| 4 | 18604 | `btreeModules.h` | `sqlite3HctBtreeIdxDelete` proto +3 args | (a) |
| 5 | 18684 | `btreeModules.h` | `sqlite3StockBtreeIdxDelete` proto +3 args | (a) |
| 6 | 90043 | `btwrapper.c` | `sqlite3StockBtreeIdxDelete` def +3 args, `assert(0)` | (a) |
| 7 | 90108 | `btwrapper.c` | `xBtreeIdxDelete` method-table slot signature | (a) |
| 8 | 90238 | `btwrapper.c` | `sqlite3BtreeIdxDelete` dispatch thunk forwards new args | (a) |
| 9 | 95182 | `hctree.c` | `sqlite3HctBtreeIdxDelete` def +3 args, +doc comment | (a) |
| 10 | 95196 | `hctree.c` | the actual skip logic (`memcmp` → `*pbNot=1`) | (a) |
| 11 | 113697 | `vdbe.c` | `OP_IdxDelete` passes P3 buffer, honours skip | (a) |
| 12 | 275623 | `fts5.c` | `fts5_source_id()` string | (c) |

**Class (b) count: 0.** Confirmed by dumping every non-context line of the diff (66 lines
total) and accounting for each one.

`libstuff/sqlite3.h` differs from the freshly generated `sqlite3.h` **only** in
`SQLITE_SOURCE_ID` and `SQLITE_SCM_DATETIME` — i.e. **no public API changed** in those ten
days.

---

## The one functional change: `OP_IdxDelete` no-op skip

This is upstream check-in work landing after Bedrock's drop. Because it is the *only*
behavioural drift, and because it is a conflict-reduction change for HC-Tree, it deserves
detail here.

### Before (what production runs today, Aug-28 drop)

`sqlite3HctBtreeIdxDelete(BtCursor*, UnpackedRecord*)` unconditionally serializes the key
and calls `sqlite3HctTreeDeleteKey()`.

### After (check-in `eedd80c1a9`)

Signature becomes:

```c
int sqlite3HctBtreeIdxDelete(
  BtCursor *pCursor, UnpackedRecord *pKey,
  const void *pIfnot, int nIfnot, int *pbNot   /* new */
);
```

and the body ([`src/hctree.c`, hunk 10]) short-circuits:

```c
if( pIfnot!=0 && nIfnot==nRec && 0==memcmp(pIfnot, aRec, nIfnot) ){
  *pbNot = 1;
}else{
  rc = sqlite3HctTreeDeleteKey(pCur->pHctTreeCsr, pKey, 0, nRec, aRec);
}
```

The VDBE side ([`src/vdbe.c` `case OP_IdxDelete`]) supplies the *record about to be
inserted* from register P3, and when the delete is skipped it nulls P3 so the paired
insert is skipped too:

```c
if( res ){ sqlite3VdbeMemSetNull(&aMem[pOp->p3]); break; }
```

Net effect: an `UPDATE` that would delete an index entry and immediately reinsert a
byte-identical one now touches nothing — removing both the write and the conflict
footprint it would have created.

### Scope limit — this is narrower than it looks

The optimization only engages when the code generator populates P3, and
`src/delete.c:926` populates it only for **expression indexes**:

```c
if( pIdx->bHasExpr && aRegIdx ){ p3 = aRegIdx[i]; }
```

For ordinary column indexes, `update.c` already decides *statically* whether an index
needs touching at all (`indexColumnIsBeingUpdated()`, `src/update.c:97`, driven by the
`aXRef[]` map at `src/update.c:309`); if no indexed column appears in the `SET` list, the
index is skipped entirely and `aRegIdx[i]==0`. Expression indexes are the gap, because
their *inputs* can change while their *output* does not — a static column test cannot see
that. This check-in closes exactly that gap and no more.

**Consequence for us:** if Expensify's schema has no expression indexes, upgrading to this
check-in changes nothing about conflict rates. If it does have them on hot tables, it is
free conflict reduction. Worth checking the production schema for
`CREATE INDEX … ON t(expr)` before treating this as a reason to re-vendor.

**The remaining gap** — and a live P1/P2 lead — is the case where an indexed column *is*
in the `SET` list but is assigned its existing value (`UPDATE t SET x=x`, or an ORM that
writes back every column). The static test says "index affected", so HC-Tree deletes and
reinserts an identical index entry, manufacturing a write-set entry and a potential
conflict out of a semantic no-op. Tracked in `10-conflict-investigation.md`.

---

## Drifted-code register

Every later finding must state whether it touches drifted code. The complete list of
functions that differ between the vendored drop and check-in `eedd80c1a9749300`:

- `sqlite3BtreeIdxDelete` (dispatch thunk, `btwrapper.c`)
- `sqlite3HctBtreeIdxDelete` (`hctree.c`)
- `sqlite3StockBtreeIdxDelete` (`btwrapper.c`)
- `OP_IdxDelete` case (`vdbe.c`)
- the `xBtreeIdxDelete` slot in the btree method table (`btwrapper.c`)

Everything else in this analysis applies **identically** to the code running in
production. That is an unusually clean position to analyse from.

---

## Notes for reproduction

- The upstream checkout lives in the session scratchpad and is deliberately **not**
  committed to Bedrock. Everything needed to recreate it is above.
- `configure --enable-all` additionally enables extensions beyond Bedrock's flag set; this
  affects which *extensions* are compiled, not the core engine text, and produced no diff
  hunks outside the ones listed. If a future check wants byte-exactness on extensions,
  match Bedrock's `AMALGAMATION_FLAGS` extension list explicitly.
- `AMALGAMATION_FLAGS` is a misleading name: it mixes generation-time options
  (`SQLITE_ENABLE_UPDATE_DELETE_LIMIT`), real compile-time options
  (`SQLITE_SHARED_MAPPING`, `SQLITE_ENABLE_WAL_BIGHASH`, …), and at least one flag that
  does nothing at all (`SQLITE_MUTEX_ALERT_MILLISECONDS`, see `12-bugs.md` #1).
