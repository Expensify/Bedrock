# Cluster: sqlite-unit-commit

## What these units have in common

This cluster was formed algorithmically from name/intent token overlap around
"SQLite" and "commit," but that overlap is only partially real. Two units
genuinely share a theme — commit/rollback semantics of a `SQLite` handle:
`AfterCommitCallbackTest` (after-commit callback firing rules) and
`WriteLocalUnreplicatedTest` (commit-count/journal invariants and rollback for
`writeLocalUnreplicated()`). Both build a temp on-disk DB via an RAII
`*TempDBFile` helper struct and drive a real `SQLite` object through
`test/lib/BedrockTester.h`.

The remaining three units matched on weaker signal:

- `SIsValidSQLiteDateModifierTest` shares the literal token "SQLite" (it's
  testing `SIsValidSQLiteDateModifier`, a string-validation free function in
  `libstuff`) but has nothing to do with commits, transactions, or a live DB
  handle at all — it's pure string-parsing logic.
- `SQLiteNodeTest` shares "SQLite" via the class name `SQLiteNode`, and one of
  its two tests uses commit *count* as a peer-selection tiebreaker, but the
  unit's actual subject is sync-peer selection/lookup in the replication
  layer, not commit semantics of a database handle.
- `SDeburrTest` shares neither "SQLite" nor "commit" in any real sense — it
  tests Unicode transliteration (`SDeburr::deburr`) and appears to have been
  swept in on some other token match (possibly generic "test fixture"
  similarity). It has no thematic connection to the other four units.

So: this is a two-unit theme (SQLite commit/write behavior) diluted by three
units pulled in on shallow lexical overlap, not four unrelated additions —
worth flagging plainly since the label implies a five-unit cohesive group that
doesn't actually exist.

## Units

| Unit | Lines | Summary |
|---|---|---|
| `AfterCommitCallbackTest` | 123 | Verifies SQLite's after-commit callback list fires once per commit, not on rollback or a lost/conflicted commit, and is shared across a copy-constructed handle. |
| `SDeburrTest` | 86 | Unit tests for `SDeburr::deburr`, the Unicode-to-ASCII transliteration helper (diacritics, ligatures, language-specific letters, emoji/CJK passthrough). No SQLite or commit relevance. |
| `SIsValidSQLiteDateModifierTest` | 75 | Tests the libstuff free function `SIsValidSQLiteDateModifier`, validating strings like `"+1 DAY"`; pure string parsing, no DB handle or commit involved. |
| `SQLiteNodeTest` | 170 | Tests `SQLiteNode`'s sync-peer selection (fastest logged-in peer by latency, tie-broken by commit count) and name-based peer lookup, using a friend-granted white-box tester class and a no-op `SQLiteServer` stub. |
| `WriteLocalUnreplicatedTest` | 146 | Tests `SQLite::writeLocalUnreplicated()`: commit count and journal untouched, clean rollback on a failed query, safe behavior under a concurrent commit from another handle. |

All five units score 5/5 on name_fit, location_fit, and naming_quality per
their own unit records — the per-unit metadata reports no problems fitting
`test/tests/`. The issue here is cluster cohesion, not unit placement.

## Misfits (relative to this cluster, not necessarily the directory)

- **`SDeburrTest`** — no thematic connection to SQLite commit behavior at all.
  Fits `test/tests/` fine (per its own location_fit score) but does not belong
  in a "sqlite-unit-commit" grouping. This is a clustering-algorithm artifact,
  not a code placement problem — nothing to escalate.
- **`SIsValidSQLiteDateModifierTest`** — same story: legitimate `test/tests/`
  citizen, matched into this cluster only on the literal substring "SQLite."
  It tests a pure string-validation helper, unrelated to commit/transaction
  semantics.
- **`SQLiteNodeTest`** — closer than the above two (it does touch commit
  *count* as a tiebreaker), but its real subject is peer-selection logic in
  the replication layer, not commit/write semantics of a `SQLite` handle. A
  cluster boundary question, not a code problem.

None of these are directory-placement misfits — the input file's own
name_fit/location_fit scores for every unit are 5/5. All three are
"cluster doesn't match its label" observations for the parent Pass A/B agent
to weigh when deciding whether to trust or re-derive clustering elsewhere in
`test/tests/`.

<!-- ROLLUP
theme: SQLite commit/write-path unit tests (diluted: 2 of 5 units are genuinely on-theme; 3 matched on shallow "SQLite" token overlap only)
exports: [after-commit-callback semantics, writeLocalUnreplicated commit/rollback invariants, SIsValidSQLiteDateModifier string validation, SQLiteNode sync-peer selection, SDeburr Unicode transliteration tests]
depends_on_dirs: [libstuff, sqlitecluster, test/lib]
depended_on_by: []
misfit_count: {high: 0, med: 0, low: 3}
resolved_locally: 0
escalate: []
-->
