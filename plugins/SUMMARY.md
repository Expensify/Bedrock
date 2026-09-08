# plugins/

## Theme

`plugins/` is where Bedrock's request-handling surface lives: each unit here
is one `BedrockPlugin_*` that registers a set of request verbs with
`BedrockServer` and answers them through a `BedrockCommand` subclass's
`peek()`/`process()` pair, owning whatever SQLite schema it privately needs.
This is not a grab-bag of features — it is the plugin registration mechanism
itself, instantiated five times: a cache, a compression UDF provider, a raw-SQL
escape hatch, a job queue, and a MySQL-wire-protocol shim. What ties them
together is the contract each honors with the server, not a shared subject
matter.

## Contents

| Unit | Files | Lines | Role |
|---|---|---|---|
| Cache | Cache.h/.cpp | 374 | Named-blob cache (ReadCache/WriteCache) backed by a `cache` SQLite table, with an in-memory LRU tracker deciding evictions |
| Compression | Compression.h/.cpp | 414 | Registers zstd-dictionary compress()/decompress() SQLite UDFs; dictionaries loaded once at startup |
| DB | DB.h/.cpp | 490 | Catch-all raw-SQL `Query` command; sqlite3-CLI-style output flags (-json/-csv/etc.) and named bound params |
| Jobs | Jobs.h/.cpp | 1740 | SQLite-backed scheduled job queue: create/dequeue/update/finish/retry/fail/cancel, parent-child chains, repeat schedules, ops blacklist |
| MySQL | MySQL.h/.cpp | 1148 | Speaks the MySQL wire protocol on its own port, translating recognized client queries into internal `Query` commands |

No subdirectories.

## Coherence

All five belong together at the whole-unit level: every one is a
`BedrockPlugin_*` implementing the same registration contract, every one
scores 5/5 on location_fit, and every one pulls from the same dependency set
(`libstuff/libstuff.h`, `BedrockPlugin.h`, `BedrockServer.h`, `libstuff/SQResult.h`).
There is no case here of a whole unit wearing the wrong directory — DB's
name_fit is the softest (4/5, since "DB" undersells that it's specifically
the raw-SQL-passthrough plugin, while every plugin touches the DB), but it
still is a plugin, correctly placed.

The friction is internal, not directory-level: several plugins each carry a
sub-part that is either fully generic (owing nothing to the plugin it lives
in) or fully business-specific (owing nothing to the generic framework it's
embedded in). Jobs is the extreme case — at 1740 lines it is more than the
next two plugins combined, and it bundles at least three separable concerns
(job CRUD, an ops-safety blacklist, and Expensify-specific schema literals)
behind one interface.

## Misfits

Six sub-unit misfits were flagged across three plugins. Two resolve inside
this directory; four need to leave it.

**Resolved locally:**

- **`BedrockPlugin_Jobs` crashed-job blacklist** (`getCrashedBedrockJobPatterns`,
  `onNodeLogin`, `_crashedBedrockJobPatterns`, `CrashBedrockJob`/
  `ClearCrashedBedrockJobs`) — an ops-safety kill-switch layered onto job
  scheduling, distinct from job CRUD but tightly coupled to `GetJob(s)`
  peek/process. That coupling is exactly why it doesn't need a new directory:
  split it into its own translation unit inside `plugins/` (e.g.
  `plugins/JobsBlacklist.h/.cpp`, included by Jobs) so the concern is
  separated in code without breaking the peek/process wiring it depends on.
- **`MySQL`'s `g_MySQLVariables`** (~300-row fake AWS-RDS variable table) —
  pure static data, no logic, dominating the file's line count. This is a
  packaging problem, not a placement problem: move the table out of the
  header/cpp into a generated or included data file still inside `plugins/`
  (e.g. `plugins/MySQLVariables.inc`), keeping it next to the code that serves
  it.

**Escalated:**

- **`BedrockPlugin_Cache::LRUMap`** — a fully generic, mutex-protected
  string-keyed LRU tracker with zero cache-specific logic, currently nested
  privately inside one plugin where no other plugin can reach it. Its natural
  reuse boundary is outside `plugins/` entirely.
- **`BedrockPlugin_DB`'s sqlite3-CLI argument/error layer**
  (`Sqlite3QRFSpecWrapper`, `parseSQLite3Args`, `generateErrorContextMessage`) —
  wraps `libstuff/qrf.h`'s C struct with nothing DB-plugin-specific in it;
  belongs with the header it wraps, not the one plugin that happens to use it
  first.
- **`BedrockPlugin_Jobs::scopedDisableNoopMode`** — a generic SQLite-noop RAII
  guard, useful to any plugin that touches noop mode, currently defined
  file-local to Jobs.cpp where nothing else can use it.
- **`BedrockPlugin_Jobs::upgradeDatabase` Expensify-specific index literals**
  (`jobsPriorityNextRunManualSmartScan*`, `jobsManualSmartscanReceiptID`,
  `jobsPriorityNextRunWWWProd`/`WWWStag`) — partial-index WHERE clauses that
  bake one deployment's job-name conventions (`manual/SmartScan*`,
  `www-prod/*`, `www-stag/*`) into otherwise-generic job-queue DDL. Severity
  high: this isn't a wrong-directory problem, it's a missing seam between
  "generic job-queue schema" and "one operator's job-naming scheme," and
  fixing it means deciding where deployment-specific customization lives
  repo-wide — a decision this directory can't make alone.

A note on suggested homes: three of the four escalated items point toward
"libstuff" by default, but libstuff is flagged elsewhere as already
overloaded, and stacking three more unrelated pieces (a container, a CLI-arg
parser, an RAII guard) onto it without a finer subdivision would just move
the coherence problem one level up rather than solve it. This rollup
deliberately leaves `suggested_home` as a direction, not a destination — the
parent is better positioned to decide whether libstuff absorbs these as-is or
whether a new peer location (e.g. a `libstuff/containers` or a plugin-support
header) is warranted.

**Pass B revisits that note — see "Inbound expectations, and misfits
revisited" below** for how libstuff's own rollup
sharpens (and in one case overturns) the "declined, libstuff is overloaded"
call for these three items, and adds a fourth misfit this directory did not
flag on its own: `Compression`'s undeclared inbound dependency from
sqlitecluster.

## Role in the system

Intended order: `libstuff -> sqlitecluster -> plugins -> root`. plugins sits
correctly above sqlitecluster and below root — every unit here scores 5/5 on
location_fit and the directory's own `depends_on_dirs` (`libstuff`,
`sqlitecluster`) matches that order exactly; no plugin reaches sideways into
a sibling it shouldn't, and none of the five units is itself misplaced.

**Boundary with sqlitecluster — leaks, but not from a place this directory
controls.** plugins is supposed to be the *only* side depending on
sqlitecluster, never the reverse. `BedrockPlugin_Compression` is, on its own
terms, an ordinary plugin: it registers SQLite UDFs and owns its
`zstdDictionaries` table like any other plugin owns its schema — nothing
about `Compression.h`/`.cpp` looks wrong from inside `plugins/`, which is why
Pass A did not flag it. But sqlitecluster's own Pass B rollup reports that
`SQLite.cpp` and `SQLiteNode.cpp` both `#include <plugins/Compression.h>`
and call `BedrockPlugin_Compression::compress/decompress` directly to
handle journal entries. That means `Compression` has an inbound caller this
directory's own dependency list (and Pass A view) could not see: the
replication engine one layer below it. The boundary violation is real, but
it is sqlitecluster reaching up, not `Compression` reaching down — nothing
in `plugins/` needs to change its own behavior, only what may call into it
uninvited.

**Fix (mirrors sqlitecluster's Pass B, same conclusion from this side):**
sink the raw dictionary-based compress/decompress primitive to libstuff.
`BedrockPlugin_Compression` keeps the SQLite UDF registration
(`compress()`/`decompress()` as SQL functions), the `zstdDictionaries`
schema, and startup dictionary loading — all the actually plugin-shaped
parts — and becomes a consumer of the libstuff primitive rather than the
only implementation of it. Once sqlitecluster calls libstuff directly, the
undeclared inbound edge from sqlitecluster disappears and `plugins`'
`depends_on_dirs` stays exactly what it already claims.

## Inbound expectations, and misfits revisited

**Inbound expectations.** root depends on plugins for the five
`BedrockPlugin_*` registrations and the `peek()`/`process()` pattern; test
depends on it for Jobs-plugin coverage. Both are satisfied by what's
exported here — no gap visible from this side. The one thing plugins
*implicitly* promises outward and doesn't fully control is that nothing
below it (sqlitecluster) reaches back in; "Role in the system" above shows
that promise is currently broken, unbeknownst to this directory until the
sibling view made it visible. Concretely, add this as a misfit this
directory now acknowledges:

- **`BedrockPlugin_Compression` has an undeclared caller in sqlitecluster**
  (`SQLite.cpp`, `SQLiteNode.cpp` call `compress`/`decompress` directly) —
  severity high (it's the root-flagged inverted-layering violation, seen
  from the callee's side); not resolvable inside `plugins/` alone since the
  fix touches libstuff and sqlitecluster too. See "Role in the system"
  above for the concrete plan.

**Misfits revisited (spec item 7).** With libstuff's own Pass A rollup now
visible, the blanket "libstuff is overloaded, decline all three" call was
too coarse — it treated three different situations as one:

- **`scopedDisableNoopMode` — reassign, it was never a libstuff candidate.**
  sqlitecluster's own rollup independently flags `SQLite::setUpdateNoopMode`/
  `_noopUpdateMode` as living on the core `SQLite` class for what is
  documented as mock-only testing support. `scopedDisableNoopMode` is an
  RAII guard around exactly that state. Its natural home was never libstuff
  at all — it's `sqlitecluster/SQLite.h`, next to the state it toggles.
  `suggested_home` narrows from "libstuff or SQLite.h" to
  **`sqlitecluster/SQLite.h`**, and the libstuff-overload objection simply
  doesn't apply to this one.
- **`BedrockPlugin_DB`'s sqlite3-CLI wrapper — the overload objection
  doesn't apply here either.** Its target, `libstuff/qrf.h`, is one of
  libstuff's own small, dedicated units (like `SQResult`/`SQValue`), not the
  4612-line catch-all file libstuff's rollup names as the actual overload
  problem. Placing `Sqlite3QRFSpecWrapper`/`parseSQLite3Args`/
  `generateErrorContextMessage` alongside `qrf.h` doesn't add to that
  catch-all at all. This one can be treated as resolved-in-direction: escalate
  still (it leaves this directory), but with no remaining ambiguity about
  where.
- **`BedrockPlugin_Cache::LRUMap` — still escalate, but sharpen the
  destination.** This is the one genuine "which part of libstuff" case, and
  libstuff's rollup shows the answer is not "the flat libstuff/ root": libstuff
  has already spun `JSON::Value` out into its own `JSON/` subdirectory rather
  than adding it to the catch-all. A generic container belongs the same way —
  a new `libstuff/containers/` (or similar) subdirectory, following the
  `JSON/` precedent, not the overloaded flat namespace. `suggested_home`
  narrows accordingly.

Net effect: of the three items Pass A declined to place inside libstuff
because "libstuff is overloaded," only one (`LRUMap`) was actually blocked by
that reasoning, and even it now has a concrete direction rather than a
shrug. The other two were miscategorized by the same broad brush — one
belongs to a sibling (sqlitecluster) entirely, the other to a
part of libstuff that isn't the overloaded part.

<!-- ROLLUP
theme: plugins/ holds each BedrockPlugin_* implementation — self-contained units that register request verbs with BedrockServer and answer them via peek()/process(), each owning whatever SQLite schema it needs.
exports: [BedrockPlugin_Cache, BedrockPlugin_Compression, BedrockPlugin_DB, BedrockPlugin_Jobs, BedrockPlugin_MySQL, the peek()/process() BedrockCommand pattern used by all five, per-plugin SQLite schema ownership (cache/cacheSize, zstdDictionaries, jobs tables), MySQL wire-protocol compatibility shim]
depends_on_dirs: [libstuff, sqlitecluster]
depended_on_by: [root, test, sqlitecluster (undeclared/improper — see "Role in the system"; should be eliminated by the compression fix)]
misfit_count: {high: 2, med: 1, low: 4}
resolved_locally: 2
escalate:
  - item: "BedrockPlugin_Compression has an undeclared caller in sqlitecluster (SQLite.cpp, SQLiteNode.cpp call compress/decompress directly, bypassing plugins entirely)"
    from: plugins/Compression.h,Compression.cpp (called from sqlitecluster/SQLite.cpp, sqlitecluster/SQLiteNode.cpp)
    why: "the intended order (libstuff -> sqlitecluster -> plugins -> root) has plugins depending on sqlitecluster, never the reverse; sqlitecluster's own Pass B rollup reports this call, which plugins' Pass A view could not see since nothing about Compression.h looks wrong from inside plugins/"
    suggested_home: "libstuff (e.g. libstuff/SCompress.h/.cpp) for a raw dictionary-based zstd compress/decompress primitive; Compression keeps UDF registration, zstdDictionaries schema, and dictionary loading on top of it; sqlitecluster calls libstuff directly instead of plugins/Compression.h. Root must arbitrate since the fix edits libstuff."
  - item: BedrockPlugin_Cache::LRUMap
    from: plugins/Cache.h
    why: fully generic string-keyed LRU tracker, no cache logic, trapped private to one plugin
    suggested_home: "a new libstuff subdirectory (e.g. libstuff/containers/), following the JSON/ precedent of libstuff spinning out a thematic subdirectory rather than adding to its overloaded flat catch-all file"
  - item: BedrockPlugin_DB::Sqlite3QRFSpecWrapper / parseSQLite3Args / generateErrorContextMessage
    from: plugins/DB.h,DB.cpp
    why: generic sqlite3-CLI arg/error layer wrapping libstuff/qrf.h, nothing DB-plugin-specific
    suggested_home: "libstuff, colocated with qrf.h — qrf.h is already one of libstuff's small dedicated units, not the 4612-line catch-all file, so this placement doesn't touch the part of libstuff that's actually overloaded"
  - item: BedrockPlugin_Jobs::scopedDisableNoopMode
    from: plugins/Jobs.cpp
    why: "generic SQLite-noop RAII guard wrapping SQLite::setUpdateNoopMode/_noopUpdateMode, which sqlitecluster's own rollup places on the core SQLite class (documented as mock-only test support) — this was never a libstuff candidate"
    suggested_home: sqlitecluster/SQLite.h, next to the noop-mode state it toggles
  - item: BedrockPlugin_Jobs::upgradeDatabase Expensify-specific index literals (jobsPriorityNextRunManualSmartScan*, jobsManualSmartscanReceiptID, jobsPriorityNextRunWWWProd/WWWStag)
    from: plugins/Jobs.cpp
    why: deployment-specific job-name literals baked into otherwise-generic job-queue schema DDL; needs a repo-wide customization seam, not a local fix
    suggested_home: null
-->
