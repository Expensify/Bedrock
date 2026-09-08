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

<!-- ROLLUP
theme: plugins/ holds each BedrockPlugin_* implementation — self-contained units that register request verbs with BedrockServer and answer them via peek()/process(), each owning whatever SQLite schema it needs.
exports: [BedrockPlugin_Cache, BedrockPlugin_Compression, BedrockPlugin_DB, BedrockPlugin_Jobs, BedrockPlugin_MySQL, the peek()/process() BedrockCommand pattern used by all five, per-plugin SQLite schema ownership (cache/cacheSize, zstdDictionaries, jobs tables), MySQL wire-protocol compatibility shim]
depends_on_dirs: [libstuff, sqlitecluster]
depended_on_by: []
misfit_count: {high: 1, med: 1, low: 4}
resolved_locally: 2
escalate:
  - item: BedrockPlugin_Cache::LRUMap
    from: plugins/Cache.h
    why: fully generic string-keyed LRU tracker, no cache logic, trapped private to one plugin
    suggested_home: libstuff (or a new shared containers location — libstuff is already overloaded, see note)
  - item: BedrockPlugin_DB::Sqlite3QRFSpecWrapper / parseSQLite3Args / generateErrorContextMessage
    from: plugins/DB.h,DB.cpp
    why: generic sqlite3-CLI arg/error layer wrapping libstuff/qrf.h, nothing DB-plugin-specific
    suggested_home: libstuff/qrf.h or a new libstuff unit alongside it
  - item: BedrockPlugin_Jobs::scopedDisableNoopMode
    from: plugins/Jobs.cpp
    why: generic SQLite-noop RAII guard, file-local to Jobs.cpp, unreachable by other plugins that need it
    suggested_home: libstuff or SQLite.h
  - item: BedrockPlugin_Jobs::upgradeDatabase Expensify-specific index literals (jobsPriorityNextRunManualSmartScan*, jobsManualSmartscanReceiptID, jobsPriorityNextRunWWWProd/WWWStag)
    from: plugins/Jobs.cpp
    why: deployment-specific job-name literals baked into otherwise-generic job-queue schema DDL; needs a repo-wide customization seam, not a local fix
    suggested_home: null
-->
