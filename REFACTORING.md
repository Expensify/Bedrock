# Bedrock — Structural Refactoring Report

Every claim below was verified against source by an agent instructed to falsify
it. Claims that did not survive were removed; claims that survived in weaker
form are stated in the weaker form. `.arch/verified/corrections.md` records what
changed and why — including several findings this analysis initially got wrong.

Line references are to the pre-annotation tree (`git show <ref>:<path>`), since
this pass inserted a `SUMMARY` comment block at the top of every source file.

---

## The one-paragraph version

Bedrock's structure is sound at the directory level and drifting at the file
level. `plugins/`, `sqlitecluster/`, and `test/` each contain what their names
promise. The problem is concentrated in one file: **`libstuff/libstuff.cpp` is
50.5% six unrelated subsystems** (1,928 of 3,816 real lines), and because 68 of
150 units depend on it, that single file sets the coupling floor for the whole
codebase. Secondarily, three dependencies run against the intended layering, and
one of them — journal compression — is load-bearing at runtime rather than a
stray include. Nothing here is broken today. All of it is design debt with a
measurable cost.

---

## Priority 1 — Decompose `libstuff/libstuff.{h,cpp}`

**Blast radius 68 of 150 units. 1,928 of 3,816 real lines are six separable
subsystems.**

An earlier draft of this report claimed dedicated units already existed for
these families and the work was a stalled migration. **That was wrong for the
three largest.** `SQResult`/`SQValue` are the data types `SQuery` consumes and
contain no `sqlite3_` calls; `SHTTPSManager` touches the HTTP grammar at two
sites; the socket primitives have 14 callers outside `STCPManager`. Five of six
families need **new** units. That is a bigger job than it first appeared.

Ordered by extraction cost, cheapest first:

| # | Family | Lines | Destination | Notes |
|---|---|---|---|---|
| 1 | Crypto — `SAESEncrypt/Decrypt`, `SHashSHA1/256`, `SEncodeBase64`, `SHMAC*` | 170 | **new** `SCrypto` unit | Cleanest. Two disjoint regions; only outward dependency is the `SASSERT` macro. The AES block is even misfiled *within* the junk drawer — it sits under the `// Network helpers` banner. |
| 2 | Syslog transport — `SLogSocketFD`, `SSyslogSocketDirect`, `SSyslogNoop` | 68 | `SLog.cpp` | `SLog.cpp` is 178 lines and holds no transport. Precedent: `SFluentdLogger` was extracted as a class but left its free-function facade behind (`libstuff.cpp:384-425`) — the same pattern, twice. |
| 3 | Gzip — `SGZip`, `SGUnzip` | 102 | with HTTP, or delete | `SGZip` has exactly one caller (`SComposeHTTP`), so it is an HTTP helper. **`SGUnzip` has zero production callers** — only `test/tests/LibStuffTest.cpp:540`. Audit for deletion first. |
| 4 | HTTP grammar — `SParseHTTP`, `SComposeHTTP`, `SParseURI*`, `SComposePOST` | 645 | **new** unit near `SData` | Real consumer is `SData.cpp`, not `SHTTPSManager`. The `SData` include cycle is already solved and shipping (`libstuff.h:145` forward-declares `struct SData`), so this is a cost, not a blocker. |
| 5 | SQLite engine — `SQuery`, `SQVerifyTable*`, `SQList` | 482 | **new** execution unit | Busy-retry, parameter binding, corruption detection, slow-query logging. Carry the `SQ()` declarations along: `SQList<Container>` (`libstuff.h:855`) calls them. |
| 6 | Socket primitives — `S_socket`, `S_poll`, `SFDset`, … | 467 | **leave for now** | Genuinely blocked. See below. |

**Why the socket family never moved — a legitimate reason.** `fd_map`
(`libstuff.h:763`) is the typedef the entire application's poll loop is written
against. It cannot follow `S_poll`/`SFDset` into `STCPManager.h`, because that
header already includes `libstuff.h`. Inverting it would force `BedrockServer`,
`SQLiteNode`, `BedrockCommand`, `main.cpp`, and `SSynchronizedQueue.h` to depend
on the TCP manager just to run a poll loop. Do this last, or not at all.

**Counter-arguments tested and rejected.** No function in any family is defined
inline or as a template in the header, so moving them costs zero inlining. The
plugin ABI (`-rdynamic` + `dlopen`) does constrain *renaming and hiding* these
symbols — but relocating a function to another `.cpp` in the same binary changes
neither its mangled name nor its dynamic-symbol export, so it does not constrain
*moving*.

---

## Priority 2 — `libstuff.h` leaks the SQLite C API into 78 translation units

`libstuff.h:118` includes `qrf.h`; `qrf.h:21` includes `sqlite3.h`. Every one of
the **78 translation units** that includes `libstuff.h` therefore parses the
full SQLite C API — **+3,272 preprocessed lines each**. Only about four
subsystems actually use `sqlite3_qrf_spec`.

The giveaway that this is accidental: `libstuff.h:142` still carries a now-dead
`struct sqlite3;` forward declaration. Someone deliberately kept SQLite out of
this header, and `qrf.h` silently defeated it.

Cheapest high-value fix in the report. Forward-declare the qrf types, or move
the `qrf.h` include to the few `.cpp` files that need it.

---

## Priority 3 — Journal compression inverts the layering, at runtime

`sqlitecluster/SQLite.cpp:44` and `sqlitecluster/SQLiteNode.cpp:55` both include
`plugins/Compression.h`. Call sites:

| Location | Function |
|---|---|
| `SQLite.cpp:341` | `SQLite::commonConstructorInitialization` → `registerSQLite` |
| `SQLite.cpp:1096` | `SQLite::prepare` → `compress` |
| `SQLite.cpp:1320` | `SQLite::rollback` → `decompress` (logging only) |
| `SQLiteNode.cpp:1874` | `SQLiteNode::_recvSynchronize` → `decompress` |
| `SQLiteNode.cpp:2051` | `SQLiteNode::_handleBeginTransaction` → `decompress` |

Two things make this more than an include:

1. **It is a three-layer cycle.** `plugins/Compression.h:51` → `BedrockPlugin.h`
   → `BedrockCommand.h:50,51` → `sqlitecluster/SQLiteCommand.h:31` →
   `SQLiteNode.h`. So `sqlitecluster` → root → `sqlitecluster`.
2. **It is load-bearing at runtime.** `_dictionaries` is populated only via
   `initializeFromDB` → `loadDictionariesFromDB` (`Compression.cpp:66-68`),
   driven from `BedrockServer.cpp:188` over registered plugins. Journal
   compression silently depends on the Compression plugin being registered.

**Fix:** sink a dictionary-based zstd compress/decompress primitive into
libstuff. `sqlitecluster` calls it directly and drops the include; `plugins`
keeps UDF registration, the `zstdDictionaries` schema, and dictionary loading,
becoming a consumer rather than sole owner.

*Precision note:* `BedrockPlugin_Compression::getCommand` returns `nullptr`
(`Compression.cpp:61-64`). It is a codec that registers through the plugin
mechanism, not a command handler. Failure mode when unconfigured is benign —
`journalZstdDictionaryID` defaults to 0 and `compress` returns input unchanged.

---

## Priority 4 — Small, unambiguous, compiler-verified

| Finding | Evidence | Fix |
|---|---|---|
| **`AutoScopeOnPrepare` belongs in `sqlitecluster`** | Includes `sqlitecluster/SQLite.h` (`:36`); its sole consumer in the repo is `sqlitecluster/SQLiteCore.cpp:22,38`. The header doesn't even need the include — `SQLite` appears only as a reference member and in function-pointer signatures. | Move the pair to `sqlitecluster/`. Cleanest fix in the report. |
| **Three dead includes in `SHTTPSManager.cpp`** | `BedrockServer.h` (:39), `sqlitecluster/SQLiteNode.h` (:41), `BedrockPlugin.h` (:38). Verified by deleting all three and compiling: `g++ -fsyntax-only -std=c++20 -I.` → exit 0. | Delete. Severs the file's entire upward dependency. |
| **`version.h` is an orphan** | `SVERSION` has two real-code references, both its own definition. `version.h` is included by nothing; no `-DSVERSION` in any build file. Superseded by `bedrockVersion.h`'s `VERSION`. | Delete the file. |
| **`commandPortSuppressionReasons` is dead** | One real-code reference: the declaration at `BedrockServer.h:246`. Its doc comment cites a `commandPortSuppressionCount` type that does not exist. | Delete. (Public member, so an out-of-repo plugin could in principle touch it.) |
| **File-scope helpers have external linkage** | Zero `static` at file scope and zero anonymous namespaces across all 3,816 real lines of `libstuff.cpp`. `_SParseHTTP_GetUpToNext`, `_SParseHTTP_GetUpToEnd`, `_SDecodeURIChar`, `_SParseJSONString/Array/Object/Value`, and the four syslog globals all export unnecessarily despite `_`-prefixed "private" naming. | Mark `static` — but see the ABI caveat below. |

**ABI caveat on the last row.** Bedrock links `-rdynamic` and `dlopen`s
out-of-tree plugins that call these free functions with no linkage of their own
(`TestPlugin.cpp:889` calls `SParseURI`). A blanket never-mark-static habit is a
coherent, if overbroad, response. Confirm against the closed-source plugin set
before hiding any symbol.

---

## Priority 5 — Layering, lower confidence

**`libstuff/SSignal.cpp` reaches into `sqlitecluster`.** `_SSignal_StackTrace`
calls `SQLiteNode::KILLABLE_SQLITE_NODE->kill()` (`:342,344`). Report this as a
layering violation only. Do **not** headline it as signal-unsafe: that handler
is already unsafe by design and says so at `SSignal.cpp:266-269` (`malloc`,
`backtrace`, `__cxa_demangle`, syslog). The `kill()` call is placed last, after
all logging and immediately before `abort()`, so a hang there costs only the
port-release optimization. **Fix:** generic crash-hook API in libstuff, concrete
`kill()` registration from `sqlitecluster`.

**`SHTTPSManager`'s 9-line subclass.** `SStandaloneHTTPSManager`
(`SHTTPSManager.h:44-98`) is complete and plugin-free. `SHTTPSManager`
(`:100-108`) adds only two constructors and a `BedrockPlugin& plugin` member
that **is never read anywhere in this repo**. All production consumers use
`Transaction`/`Socket`, inherited from the standalone base.

*This is not a header dependency.* An earlier draft said `SHTTPSManager.h`
includes `BedrockPlugin.h`; it does not — line 42 is a forward declaration
`class BedrockPlugin;`, the minimal-coupling form. And "never read in-repo" does
not imply "safe to remove": `plugin` is `protected`, and out-of-tree plugins may
subclass. Treat as a question for whoever owns the closed-source plugin set.

---

## Priority 6 — Plugin-local generics with identified homes

| Item | Home | Rationale |
|---|---|---|
| `BedrockPlugin_Cache::LRUMap` | new `libstuff/containers/` | Fully generic string-keyed LRU with no cache-specific logic, trapped private to one plugin. A new subdirectory follows the `JSON/` precedent rather than adding to the catch-all. |
| `BedrockPlugin_DB::Sqlite3QRFSpecWrapper`, `parseSQLite3Args`, `generateErrorContextMessage` | beside `libstuff/qrf.h` | Wraps `qrf.h`'s C struct with nothing DB-plugin-specific. `qrf.h` is already an isolated small unit — not the catch-all. |
| `BedrockPlugin_Jobs::scopedDisableNoopMode` | `sqlitecluster/SQLite.h` | Wraps `setUpdateNoopMode`/`_noopUpdateMode`, which live there. Not a libstuff matter at all. |
| `MySQL`'s `g_MySQLVariables` (~300 static rows) | a data file in `plugins/` | Packaging, not placement. |

---

## Test findings

Kept separate so they do not crowd the source list.

### Four tests are compiled but never execute

| Test | Location | Why |
|---|---|---|
| `FinishJobTest::negativeDelay` | `test/clustertest/tests/FinishJobTest.cpp:353` | Absent from the `TEST(...)` list at `:48-65` |
| `FinishJobTest::positiveDelay` | `:376` | Same |
| `LibStuff::testUpperLower` | `test/tests/LibStuffTest.cpp:776` | Absent from the list at `:103-141` |
| `LeadingTest::standDownTimeout` | `test/clustertest/tests/LeadingTest.cpp:122` | Registration commented out at `:50`, "Disabled for speed" |

A full sweep of all 93 test files found no others. Note the fixture is `struct
LibStuff`, not `LibStuffTest` — that is only the filename.

`SToUpper`/`SToLower` lose their only *dedicated* coverage, but retain
incidental coverage: `SToLower` in live assertions at `MySQLTest.cpp:76,93` and
`MySQLUtilsTest.cpp`, `SToUpper` via `plugins/MySQL.cpp:50-139` and
`SIsValidSQLiteDateModifier`. A break would fail existing tests.

### `MySQLTest.cpp` duplicates `MySQLUtilsTest.cpp`

Both call the identical set of ten `MySQLUtils::` free functions — set
difference empty in both directions. Neither constructs a plugin, command,
server, or socket, so no dispatch path exists to exercise. `MySQLTest` has 6
tests / 26 assertions; `MySQLUtilsTest` has 12 / 81, a strict superset. Even the
"complex" `key_column_usage` case at `MySQLTest.cpp:150-192` is already covered
at `MySQLUtilsTest.cpp:260-290`.

The banner at `MySQLTest.cpp:43-52` claims end-to-end plugin coverage
"rather than just the utility functions" — contradicted by every line beneath
it. **Delete the file, or make it do what its comment says.**

### `BedrockClusterTester` should live in `test/lib`

It is `typedef ClusterTester<BedrockTester>` (`BedrockClusterTester.h:82`) — an
alias for a template instantiation holding `list<T> _cluster`, not a peer
harness. Its includes are `<libstuff/SData.h>` and `<test/lib/BedrockTester.h>`;
**it already depends on nothing in `test/clustertest`.** An earlier draft said
removing the `testplugin.so` block was a prerequisite — it is not; that block
uses only a runtime string path and creates no compile or link edge. The move is
unblocked today, and mechanical: all 36 consumers use the identical
`#include <test/clustertest/BedrockClusterTester.h>` spelling.

### One method crosses a directory boundary

`test/clustertest/tests/FinishJobTest.cpp:42` is the only file in
`test/clustertest` that includes anything from `test/tests`, and it needs
exactly one symbol: `JobTestHelper::getTimestampForDateTimeString`. The build
system documents the coupling explicitly:

```make
CLUSTERTESTCPP = $(shell find test -name '*.cpp' -not -path 'test/tests*' ...)
CLUSTERTESTCPP += test/tests/jobs/JobTestHelper.cpp
```

It excludes the entire single-node tree, then adds back precisely one file.
Move `JobTestHelper` to `test/lib`. Note that deleting the dead tests does not
remove the dependency: 2 of the 6 call sites are in `positiveDelay`, but 4 are
in live tests.

### Coverage gaps, by comparison with production structure

Of `plugins/`'s five implementations, only **Jobs** has mirrored test coverage.
**Cache, Compression, DB, and MySQL have none.** Also unnamed in any test:
`sqlitecluster`'s wire-format and pooling layer (`SQLiteClusterMessenger`,
`SQLitePool`, `SQLiteServer`, `SQLiteCommand`) and libstuff's networking/TLS
stack, `SData`, `SLog`/`SSignal`, and `SThread`.

Benchmarks cover only libstuff string functions. Two hot paths are untimed and
demonstrably matter: `SData::deserialize` (someone hand-tuned buffer padding for
a downstream parser) and the SQLite journal commit path (calls into Compression
on every committed write).

---

## Not defects — deliberate decisions worth documenting

**`libstuff/JSON` has zero in-repo production callers, by design.** An earlier
draft recommended finishing the migration or retiring the duplicate. Both are
wrong. `Makefile:69-72` builds it as a separate `libjson.a` "so embedding
applications can provide their own JSON symbols during migration";
`Makefile:25` links `--exclude-libs`; and `checkjsonsymbols`
(`Makefile:121-135`) **fails the build** if anything beyond
`JSON::setMetricsObserver` and `JSON::reportMetrics` is strong-exported. Zero
in-repo callers is the enforced, intended state.

The real observation is that Bedrock maintains **two JSON implementations** —
`JSON::Value` for embedders and `SParseJSONObject`/`SComposeJSONObject` for
itself. That is a deliberate cost. It should be visible in the architecture
docs, not discovered from a Makefile.

**Nothing here currently breaks.** `Makefile:25` links
`-Wl,--start-group -lbedrock -lstuff -ljson -Wl,--end-group`. Group linking
exists to resolve circular static-archive dependencies, so the build already
accommodates the mutual dependency between `libstuff.a` and `libbedrock.a`.
Treat this report as design debt with a measurable cost, not as a defect list
with urgency attached.

---

## Open questions requiring a human decision

1. **`BedrockPlugin_Jobs::upgradeDatabase` bakes Expensify-specific index
   literals** (`manual/SmartScan*`, `www-prod/*`, `www-stag/*`) into the schema
   DDL of an otherwise reusable job queue. Does Bedrock want a customization
   seam, or is it acknowledged as an Expensify-first product? A tooling pass
   cannot answer this.
2. **`SData::deserialize` over-allocates for a simdjson parser that does not
   exist in this repo.** `simdjson` appears in exactly one comment
   (`SData.cpp:159`) and zero code; no vendored copy exists. The consumer is
   out-of-repo. Either document the contract or drop the padding.
3. **`QueryTest::testPercentile`** tests a compiled-in SQLite extension rather
   than Bedrock's Query command. No owning unit is visible for it.
4. **`class_hierarchy.md`'s TODOs** — un-nesting `STCPManager::Socket`/`Port`,
   collapsing `STCPNode`/`SQLiteNode` — could not be confirmed resolved or
   outstanding from this analysis. A hand-maintained design doc with no
   verification hook drifts silently in either direction.

---

## What this analysis got wrong

Recorded because the failure modes generalize, and because a report that only
lists its successes is not calibrated.

| Claim | Corrected to |
|---|---|
| Dedicated units already exist for libstuff's stranded families | True for 1 of 6. Five need new units. `SQResult` is a data type, not an engine; `SHTTPSManager` is a transaction manager, not a grammar. |
| `SHTTPSManager.h` includes `BedrockPlugin.h` | Forward declaration, not an include. |
| `libstuff/JSON` is an unused duplicate to retire | Deliberate and CI-enforced for external embedders. |
| `SToUpper`/`SToLower` have zero coverage | No *dedicated* coverage; incidental coverage exists. |
| Removing the testplugin block unblocks the `BedrockClusterTester` move | Already unblocked; the cleanup is independent. |
| Three dead tests | Four. |
| Two dead includes in `SHTTPSManager.cpp` | Three. |
| `SSignal`'s `kill()` is unsafe in a signal handler | True but not a discovery — the handler is documented as unsafe by design. |

The pattern: **file-local and summary-level reasoning reliably identifies that
something is misplaced, and reliably guesses wrong about where it should go.**
Destination claims need verification against source. Diagnosis was ~90%
accurate; prescription was closer to 50%.
