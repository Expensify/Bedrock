# Corrections found by Phase 4 verification

Claims that earlier phases stated with more confidence than the source supports.
Recorded because a refactoring report is only useful if its factual claims hold.

| Earlier claim | Verdict | Correction |
|---|---|---|
| `libstuff/JSON` is an unused parallel implementation; finish the migration or retire the duplicate | MISLEADING | The separation is deliberate and **build-enforced**. `Makefile:69-72` builds it as a separate `libjson.a` "so embedding applications can provide their own JSON symbols during migration"; `Makefile:25` links `--exclude-libs`; `Makefile:121-135` (`checkjsonsymbols`) **fails the build** if anything but `JSON::setMetricsObserver`/`reportMetrics` is strong-exported. Zero in-repo callers is the intended, CI-protected state. Report the dual-implementation cost as deliberate, not vestigial. |
| `SToUpper`/`SToLower` have zero coverage | OVERSTATED | No dedicated test, but incidental live coverage exists: `SToLower` in assertions at `test/tests/MySQLTest.cpp:76,93` and `MySQLUtilsTest.cpp:90,94,98,125,129`; `SToUpper` via `plugins/MySQL.cpp:50-139` and `libstuff/libstuff.cpp:3332`. Say "no direct unit coverage", not "no coverage". |
| Removing the `testplugin.so` block unblocks moving `BedrockClusterTester` | WRONG PREMISE | The header already depends on nothing in `test/clustertest` (includes: `<libstuff/SData.h>`, `<test/lib/BedrockTester.h>`). The block uses only a runtime string path — no compile or link edge. The cleanup is independent; the move is unblocked today. |
| `LibStuffTest::testUpperLower` | WRONG SYMBOL | The fixture is `struct LibStuff`; `LibStuffTest` is only the filename. Cite `LibStuff::testUpperLower`. |
| Three dead tests | INCOMPLETE | A fourth exists: `LeadingTest::standDownTimeout`, commented out at `test/clustertest/tests/LeadingTest.cpp:50` ("Disabled for speed"), implemented at `:122`. |
| `SToJSON` is a production JSON entry point | OVERSTATED | No direct production caller; live only as an internal helper of `SComposeJSONObject` (`libstuff.cpp:1653`) and the `SComposeJSONArray` template (`libstuff.h:739`). |
| `BedrockClusterTester` is "a generic composition over BedrockTester" | LOOSE | It is a `typedef ClusterTester<BedrockTester>` (`BedrockClusterTester.h:82`). The template holds `list<T> _cluster` — it contains N testers; the name is an alias, not a class. |

## Caveat carried into the refactor plan

Deleting the dead `FinishJobTest` methods does NOT remove the cross-directory
`JobTestHelper` dependency: 2 of the 6 call sites are in the dead
`positiveDelay`, but 4 (`:434,435,498,556`) are in registered, live tests.

## Layering verification (second pass)

| Earlier claim | Verdict | Correction |
|---|---|---|
| `libstuff/SHTTPSManager.h` depends on `BedrockPlugin.h` | **REFUTED** | The header includes only `SData.h` and `STCPManager.h`. The reference is a forward declaration `class BedrockPlugin;` (`SHTTPSManager.h:42`) — minimal coupling, no compile-time reach. Stating it as an include is factually wrong. The real finding: `SStandaloneHTTPSManager` (`:44-98`) is complete and plugin-free, while `SHTTPSManager` (`:100-108`) is a 9-line subclass adding only `BedrockPlugin& plugin`, never read in-repo. |
| ...therefore the coupling is not load-bearing | **DOES NOT FOLLOW** | `plugin` is `protected` and Bedrock `dlopen`s out-of-tree plugins (`main.cpp:188`, linked `-rdynamic`). External subclasses may use it. Removing the subclass is a potential API break for closed-source consumers. |
| `SHTTPSManager.cpp` has 2 dead includes | **UNDERSTATED** | Three: `BedrockServer.h` (:39), `sqlitecluster/SQLiteNode.h` (:41), and `BedrockPlugin.h` (:38). Verified by deletion + `g++ -fsyntax-only -std=c++20 -I.` → exit 0. Removing all three severs the file's entire upward dependency. |
| sqlitecluster depends on an application-level **command** plugin | **IMPRECISE** | `BedrockPlugin_Compression::getCommand` returns `nullptr` (`plugins/Compression.cpp:61-64`). It is a codec registered through the plugin mechanism, not a command handler. |
| The Compression coupling is an inverted include | **UNDERSTATED** | It is a three-layer include cycle: `plugins/Compression.h:51` → `BedrockPlugin.h` → `BedrockCommand.h:50,51` → `sqlitecluster/SQLiteCommand.h:31` → `SQLiteNode.h`. And it is load-bearing at runtime: `_dictionaries` is populated only via `initializeFromDB` → `loadDictionariesFromDB` (`Compression.cpp:66-68`) from `BedrockServer.cpp:188`, so journal compression silently requires plugin registration. |
| `SQLiteNode` calls decompress in `_handleBeginTransaction` | **INCOMPLETE** | Also `_recvSynchronize` (`SQLiteNode.cpp:1874`). The unit's own inserted SUMMARY misses this — do not inherit the error. |
| `SSignal`'s `kill()` call is unsafe in a signal handler | **TRUE BUT MISLEADING AS A HEADLINE** | The handler is already unsafe by design and says so at `SSignal.cpp:266-269` (`malloc`, `backtrace`, `__cxa_demangle`, syslog). `kill()` is placed last, after logging, immediately before `abort()` (:350), so a hang costs only port release. Report the layering point; do not present signal-unsafety as a discovery. |
| The intended layering is documented | **OVERSTATED** | `HIERARCHY.md` is this analysis's own generated tree, not repo documentation. `class_hierarchy.md` covers class relationships, not directory layering. The best textual evidence is `libstuff/README.md:2`. Do not claim a documented four-layer order. |

### Completeness check
All upward includes from libstuff were enumerated: exactly five, mapping
one-to-one onto the claims above. No libstuff violations were missed.

### Why none of this currently breaks
`Makefile:25` links `-Wl,--start-group -lbedrock -lstuff -ljson -Wl,--end-group`.
Group linking exists to resolve circular static-archive dependencies, so the
build already concedes libstuff.a and libbedrock.a are mutually dependent.
Nothing is broken today — which argues against urgency framing, and for
presenting these as design debt rather than defects.

### Recommended report order
`AutoScopeOnPrepare` and the dead includes first (unambiguous,
compiler-verified, zero risk), then Compression (the only one with runtime
consequences), then SSignal as a pure layering point, then SHTTPSManager
rewritten to target the 9-line subclass rather than a nonexistent include.

## libstuff decomposition verification (third pass) — the largest correction

The DIAGNOSIS holds. Six families occupy **1,928 of 3,816 real lines in
libstuff.cpp (50.5%)**, excluding this project's inserted SUMMARY block
(libstuff.cpp 1-92, libstuff.h 1-115).

The PRESCRIPTION was wrong on the three biggest families. Repeated throughout
this analysis was the claim that "dedicated units already exist, so the question
is why this never moved." In each of these three, the named destination exists
but owns a DIFFERENT concern.

| Family | Lines | Claimed destination | Verdict |
|---|---|---|---|
| SQLite engine (`SQuery`/`SQVerifyTable*`/`SQList`) | 482 | `SQResult`/`SQValue`/`SQliteParameter` | **REFUTED.** Those are the data types `SQuery` consumes. `SQResult.cpp`, `SQliteParameter.cpp`, `SQResultFormatter.cpp` contain zero `sqlite3_` calls; `SQValue.cpp` has one. No execution unit exists — needs a NEW one. |
| HTTP grammar (`SParseHTTP`/`SComposeHTTP`/`SParseURI*`) | 645 | `SHTTPSManager` | **REFUTED.** `SHTTPSManager` derives from `STCPManager` and manages Transaction lifecycle. It touches the grammar at exactly 2 sites, both `SParseURI` (`SHTTPSManager.cpp:251,268`). The real owner-consumer is `SData.cpp:46,142,147,157`. Needs a NEW unit. |
| Socket primitives (`S_socket`/`S_poll`/`SFDset`...) | 467 | `STCPManager` ("only real caller") | **REFUTED.** 14 call sites across 10 files outside STCPManager, including the application's own poll loops: `BedrockServer.cpp:336,1235-1244,2183`, `sqlitecluster/SQLiteNode.cpp:2182,2201`, `BedrockCommand.cpp:217`, `main.cpp:452`. |
| Crypto (AES/SHA/base64/HMAC) | 170 | a new `SCrypto` unit | **CONFIRMED** — and the cleanest. Two disjoint regions; only outward dependency is the `SASSERT` macro. `SSSLState` is not a counterexample (disjoint mbedtls surface). |
| Syslog transport | 68 | `SLog.cpp` | **CONFIRMED.** `SLog.cpp` is only 178 lines / 5 symbols and holds no transport. Precedent: `SFluentdLogger` was extracted as a class but its free-function facade stayed behind (`libstuff.cpp:384-425`) — the same leave-the-facade pattern, twice. |
| Gzip | 102 | — | **CONFIRMED but smaller and stranger.** `SGZip` has exactly one production caller (`SComposeHTTP`, `libstuff.cpp:1472`) so it is an HTTP helper; `SGUnzip` has ZERO production callers — only `test/tests/LibStuffTest.cpp:540`. Effectively dead. |

### New defect found during verification

`libstuff.h:118` includes `qrf.h`; `qrf.h:21` includes `sqlite3.h`. So all **78**
translation units that include `libstuff.h` parse the SQLite C API —
**+3,272 preprocessed lines each** (libstuff.h expands to 94,287). Only ~4
subsystems use `sqlite3_qrf_spec`. `libstuff.h:142` still carries a now-dead
`struct sqlite3;` forward declaration: the author intended to keep SQLite out of
the header, and `qrf.h` silently defeated it.

### Counter-arguments tested

- **Inline performance — fails.** No function in any of the six families is
  defined inline or as a template in the header. Moving them costs zero
  inlining. (One constraint: `SQList<Container>` at `libstuff.h:855` calls
  `SQ()`, so a SQL extraction must carry the `SQ` declarations.)
- **Circular dependency — real for exactly one family.** `fd_map`
  (`libstuff.h:763`) is the typedef the whole application's poll loop is written
  against, and cannot follow `S_poll`/`SFDset` into `STCPManager.h` because that
  header already includes `libstuff.h`. **This is why the socket family never
  moved, and it is legitimate.** For HTTP the analogous cycle is already solved
  and shipping (`libstuff.h:145` forward-declares `struct SData`), so there it
  is a cost, not a blocker.
- **Plugin ABI — real but narrow.** `bedrock` links `-rdynamic`; plugins are
  `dlopen`ed and call these free functions with no linkage of their own
  (`TestPlugin.cpp:889` calls `SParseURI`). That explains the blanket
  never-mark-anything-static habit. It does NOT defend leaving code in place:
  moving a function to another `.cpp` in the same binary changes neither its
  mangled name nor its dynamic-symbol export. The ABI constrains renaming and
  hiding, not moving.

### Extraction order, by cost

1. **Crypto** (170 ln) — self-contained but for `SASSERT`; new unit.
2. **Syslog transport** (68 ln) — `SLog.cpp` is the obvious home.
3. **Gzip** (102 ln) — audit `SGUnzip` for deletion first.
4. **HTTP** (645 ln) — new unit; `SData` cycle to manage, technique already proven in-repo.
5. **SQLite** (482 ln) — new unit; also fixes the `qrf.h` → `sqlite3.h` leak into 78 TUs.
6. **Sockets** (467 ln) — genuinely blocked on `fd_map`. Do last, or not at all.
