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
