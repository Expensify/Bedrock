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
