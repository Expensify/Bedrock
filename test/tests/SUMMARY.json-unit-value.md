# Cluster: json-unit-value (test/tests)

## 1. What these units have in common

Weakly, and only for 4 of the 9: `JSONParserTest`, `JSONTest`, `JSONUtilsTest`,
and `JSONValueTest` are genuine unit tests of the `libstuff/JSON/*` API
(`Value`, `Parser`, `Utils`, `Metrics`) — parsing, serializing, merging,
sanitizing, and the value container's construction/copy/cast semantics. That
much is a coherent sub-group: four fixtures, one API surface, split across
Value-construction vs. Value-parsing vs. Value-serialize/metrics vs.
Utils-level helpers.

The other 5 units — `LibStuffTest`, `MySQLTest`, `MySQLUtilsTest`,
`SFluentdLoggerTest`, `STimeTest` — do not share that theme. `LibStuffTest`
touches JSON only incidentally (two of its ~37 methods, `testJSONDecode`/
`testJSON`, exercise JSON parse/compose as one of many libstuff utilities it
smoke-tests). The MySQL, SFluentdLogger, and STime tests have no JSON content
at all; they were almost certainly pulled in by the algorithm on the token
"test"/"unit"/generic-fixture overlap, or on `LibStuffTest`'s JSON mention,
rather than on any real intent match. **This cluster is not thematically
coherent as grouped** — it reads as "JSON value/parsing tests" plus four
unrelated fixtures that happen to sit in the same directory.

## 2. One line per unit

| Unit | Lines | Fits cluster theme? | Summary |
|---|---:|---|---|
| `JSONParserTest` | 189 | yes | `JSON::Value::parse()`: scalar types, unicode escapes, nested arrays/objects, malformed input. |
| `JSONTest` | 139 | yes | `JSON::Value` parse/serialize round-trip, exact-uint64 boundary, equality, and the `JSON::MetricsObserver` hook. |
| `JSONUtilsTest` | 453 | yes | `JSON::Utils` merge/merge-patch (RFC 7396 + SQLite-compatible mode), field-stripping, key search, transport sanitization, JSON-path parsing. |
| `JSONValueTest` | 862 | yes | `JSON::Value` construction, type checks, array/object containers, indexing, merge/path access, cast operators, copy/move/alias semantics. |
| `LibStuffTest` | 1283 | partial | Broad libstuff smoke test (crypto, HTTP parse/compose, string/regex helpers, file IO, SQLite bound params); JSON is 2 of ~37 methods. |
| `MySQLTest` | 157 | no | Claims to be a plugin-level end-to-end test but only reruns `MySQLUtils::` free functions already covered by `MySQLUtilsTest`. |
| `MySQLUtilsTest` | 285 | no | `MySQLUtils` query pattern-matching/rewriting: `VERSION()`, `information_schema`, `SHOW KEYS`, FK lookups, table-name extraction. |
| `SFluentdLoggerTest` | 136 | no | `SFluentdLogger` async buffering/delivery of NDJSON lines to a Fluentd-style TCP endpoint. |
| `STimeTest` | 42 | no | `STimestampToEpoch`/`STimestampMSToEpoch` epoch conversion. |

## 3. Misfits

**Cluster-level (does not fit this cluster's theme, but fits the directory
fine — resolvable at the directory level, not a code problem):**
`MySQLTest`, `MySQLUtilsTest`, `SFluentdLoggerTest`, `STimeTest` have zero JSON
content; `LibStuffTest` has only incidental JSON content. All five belong in
`test/tests/` — they're just mis-clustered here. Since this is the only
cluster file in which these four/five units surface to me, I've still listed
them above in full so the directory-level Pass A doesn't lose them; the
directory's Pass A should treat this as a clustering artifact, not a code
issue.

**Content-level (from unit data, real issues independent of clustering):**

- **`MySQLTest.cpp` (whole file), high severity.** Its own header comment
  claims to test "the actual plugin behavior end-to-end," but every one of
  its 6 tests calls the same `MySQLUtils::` free functions `MySQLUtilsTest`
  already covers, with fewer cases and no plugin dispatch/socket-protocol
  exercised. The claimed integration-vs-unit distinction between the two
  files does not hold in practice — this reads as duplicated coverage.
  Suggested home is inside this same directory (merge into
  `test/tests/MySQLUtilsTest.cpp`, or replace with a genuine plugin-level
  integration test), but deleting/merging a whole test file is a judgment
  call, so I'm escalating it rather than declaring it resolved.
- **`LibStuffTest::testUpperLower`, med severity.** Fully written test method
  for `SToUpper`/`SToLower` is never added to the fixture's `TEST(...)`
  registration list, so it never runs — those two functions have no live
  coverage. Fix is a one-line addition inside the same file; resolvable
  locally.
- **`JSONParserTest` vs. `JSONTest`, low severity.** Both files parse nested
  `JSON::Value` structures with no documented split of responsibility
  (`JSONParserTest` covers value-shape/edge-case parsing, `JSONTest` covers
  serialize round-trip/equality/metrics, but the nested-parsing coverage
  overlaps). Both files are in this same directory; resolvable locally
  (document the split, or consolidate the overlapping cases).

## Input adequacy note

The unit records here are sufficient for the calls made above. No source
files, `index.json`, or other cluster files were read, per the hard
constraint.

<!-- ROLLUP
theme: JSON::Value/JSON::Utils unit-test coverage (4 of 9 units) plus four unrelated fixtures (MySQL, SFluentdLogger, STime) and one broad libstuff smoke test mis-clustered in by token overlap
exports: [JSON::Value construction/cast/copy-semantics coverage, JSON::Value parse/serialize/metrics-hook coverage, JSON::Utils merge/merge-patch/sanitize/path coverage, libstuff free-function smoke coverage, MySQLUtils query-rewrite coverage, SFluentdLogger async-delivery coverage, STime epoch-conversion coverage]
depends_on_dirs: [libstuff, libstuff/JSON, plugins, sqlitecluster, test/lib]
depended_on_by: []
misfit_count: {high: 1, med: 1, low: 5}
resolved_locally: 6
escalate:
  - item: MySQLTest.cpp (whole file)
    from: test/tests/MySQLTest.cpp
    why: docstring claims plugin-level end-to-end coverage but every test reruns MySQLUtils:: free-function cases already covered by MySQLUtilsTest.cpp, with no plugin dispatch exercised
    suggested_home: merge into test/tests/MySQLUtilsTest.cpp, or replace with a real plugin-level integration test
-->
