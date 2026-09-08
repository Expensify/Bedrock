# test/tests

## 1. Theme

`test/tests` is Bedrock's flat top-level regression suite: one tpunit fixture
per production symbol or command being verified, compiled into a single test
binary. Two-thirds of it is BedrockTester-driven integration coverage of the
server's actual command surface (`Query`/`Write`/`Status`/`CommandPort`,
outbound HTTP passthrough, plus the queue-level rate limiter that guards
those commands); the rest is unit coverage of the support libraries that
surface bugs cheaper as pure unit tests — `libstuff`'s JSON value/parser/utils
API, SQLite handle commit/rollback semantics, and assorted `libstuff`
primitives (DNS resolution, ring buffers, string/date validators, Unicode
transliteration). One plugin — Jobs — is large and self-contained enough to
get its own subdirectory (`jobs/`) rather than living as loose files here.
The job this directory does for the system: it *is* the executable spec for
"does the server do what a command promises," at both the command-dispatch
level and the libstuff-library level underneath it.

## 2. Contents

| Child | Kind | What it covers |
|---|---|---|
| `jobs/` | subdirectory | Full command-by-command test suite for the Jobs plugin (11 fixtures + shared helper); already has its own SUMMARY.md. |
| `AfterCommitCallbackTest` | unit | SQLite after-commit callback fires once per commit, not on rollback/lost commit, shared across a copied handle. |
| `AsyncResolveTest` | unit | Deferred DNS resolution in `STCPManager::Socket`/`SResolver`: sync/deferred paths, failure/wake behavior. |
| `BlockingCommandQueueTest` | unit | `BedrockBlockingCommandQueue` rate limiting: sliding-window accounting, block-duration hold/clear, via a mocked clock. |
| `ChainedHTTPTest` | integration | `chainedrequest` command fanning out to multiple external HTTP requests. |
| `CommandPortTest` | integration | `SuppressCommandPort`/`ClearCommandPort` reason-recording and reopen semantics. |
| `FastHTTPParsing` | unit | `SFastBuffer::startsWithHTTPRequest()` across line endings and split buffers. |
| `JSONParserTest` | unit | `JSON::Value::parse()`: scalars, unicode escapes, nested structures, malformed input. |
| `JSONTest` | unit | `JSON::Value` parse/serialize round-trip, uint64 boundary, equality, `MetricsObserver` hook. |
| `JSONUtilsTest` | unit | `JSON::Utils` merge/merge-patch (RFC 7396), field-stripping, key search, sanitization, path parsing. |
| `JSONValueTest` | unit | `JSON::Value` construction, type checks, containers, cast/copy/move/alias semantics. |
| `LibStuffTest` | unit | Broad libstuff smoke test — crypto, HTTP parse/compose, string/regex helpers, file IO, SQLite bound params (JSON is 2 of ~37 methods). |
| `MySQLTest` | unit | Claims plugin-level end-to-end coverage; in practice reruns `MySQLUtils::` cases already in `MySQLUtilsTest`. |
| `MySQLUtilsTest` | unit | `MySQLUtils` query pattern-matching/rewriting (`VERSION()`, `information_schema`, `SHOW KEYS`, FK lookups). |
| `QueryTest` | integration | `Query` command: malformed/multi-statement queries, no-WHERE DELETE guard, SQLite percentile/median aggregates. |
| `ReadTest` | integration | Read-only `SELECT` via `Query`, smoke + malformed-query rejection. |
| `SDeburrTest` | unit | `SDeburr::deburr` Unicode-to-ASCII transliteration. |
| `SFluentdLoggerTest` | unit | `SFluentdLogger` async buffering/delivery of NDJSON to a Fluentd-style endpoint. |
| `SIsValidSQLiteDateModifierTest` | unit | `SIsValidSQLiteDateModifier` string validation (pure parsing, no DB handle). |
| `SQLiteNodeTest` | unit | `SQLiteNode` sync-peer selection/lookup in the replication layer. |
| `SRingBufferTest` | unit | `SRingBuffer` push/pop, capacity, FIFO, producer/consumer shutdown-and-wake handshake. |
| `SSLTest` | integration | Outbound HTTPS passthrough and proxied transactions via `SHTTPSProxySocket`. |
| `STimeTest` | unit | `STimestampToEpoch`/`STimestampMSToEpoch` conversion. |
| `StatusTest` | integration | `Status` command response includes expected diagnostic fields. |
| `WriteLocalUnreplicatedTest` | unit | `SQLite::writeLocalUnreplicated()` commit-count/journal invariants and rollback. |
| `WriteTest` | integration | `Write` path: INSERT/UPDATE/DELETE, no-WHERE guard, parallel writes, non-deterministic-SQL blocking. |

## 3. Coherence

The four pre-computed clusters vary sharply in how real they are, and two of
the cluster agents said so themselves: `json-unit-value` covers a genuine
theme for only 4 of its 9 units (the JSON API tests), with MySQL/STime/
SFluentdLogger swept in on generic "test"/"unit" token overlap and
`LibStuffTest` swept in on an incidental JSON mention. `sqlite-unit-commit`
is genuine for only 2 of its 5 (the two commit/rollback tests), with
`SDeburrTest`, `SIsValidSQLiteDateModifierTest`, and `SQLiteNodeTest` pulled
in on nothing more than the literal substring "SQLite". `fast-wake-buffer`
is half-real (2 of 3: async DNS + ring buffer share a genuine wake/poll
mechanism; `FastHTTPParsing` doesn't). Only `command-bedrock-integration`
is substantially real: 8 of its 9 units genuinely share the
BedrockTester-drives-a-command-and-checks-the-response pattern, with only
`BlockingCommandQueueTest` riding in on the word "command" alone.

Taken together, this says something about how `test/tests` is actually
organized: **not by test-scenario theme, but by production symbol** — one
fixture per class, free function, or command, named `FooTest` for `Foo`,
sitting flat in the directory in no particular order. The clustering
algorithm found real signal only where a whole family of files already
shares the same *kind* of subject (Bedrock's core commands); it manufactured
false themes wherever multiple unrelated symbols happen to share a common
English word ("SQLite", "test", "unit", "command"). This directory is a
symbol-indexed pile, not a theme-indexed one — the single case of intentional
grouping-by-theme in the whole tree is `jobs/`, which was pulled into its own
subdirectory precisely because it is one plugin's *entire* command surface
(11 files), not because anyone grouped it by keyword. Structurally, the 8
genuine `command-bedrock-integration` members are the same shape as `jobs/`'s
contents (one core-Bedrock-command per file) just not yet given the same
subdirectory treatment — which is a much stronger observation than "clusters
2 and 4 are noisy."

`jobs/` itself is unambiguously coherent (per its own SUMMARY) and needs no
revisiting here.

## 4. Misfits

Consolidating what the four clusters and `jobs/` flagged, and being decisive
rather than re-forwarding what a cluster already settled:

**Cluster-fit artifacts (not real misfits — these units fit `test/tests`
fine, they were just mis-clustered by the algorithm). Resolved locally, no
action needed:** `SDeburrTest`, `SIsValidSQLiteDateModifierTest`,
`SQLiteNodeTest` (wrongly swept into `sqlite-unit-commit`); `FastHTTPParsing`
(wrongly swept into `fast-wake-buffer`); `BlockingCommandQueueTest` (wrongly
swept into `command-bedrock-integration`); `MySQLUtilsTest`,
`SFluentdLoggerTest`, `STimeTest`, `LibStuffTest` (wrongly swept into
`json-unit-value`). Two of the source cluster files left these uncounted as
resolved-vs-escalated in their own ROLLUP math (`sqlite-unit-commit` shows
low:3 but resolved_locally:0, escalate: []) — that's a small spec gap in
those two children's bookkeeping, not a real ambiguity; the substance is
clear and closed here.

**Real, actionable misfits:**

- **`MySQLTest.cpp` (whole file), high.** Flagged by `json-unit-value` as
  duplicate coverage of `MySQLUtilsTest` with no plugin dispatch exercised,
  despite its own docstring claiming plugin-level end-to-end testing. The
  cluster escalated this because deleting/merging a file is a judgment call —
  but its own suggested fix (merge into `test/tests/MySQLUtilsTest.cpp`) is
  a move *within this directory*, so it belongs here, not further up.
  **resolved-locally**: merge `MySQLTest.cpp`'s cases into
  `MySQLUtilsTest.cpp` and delete the file, or replace it with a genuine
  plugin-dispatch integration test if that coverage is wanted.
- **`LibStuffTest::testUpperLower`, med.** Written but never registered in
  the fixture's `TEST(...)` list, so `SToUpper`/`SToLower` have no live
  coverage. **resolved-locally**: one-line addition inside
  `LibStuffTest.cpp`.
- **`AsyncResolveTest.cpp` naming, low.** Fixture/global instance named
  `AsyncResolve`/`__AsyncResolve` instead of the `Foo`/`FooTest`/`__FooTest`
  convention every sibling follows. **resolved-locally**: rename in place.
- **`WriteTest::keywordsAsValue`, low.** Pins a known SQL-parsing limitation
  tracked in an external issue, not a placement problem. **resolved-locally**:
  keep, but track as debt rather than intended behavior.
- **`JSONParserTest` vs. `JSONTest`, low.** Undocumented overlap in
  nested-`Value`-parsing coverage between the two files. **resolved-locally**:
  document the split or consolidate the overlapping cases.
- **`QueryTest::testPercentile`, low.** Tests a compiled-in SQLite extension
  (percentile/median aggregates), not Bedrock's own `Query` command handling.
  Whether a better home exists (a dedicated SQLite-features test elsewhere in
  the repo) genuinely can't be judged from this directory alone.
  **escalate** — the only item this directory forwards upward.

<!-- ROLLUP
theme: Bedrock's flat top-level test suite — one BedrockTester integration fixture per core server command, one unit fixture per libstuff/JSON support-library symbol, plus a dedicated Jobs-plugin subdirectory
exports: [BedrockTester single-command integration pattern (Query/Write/Status/CommandPort/outbound-HTTP), BedrockBlockingCommandQueue rate-limit coverage, JSON::Value/Parser/Utils unit-test coverage, SQLite commit/rollback and SQLiteNode peer-selection coverage, libstuff primitive coverage (async DNS, ring buffer, string/date validators), Jobs-plugin command-suite coverage (via jobs/)]
depends_on_dirs: [libstuff, libstuff/JSON, plugins, sqlitecluster, test/lib]
depended_on_by: []
misfit_count: {high: 1, med: 1, low: 12}
resolved_locally: 13
escalate:
  - item: QueryTest::testPercentile
    from: test/tests/QueryTest.cpp
    why: tests a compiled-in SQLite extension (percentile/median aggregates), not Bedrock's Query command handling; unclear if a better home exists among unseen siblings
    suggested_home: null
-->
