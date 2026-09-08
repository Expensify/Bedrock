# Cluster: command-bedrock-integration

## What these units have in common

Eight of the nine units are integration tests that drive a real `BedrockTester`
instance and assert on the wire-level response to a specific Bedrock command:
`Query` (QueryTest, ReadTest), `Write` (WriteTest), `Status` (StatusTest),
`CommandPort` suppress/clear (CommandPortTest), and outbound HTTP/HTTPS
(ChainedHTTPTest, SSLTest, via the `chainedrequest`/passthrough-request
commands). They share a common shape: spin up (or reuse) a tester, send an
`SData` request, assert on the response code/body. The ninth,
`BlockingCommandQueueTest`, is a pure unit test of
`BedrockBlockingCommandQueue`'s rate-limiting logic via a test-clock subclass —
no live command port involved. The cluster was formed by name/intent token
overlap on "command"/"Bedrock command handling," and while eight members
genuinely share the "exercise a Bedrock command end-to-end" pattern, that
overlap is looser than a hand-picked theme would be — see Misfits.

## Units

| Unit | Lines | Summary |
|---|---|---|
| BlockingCommandQueueTest | 313 | Unit tests (test-clock subclass, no live server) for per-identifier/per-command rate limiting in `BedrockBlockingCommandQueue`: sliding-window accounting, block-duration hold/clear, log-prefix restoration. |
| ChainedHTTPTest | 69 | Integration test: one `chainedrequest` command fans out to multiple external HTTP requests via the clustertest test plugin; checks each site's status. |
| CommandPortTest | 42 | Integration test for `SuppressCommandPort`/`ClearCommandPort`: reason recording, mismatched-clear no-op, matching-clear reopen. |
| QueryTest | 130 | Integration tests (8) for the `Query` command: malformed/missing/unterminated queries, multi-statement execution, no-WHERE DELETE guard, SQLite percentile/median aggregates. |
| ReadTest | 52 | Smoke tests for read-only `SELECT` via `Query`, with/without HTTP method line, plus malformed-query rejection. Generic name hides that it's specifically a Query-command test. |
| SSLTest | 97 | Integration tests for outbound HTTPS: passthrough requests to real sites, and a proxied transaction through `SHTTPSProxySocket`/`SStandaloneHTTPSManager`. |
| StatusTest | 21 | Smoke test that the `Status` command response includes expected diagnostic fields (plugins, checkpoint/freelist/page counts). |
| WriteTest | 219 | Integration tests (15) for the `Write` path: INSERT/UPDATE/DELETE, no-WHERE guard, parallel writes, HTTP-style and shorthand syntax, non-deterministic-SQL blocking on write. |

## Misfits

- **BlockingCommandQueueTest** (cluster fit, low): a unit test of internal
  queue rate-limiting behavior with a mocked clock — no `BedrockTester`, no
  live command port, no wire-level assertion. It shares only the word
  "command" with the other eight; it does not share their
  drive-a-real-command-and-check-the-response pattern. It clearly still fits
  `test/tests` (location_fit 5, per its own record) — **resolved-locally**:
  it belongs in the directory, just not conceptually in this cluster.
- **QueryTest::testPercentile** (sub-unit, low, flagged by the unit itself):
  verifies SQLite's compiled-in `SQLITE_ENABLE_PERCENTILE` aggregate
  functions — a build-flag/extension check riding inside the Query-command
  fixture rather than testing Bedrock's own command handling. Whether a
  better home exists (e.g. a dedicated SQLite-features test) can't be judged
  from this cluster alone — **escalate**, suggested_home unknown.
- **WriteTest::keywordsAsValue** (sub-unit, low, flagged by the unit itself):
  asserts on a known SQL-parsing limitation tracked in an external Expensify
  issue rather than intended behavior. Not a placement problem — it's a
  regression pin for known tech debt — **resolved-locally**, no relocation
  needed, but worth keeping visible as tracked debt rather than "expected
  behavior."
- Secondary, non-blocking observation: ChainedHTTPTest and SSLTest test
  *outbound* HTTP/HTTPS behavior (the server as an HTTP client) rather than
  inbound command handling proper. They still route through a Bedrock
  command (`chainedrequest`, passthrough sendrequest) via BedrockTester, so
  they're kept in-cluster, but they sit at the edge of the theme alongside
  the five single-command tests (Query/Read/Write/Status/CommandPort).

## Report notes

Input is unit-level records only (no source files, index, or sibling
clusters read), per the bounded-fan-in constraint.

<!-- ROLLUP
theme: BedrockTester-driven integration tests exercising individual Bedrock commands (Query/Write/Status/CommandPort/outbound-HTTP), plus one command-queue rate-limit unit test riding the same name-token cluster
exports: [BedrockTester single-command integration pattern, Query/Write path safety-guard coverage (no-WHERE guard, non-deterministic-SQL block), CommandPort suppress/clear lifecycle test, outbound HTTP/HTTPS integration coverage (chained fan-out, SSL passthrough+proxy), Status response field contract test, BedrockBlockingCommandQueue rate-limit unit coverage]
depends_on_dirs: [libstuff, test/lib, sqlitecluster]
depended_on_by: []
misfit_count: {high: 0, med: 0, low: 3}
resolved_locally: 2
escalate:
  - item: QueryTest::testPercentile
    from: test/tests/QueryTest.cpp
    why: tests a compiled-in SQLite extension (percentile/median aggregates), not Bedrock's Query command handling; unclear if a better home exists among unseen siblings
    suggested_home: null
-->
