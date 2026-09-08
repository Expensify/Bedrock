# test/tests/jobs

## 1. Theme

This directory is the test suite for the Jobs bedrock plugin: one tpunit
fixture per plugin command (Create, CreateJobs, Get, GetJobs, Update, Retry,
Fail, Cancel, Delete, RequeueJobs, Query), plus a shared timestamp-parsing
helper. Collectively these tests pin down the job queue's state machine
(QUEUED/RUNQUEUED/RUNNING/FINISHED/FAILED), its priority/parent-child
semantics, and the nextRun/repeat/retryAfter scheduling arithmetic, each
driven end-to-end against a real server via `BedrockTester`. This is not
incidental test coverage — it is the executable spec for the Jobs command
set's behavior.

## 2. Contents

| Unit | Files | Lines | What it covers |
|---|---|---|---|
| CancelJobTest | CancelJobTest.cpp | 393 | CancelJob: rejection rules (nonexistent/has-children/non-cancellable state), correct QUEUED-child cancellation |
| CreateJobTest | CreateJobTest.cpp | 620 | CreateJob: defaults, priority/data/repeat, unique-job merge/overwrite, parent/child validity, retryAfter lifecycle |
| CreateJobsTest | CreateJobsTest.cpp | 266 | CreateJobs (bulk): JSON-array creation, malformed-JSON rejection, parent-running enforcement, mocked propagation, conflicts |
| DeleteJobTest | DeleteJobTest.cpp | 200 | DeleteJob: rejects nonexistent/has-children/RUNNING, allows FINISHED |
| FailJobTest | FailJobTest.cpp | 126 | FailJob: rejects nonexistent/wrong-state, marks RUNNING/RUNQUEUED as FAILED |
| FailedJobReplyTest | FailedJobReplyTest.cpp | 173 | GetJob/GetJobs requeue jobs whose reply can't be delivered, and re-fetchability afterward |
| GetJobTest | GetJobTest.cpp | 971 | GetJob/GetJobs: dequeue ordering, parent/child, errors, CrashBedrockJob GLOB blacklist |
| GetJobsTest | GetJobsTest.cpp | 130 | GetJobs (bulk): fetch moves jobs to RUNQUEUED with retryAfter nextRun; per-repeat-modifier reschedule on finish |
| InifiniteRetryAfterTest | InifiniteRetryAfterTest.cpp | 106 | retryAfter retry-limit: auto-fail after 10 tries; in-limit repeat clears retryAfterCount |
| JobTestHelper | JobTestHelper.h/.cpp | 17 | Shared static helper: parses jobs-table datetime string to time_t |
| QueryJobTest | QueryJobTest.cpp | 62 | QueryJob: field count/values for a given jobID |
| RequeueJobsTest | RequeueJobsTest.cpp | 282 | RequeueJobs: RUNNING/RUNQUEUED to QUEUED (single+bulk), auto-requeue, bulk rename, nextRun reset |
| RetryJobTest | RetryJobTest.cpp | 903 | RetryJob: state/precondition errors, data updates, full nextRun/repeat/delay precedence incl. re-anchoring |
| UpdateJobTest | UpdateJobTest.cpp | 153 | UpdateJob: data/repeat/priority/nextRun updates, numeric-string preservation, mocked jobs, shouldClearRepeat |

No subdirectories.

## 3. Coherence

Strongly coherent: every unit is a tpunit fixture named after, and scoped to,
exactly one Jobs plugin command (or, for `CreateJobsTest`/`GetJobsTest`, its
bulk variant), following the same Create/Get/Finish-then-assert pattern
against a shared `BedrockTester`. `JobTestHelper` is the one non-fixture file,
and it exists solely to serve the fixtures around it (datetime parsing needed
by `RetryJobTest`, `CreateJobTest`, `FailJobTest`, `RequeueJobsTest`,
`GetJobsTest`). Nothing here belongs to a different directory, and nothing
outside this pattern has crept in.

Naming/quality issues flagged by children that are file-internal (not
misfits, but worth surfacing since they recur across the set):
- `InifiniteRetryAfterTest.cpp` misspells "Infinite" in the filename even
  though the struct name (`InfiniteRetryAfterJobTest`) is spelled correctly.
- `FailedJobReplyTest.cpp` has a comment copy-pasted from `CancelJobTest`
  ("Cannot cancel a job with children") describing the wrong behavior.
- `UpdateJobTest.cpp` has no per-test `tearDown` resetting the jobs table
  (only `tearDownClass`), unlike its siblings — a latent test-isolation risk,
  not currently a failure.

## 4. Misfits

Two children flagged misfits; both are the same class of problem
(plain file-scope helper functions leaking into the test binary's global
namespace) and both have a fix that lives inside this directory, so both are
resolved here rather than escalated:

- **`isBetweenSecondsInclusive`** (GetJobTest.cpp) — plain global function,
  generic name, ODR-clash risk in a multi-file test binary.
  **resolved-locally**: wrap in an anonymous namespace or mark `static` in
  place; if another job test later needs it, promote it into
  `JobTestHelper.h`, which already exists in this directory for exactly this
  purpose.
- **`absoluteDiff`, `getAllJobData`** (GetJobsTest.cpp) — same pattern: two
  free functions at file scope instead of `static`/anonymous-namespace or
  fixture-member, unlike sibling files. **resolved-locally**: same fix
  (anonymous namespace/`static`), with `JobTestHelper.h` as the consolidation
  point if reuse across files ever arises.

No misfit *found bottom-up* needs a home outside `test/tests/jobs`. Pass B,
with parent and cross-branch context now available, adds one more that does
— see §5/§6.

## 5. Role in the system

Within `test/tests`, this directory's exclusive claim is the Jobs plugin.
The sibling clusters cover SQLite commit/write-path unit tests, socket/buffer
primitives, `BedrockTester` single-command integration (Query/Write/Status/
CommandPort/HTTP), and JSON::Value/Utils coverage — none of them mention a
Jobs command, and this directory doesn't stray into theirs. That boundary
holds cleanly inside `test/tests`.

It does not hold outside it. `test/clustertest/tests/FinishJobTest.cpp` — in
a different top-level test directory, reached across the parent's own
sibling boundary — `#include`s `test/tests/jobs/JobTestHelper.h` directly and
calls its one static method six times (confirmed by grep). From this
directory's own side, the honest answer to "is `test/tests/jobs` acting as a
shared fixture library it was never meant to be" is **yes**: this directory's
theme (§1) describes it as single-node, `BedrockTester`-driven Jobs-plugin
coverage, full stop, and `JobTestHelper`'s own intent is "a tiny shared
helper for the Jobs plugin test suite" — shared *within this directory*
(it already serves five sibling fixtures here: `CreateJobTest`, `FailJobTest`,
`GetJobsTest`, `RequeueJobsTest`, `RetryJobTest`). Nothing in either
description anticipated a consumer outside `test/tests` entirely. It has
ended up shared across a top-level test-tree boundary by accident of a
convenient absolute include path, not by design.

## 6. Inbound expectations

Nothing within `test/tests` depends on this directory beyond what its own
theme promises (Jobs-plugin coverage), and that is satisfied. Outward, the
parent's rollup already recorded that `test/clustertest/tests` depends
directly on this directory's `JobTestHelper`; that dependency is real and is
the one thing this directory currently exposes by accident rather than by
design. The fix isn't to keep it here and call it intentional —
`test/tests/jobs` was never described, by itself or by its parent, as a
shared-fixture location for other test suites — nor to duplicate the
parsing logic in the consumer. Both this directory and the consumer converge
on the same answer: the single static method
(`getTimestampForDateTimeString`) that both need belongs in `test/lib`,
alongside `BedrockTester` and the rest of the genuinely-shared test
infrastructure, not in a plugin-specific test directory that merely happens
to be reachable by path.

This is a misfit Pass A could not see: from a bottom-up view alone, a tiny
17-line internal helper used by five sibling files in the same directory
looks entirely appropriate right where it is — and `location_fit`/`name_fit`
both correctly scored it 5/5 for what Pass A could observe at the time. It
takes the parent's and the cross-branch sibling's context to see that the
same header has a consumer whose existence undercuts ever treating this as
purely single-node-suite-local. Added as a new escalate entry below, with
`misfit_count` incremented accordingly.

<!-- ROLLUP
theme: Command-by-command test suite for the Jobs bedrock plugin, validating job state-machine transitions, priority/parent-child rules, and nextRun/repeat/retryAfter scheduling via BedrockTester-driven integration tests.
exports: [JobTestHelper::getTimestampForDateTimeString, tpunit fixture-per-command test pattern, integration coverage for CreateJob(s)/GetJob(s)/UpdateJob/RetryJob/FailJob/CancelJob/DeleteJob/RequeueJobs/QueryJob]
depends_on_dirs: [libstuff, test/lib]
depended_on_by: [test/clustertest/tests]
misfit_count: {high: 0, med: 1, low: 2}
resolved_locally: 2
escalate:
  - item: JobTestHelper::getTimestampForDateTimeString
    from: test/tests/jobs/JobTestHelper.h
    why: reached directly by a consumer entirely outside test/tests (test/clustertest/tests/FinishJobTest.cpp), despite this directory being scoped and described as single-node Jobs-plugin coverage only, not a shared-fixture library
    suggested_home: test/lib
-->
