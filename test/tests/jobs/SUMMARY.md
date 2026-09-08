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

No misfit in this batch needs a home outside `test/tests/jobs` — nothing
escalates.

<!-- ROLLUP
theme: Command-by-command test suite for the Jobs bedrock plugin, validating job state-machine transitions, priority/parent-child rules, and nextRun/repeat/retryAfter scheduling via BedrockTester-driven integration tests.
exports: [JobTestHelper::getTimestampForDateTimeString, tpunit fixture-per-command test pattern, integration coverage for CreateJob(s)/GetJob(s)/UpdateJob/RetryJob/FailJob/CancelJob/DeleteJob/RequeueJobs/QueryJob]
depends_on_dirs: [libstuff, test/lib]
depended_on_by: []
misfit_count: {high: 0, med: 0, low: 2}
resolved_locally: 2
escalate: []
-->
