# Cluster: thread-back-logger

Input: `.arch/rollup_in/libstuff__thread-back-logger.json` (4 units, all non-test,
all directly in `libstuff/`). Pass C — cluster summary only; no source files,
`index.json`, or sibling clusters were read.

## 1. What these units have in common

All four are concurrency primitives built around the same shape: **push work
onto a background thread, and get the result (or failure) back to the caller
safely without the caller blocking on it directly.** Concretely:

- `SThread` is the generic building block: launch a callable on a
  `std::thread`, but capture any exception it throws and deliver it back
  through a `std::future` instead of letting it kill the process.
- `SRingBuffer` is the generic hand-off channel: a lock-free MPSC ring buffer
  so producer threads can push data to one consumer thread without locks.
- `SFluentdLogger` composes the two: callers push log records into an
  `SRingBuffer`, a single sender thread (started the way `SThread` starts
  threads) drains it and forwards to Fluentd over TCP, falling back to
  `syslog()` on failure.
- `SResolver` implements the same *pattern* independently: `SResolve()` starts
  a DNS lookup on a detached thread and hands back a shared `SResolution`
  object with its own PENDING/RESOLVED/FAILED state machine and a self-pipe
  fd, so either side can poll for completion without blocking.

So the cluster is really two generic primitives (`SThread`, `SRingBuffer`) plus
one concrete consumer of both (`SFluentdLogger`), plus one unit (`SResolver`)
that reinvents the same background-thread-with-safe-completion idea from
scratch rather than reusing the other two. The dependency graph confirms this:
`SFluentdLogger` → `SRingBuffer.h` + `SThread.h`; `SResolver` → only
`libstuff.h`.

## 2. One line per unit

| Unit | Files | Lines | Role |
|---|---|---|---|
| `SThread` | `SThread.h` | 66 | Exception-safe `std::thread` launcher: wraps a callable, routes its return value or thrown exception through a `future`. |
| `SRingBuffer` | `SRingBuffer.h` | 101 | Lock-free, fixed-capacity, multi-producer/single-consumer ring buffer template; today used only by `SFluentdLogger`. |
| `SFluentdLogger` | `SFluentdLogger.h/.cpp` | 132 | Async, non-blocking JSON log shipper to Fluentd over TCP, with a background sender thread and syslog fallback; built on `SRingBuffer` + `SThread`. |
| `SResolver` | `SResolver.h/.cpp` | 146 | Async, poll()-able DNS resolution on a detached thread, returning a shared, self-pipe-backed `SResolution` handle. |

## 3. Misfits

One genuine code-level misfit, already flagged at the unit level, plus one
cluster-composition observation:

- **`SRingBuffer::State`** (low severity, per source unit) — declared at file
  scope instead of nested inside `SRingBuffer`, so a generic five-letter enum
  name (`State`) leaks into every translation unit that includes
  `SRingBuffer.h`. Fits `libstuff` and fits this cluster; the fix is local to
  the file (nest the enum). Treated as **resolved-locally**, no escalation
  needed.
- **`SResolver` is a thematic, not structural, fit for this cluster.** It
  shares the surface pattern ("do work on a background thread, deliver
  completion back safely") that presumably drove the algorithmic clustering,
  but it shares no code or dependency with `SThread`/`SRingBuffer`/
  `SFluentdLogger` — it hand-rolls its own thread launch and its own
  completion signal (self-pipe + poll) instead of reusing either primitive.
  It belongs in `libstuff` (location_fit 5 in the source data) — this is not
  a misplacement, just a note that the cluster's real internal structure is
  "two primitives + one consumer" with `SResolver` standing apart. Not
  counted as a code misfit; nothing to fix.

Two naming-consistency notes worth carrying up but not rising to "misfit":
`SFluentdLogger`'s public static `instance`/`tag` implement a bare
process-wide singleton, a different pattern from the class's own private
members; and `SThread` is a PascalCase, `S`-prefixed **function template**,
which reads like a type/constructor given the repo's convention of reserving
that style for classes (source-flagged naming_quality 3/5).

<!-- ROLLUP
theme: Background-thread concurrency primitives (safe thread launch, lock-free MPSC hand-off) and their two independent consumers (async Fluentd log shipping, async DNS resolution)
exports: [SThread, SRingBuffer, SFluentdLogger, SResolution, SResolve]
depends_on_dirs: []
depended_on_by: []
misfit_count: {high: 0, med: 0, low: 1}
resolved_locally: 1
escalate: []
-->
