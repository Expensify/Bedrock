# Cluster: raii-auto-clock (libstuff)

## What these units have in common

All three are small, single-purpose utility classes (33-77 lines each) built
around the same idiom: pair a "start" action with a deferred "stop" action
bracketing some section of code, and (in two of three cases) periodically log
a percentage breakdown of time spent. Two of the three are actual RAII scope
guards named with the `Auto*` convention (`AutoScopeOnPrepare`,
`AutoTimer`/`AutoTimerTime`); the third (`SPerformanceTimer`) follows the same
start/stop-and-log shape and the project's `S`-prefix convention but is not
itself an RAII guard — callers must call `start()`/`stop()` manually rather
than scoping an object. The cluster reads as "scope-bracketing /
timing-instrumentation utilities," not a single tight abstraction.

## Units

- **AutoScopeOnPrepare** (`AutoScopeOnPrepare.h/.cpp`, 33 lines) — RAII guard
  that installs a SQLite on-prepare callback for its lifetime and removes it
  on scope exit.
- **AutoTimer** (`AutoTimer.h/.cpp`, 66 lines) — accumulates wall-clock time
  spent in bracketed sections; `AutoTimerTime` is the RAII wrapper that calls
  `start()`/`stop()`; logs percentage of wall time spent every 10s.
- **SPerformanceTimer** (`SPerformanceTimer.h/.cpp`, 77 lines) — accumulates
  wall-clock time per named sub-phase via manual `start()`/`stop()` calls;
  logs a percentage breakdown every 10s. No RAII wrapper of its own.

## Misfits

- **AutoScopeOnPrepare (whole unit)** — fits this cluster's RAII/`Auto*`
  naming pattern fine, but per its own unit data it `#include`s
  `sqlitecluster/SQLite.h` directly and exists solely to scope a
  SQLite-specific feature (`setOnPrepareHandler`/`enablePrepareNotifications`).
  That is a *directory*-level misfit (libstuff is meant to be generic and
  dependency-light), not a cluster-level one — flagged here for propagation,
  suggested home `sqlitecluster`, alongside `SQLite`. Severity: med.
- **AutoTimer** — fits the cluster theme, but the unit's own header comment
  notes a second, unrelated class also named `AutoTimer` exists in
  `BedrockCore.h` (a per-command timing guard). Same name, different purpose,
  genuinely confusable. Resolving this needs visibility outside libstuff
  (wherever `BedrockCore.h`'s directory rolls up), so it isn't resolvable from
  this cluster alone. Severity: med.
- **SPerformanceTimer** — no misfit flagged in its own unit data, and its
  location/name fit scores are clean. Noted only as a soft observation: it
  and `AutoTimer` describe almost identical behavior (accumulate durations
  per bracketed/named section, log a percentage breakdown every 10s), one as
  a manual start/stop timer and the other as an RAII-wrapped one. Whether
  this is deliberate (different call-site ergonomics) or accidental
  duplication cannot be judged from unit metadata alone — it would take
  reading both implementations, which this pass's bounded fan-in does not
  allow. Flagging it as a possible dedup candidate for whoever next has both
  in view, not counting it as a formal misfit here.

## ROLLUP

<!-- ROLLUP
theme: RAII scope-guards and start/stop timing-instrumentation utilities (Auto* + SPerformanceTimer)
exports: [AutoScopeOnPrepare, AutoTimer, AutoTimerTime, SPerformanceTimer]
depends_on_dirs: [sqlitecluster]
depended_on_by: []
misfit_count: {high: 0, med: 2, low: 0}
resolved_locally: 0
escalate:
  - item: AutoScopeOnPrepare
    from: libstuff/AutoScopeOnPrepare.h,cpp
    why: SQLite-specific (includes sqlitecluster/SQLite.h), not generic libstuff material
    suggested_home: sqlitecluster
  - item: AutoTimer
    from: libstuff/AutoTimer.h,cpp
    why: name collides with an unrelated AutoTimer class in BedrockCore.h; resolving needs a view spanning both directories
    suggested_home: null
-->
