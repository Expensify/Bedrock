# Cluster: trace-wide-stack (libstuff)

## What these units have in common

Both units install process-wide diagnostic machinery that only makes sense
once per process: global mutable state (a log mask, a pending-signal
bitmask), guarded by atomics, that is read or written from anywhere via
free functions rather than an instance. Both exist to answer "what was this
process doing when something went wrong" — `SLog` can dump a stack trace on
demand at any log level, and `SSignal`'s whole job is to catch a crash
signal, capture a stack trace, and get it to disk/log before the process
dies. Neither has its own header: both publish their API through the
`libstuff.h` catch-all instead of a dedicated `SLog.h` / `SSignal.h`, which
is itself the one structural trait tying them together as a cluster (as
opposed to just "both are diagnostics").

## Units

- **SLog** (`libstuff/SLog.cpp`, 141 lines) — process-wide log-level bitmask
  (`_g_SLogMask`) read by the `SLOG` macros, on-demand stack-trace dumping
  (`SLogStackTrace`), and a CAS-swapped whitelist (`PARAMS_WHITELIST`) that
  redacts (or, if `!GLOBAL_IS_LIVE`, throws on) unlisted structured log
  params.
- **SSignal** (`libstuff/SSignal.cpp`, 307 lines) — installs POSIX signal
  handlers and a background signal-dispatch thread; crash signals get a
  best-effort demangled backtrace logged and written to a crash file before
  `abort()`, other signals set bits in a pending bitmask (`SGetSignal` /
  `SCheckSignal` / `SClearSignals`) that can optionally wake a poll loop via
  `SSIGNAL_NOTIFY_INTERRUPT`.

## Misfits

- **Both units: no dedicated header.** `SLog`'s and `SSignal`'s public APIs
  are declared in `libstuff.h`'s catch-all instead of `SLog.h` / `SSignal.h`,
  unlike the other already-separated libstuff units (per each unit's own
  `location_fit` note). This is a shared, low-severity misfit — it fits the
  cluster theme (it's *why* they cluster) but is a directory-level
  header-organization question, not something resolvable within the cluster
  itself.
- **SSignal → sqlitecluster: `_SSignal_StackTrace` calls
  `SQLiteNode::KILLABLE_SQLITE_NODE->kill()`** on crash. This is a generic
  libstuff crash handler reaching directly into the sqlitecluster layer to
  kill peer connections — severity med, no suggested home identified at the
  unit level. This does not fit *this* cluster's theme (stack-trace/logging
  infrastructure) at all; it's a layering violation between libstuff and a
  higher-level subsystem and should be escalated past the directory, not
  treated as a naming/location quirk like the others.
- **SSignal: hardcoded `/tmp/bedrock_crash_{}.log` path** — bakes a
  product-specific filename into an otherwise general-purpose signal
  handler. Low severity, no suggested home. Fits the directory (libstuff is
  Bedrock's own utility layer) but arguably not a "generic reusable utility"
  cluster if one existed — noted for completeness, not escalated further.
- **SLog: naming inconsistency** — `_g_SLogMask` (S-prefixed global marker)
  vs. `GLOBAL_IS_LIVE` (spelled-out global) are two conventions for the same
  concept in one file. Internal, low severity, resolvable within the unit
  itself; not a cluster- or directory-level concern.

<!-- ROLLUP
theme: process-wide diagnostic state (logging + crash/signal stack-trace capture) exposed as global atomics/free functions rather than instances
exports: [SLOG-macro log-level mask (_g_SLogMask), SLogStackTrace, structured-log param whitelisting (SWhitelistLogParams/SIsLogParamWhitelisted/addLogParams), POSIX crash-signal handling with backtrace-to-disk (SInitializeSignals/SSetSignalHandlerDieFunc), pending-signal bitmask query/clear (SGetSignal/SCheckSignal/SGetSignals/SClearSignals), SSIGNAL_NOTIFY_INTERRUPT poll-loop wakeup hook]
depends_on_dirs: [libstuff, sqlitecluster]
depended_on_by: []
misfit_count: {high: 0, med: 1, low: 3}
resolved_locally: 2
escalate:
  - item: "_SSignal_StackTrace's direct call to SQLiteNode::KILLABLE_SQLITE_NODE->kill()"
    from: libstuff/SSignal.cpp
    why: a generic libstuff crash handler reaching directly into the sqlitecluster layer; a layering decision bigger than this cluster
    suggested_home: null
  - item: "SLog.cpp and SSignal.cpp have no dedicated headers (SLog.h / SSignal.h)"
    from: libstuff/SLog.cpp, libstuff/SSignal.cpp
    why: both declare their public API in libstuff.h's catch-all instead of their own header, unlike other separated libstuff units; a directory-wide header-convention question
    suggested_home: "libstuff/SLog.h, libstuff/SSignal.h"
-->
