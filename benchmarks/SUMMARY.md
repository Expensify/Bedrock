# benchmarks/

## Theme

This directory is the repo's performance-regression harness: a tiny mixin
framework (`BenchmarkBase`) plus a set of tpunit fixtures that time individual
`libstuff` functions, and a driver binary that can either just run them or
check out a baseline git ref, rebuild, rerun, and diff throughput against it.
Its job is to let a change to a hot `libstuff` function be checked for
throughput regressions before merge, not to validate correctness (that's
`test/`'s job).

## Contents

| Child | Lines | What it is |
|---|---|---|
| `BenchmarkBase.h` | 154 | Base class mixed into tpunit fixtures: warms up, times N iterations, computes MB/s, stores/prints a `BenchmarkResult` |
| `SDeburrBench.cpp` | 90 | Benchmarks `SDeburr::deburr()` (accent/diacritic stripping) across ASCII/Latin-1/mixed-script inputs |
| `SReplaceAllBench.cpp` | 100 | Benchmarks `SReplaceAll()` across varying unsafe-character-set densities and sizes |
| `SReplaceBench.cpp` | 110 | Benchmarks `SReplace()` across match-count and find/replace length asymmetries |
| `main.cpp` | 267 | Entry point: runs the registered fixtures, and with `--baseline` also drives a git checkout/rebuild/rerun/compare workflow, printing a colored throughput table |

No subdirectories.

## Coherence

The five children hang together cleanly as one small, single-purpose harness:
a base class, a driver, and benchmark fixtures built on both. There is no
second cluster here.

Worth flagging, though: the three benchmark fixtures are a narrow sample, not
a representative one. All three exercise `libstuff` string-manipulation
functions (`SDeburr`, `SReplace`, `SReplaceAll`) specifically — nothing here
benchmarks, say, `STable`, the SQLite wrapper, networking/socket paths, or
any other hot path in `libstuff` or elsewhere in the repo that plausibly
matters more for real-world throughput. This looks like coverage that exists
because someone benchmarked the functions they were touching, not a
deliberate survey of the system's performance-critical surface. That's a
fact worth carrying upward: `benchmarks/` as a directory is well-formed, but
its *coverage* should not be read as a signal about what's actually
performance-sensitive in this codebase.

## Misfits

Only one flagged, by `main.cpp`'s own unit record: the `--baseline`
comparison branch of `main()` shells out to git (stash/checkout/rev-parse)
and reruns `make bench -j32` from inside the benchmark binary itself —
build/source-control orchestration wearing a C++ benchmark's clothes. This
doesn't fit the "measure one thing precisely" quality of the other four
children.

This is not resolvable locally: the natural home is a small wrapper shell
script that invokes the plain benchmark binary twice (once per ref) and
diffs the results, but whether such a script already exists elsewhere in the
repo (a top-level `scripts/` or similar) or where it should be added is a
call that needs a wider view than this directory alone provides. Escalated.

<!-- ROLLUP
theme: Standalone benchmark harness timing individual libstuff functions (currently only SDeburr/SReplace/SReplaceAll) with an optional git-baseline throughput comparison
exports: [BenchmarkBase, BenchmarkResult, g_benchmarkResults, runBenchmarks, printComparison]
depends_on_dirs: [libstuff, test/lib]
depended_on_by: []
misfit_count: {high: 0, med: 0, low: 1}
resolved_locally: 0
escalate:
  - item: "main() --baseline branch (git stash/checkout/rebuild orchestration)"
    from: benchmarks/main.cpp
    why: Build/source-control orchestration embedded in the benchmark binary rather than driven externally
    suggested_home: "a wrapper shell script that runs the plain benchmark binary twice and diffs results; exact location needs a repo-wide view"
-->
