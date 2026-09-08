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
Now that all siblings are visible, nothing in `libstuff`, `plugins`,
`sqlitecluster`, or `test`'s own rollups names a `scripts/`-like location
either, so this stays escalated with the same suggested home, unresolved.

## 5. Role in the system

`benchmarks/` owns performance-regression detection; `test/` owns
correctness verification. That split holds cleanly at the boundary: the only
thing `benchmarks/` takes from `test/` is `test/lib/tpunit++.hpp`, the
vendored micro test-framework runner — not `BedrockTester`, not the
process-forking harness that spins up a real server. `benchmarks/` never
launches a bedrock server or exercises the command/network stack the way
`test/tests` and `test/clustertest` do; it links functions in-process and
times them directly. So the boundary with `test/` doesn't leak: `benchmarks/`
uses `test/lib` only as a generic fixture-registration mechanism, not as a
harness.

The boundary with `libstuff` is a clean one-way read: `benchmarks/` measures
`libstuff` functions but doesn't modify, wrap, or extend them, matching
`libstuff`'s stated role as the dependency-free foundation everything else
builds on and gets measured against. `benchmarks/` has no boundary at all
with `plugins/` or `sqlitecluster` — it doesn't depend on either, and
nothing here currently measures anything owned by them (see below).

## 6. Inbound expectations

No sibling's `depends_on_dirs` lists `benchmarks`, and nothing here is
consumed elsewhere — `benchmarks/` is a leaf like `test/`, owing nothing
outward. What it needs inbound is narrow and currently met: `libstuff`'s
`SDeburr`/`SReplace`/`SReplaceAll` and `test/lib`'s `tpunit++.hpp` staying
includable and stable. Nothing in either sibling's rollup suggests that's at
risk.

Pass A already named the real finding here — three fixtures, all timing
`libstuff` string functions, is not a representative sample. With the
siblings' actual exports now visible, that can be made concrete. Two hot
paths stand out as plausibly the most performance-sensitive code in the
repo and are entirely unbenchmarked:

- **JSON/`SData` parsing.** Root's own rollup escalates `SData::deserialize`'s
  "simdjson padding logic" — someone already cared enough about parse
  throughput to reach for a SIMD JSON parser and over-allocate a buffer for
  it. `SData` (the generic HTTP-like wire message) and `JSON::Value` are on
  the path of every single command in and out of the server, per `libstuff`'s
  own export list. That a simdjson-tuned parser has zero throughput benchmark
  validating it is a concrete, evidence-backed gap, not a guess.
- **The SQLite journal write path, including its compression call.**
  `sqlitecluster`'s own rollup flags that `SQLite.cpp`/`SQLiteNode.cpp` call
  into `plugins/Compression` to (de)compress journal entries — a call that
  runs on every committed write across the cluster, which is about as hot a
  path as this system has. Nothing in `benchmarks/` times `sqlitecluster`'s
  `SQLite` transaction/journal handle, `SQLiteNode` consensus overhead, or
  that compression call specifically, despite it being flagged from two
  independent directions as structurally significant.

Beyond those two, `libstuff`'s own export list names a `STCPManager`/
`SHTTPSManager`/`SSSLState` poll()-loop socket/TLS stack — the network layer
every request and response crosses — and `sqlitecluster` exports
`SQLiteClusterMessenger`/`SQLitePool` (the pooling/wire-format plumbing
between cluster nodes), none of which have any throughput coverage here
either. `plugins/`'s five `BedrockPlugin_*` implementations and their
per-plugin SQLite schema access (`Cache`'s LRU, `Jobs`'s queue queries) are
likewise entirely absent — every plugin dispatch (`peek()`/`process()`) runs
on every request `test/tests` shows this server handles, and none of it is
timed. Given that, "SDeburr/SReplace/SReplaceAll" reads less like a chosen
baseline and more like whatever the last person who touched string-escaping
code happened to benchmark — real coverage would need at minimum one
fixture on the SData/JSON parse path and one on the SQLite commit/journal
path before this directory's results say anything about where the system's
actual throughput risk lives.

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
