# Scaling design

Bedrock (152 units) is the test fixture. The target is repos 100-500x larger.
Every technique below is chosen because its cost per unit is bounded and its
context per agent is bounded. Anything requiring "load it all into one context"
is disqualified regardless of how well it works here.

## What breaks naively, and the fix

| Phase | Naive approach | Breaks at scale because | Fix |
|---|---|---|---|
| 2. Annotate | one agent per N units | nothing — embarrassingly parallel | keep; tier the model down |
| 3. Roll up | give each dir agent the full subtree | context grows with subtree size | strict fan-in: immediate children only |
| 4. Adjudicate | load whole index, reason globally | context grows with repo size | per-misfit, bounded candidate set |
| 5. Report | one agent ranks all findings | same | computable score + clustered reduce |
| all | re-run from scratch | re-pays full cost every time | content-hash cache |

## 1. Bounded fan-in (Phase 3)

Each directory agent sees **only its immediate children**: the unit records of
files directly in it, plus the `SUMMARY.md` of each direct subdirectory. Never
the transitive subtree. Fan-in is bounded by directory width, not repo size.

This is lossy — it is the "rollup of rollups" problem, and it is the price of
scaling. It is mitigated, not solved, by two things:

**a. A structured upward interface.** Every `SUMMARY.md` ends with a fixed-size
machine-readable block carrying exactly what a parent needs. The parent reads
this block rather than re-deriving from prose, so signal survives each hop at
constant cost:

```yaml
<!-- ROLLUP
theme: one line - what this directory is for
exports: [the 3-8 concepts this dir offers outward]
depends_on_dirs: [dirs this subtree includes from]
depended_on_by: [populated in the top-down pass]
misfit_count: {high: 0, med: 3, low: 7}
escalate:
  - items whose resolution needs a decision above this directory
-->
```

`escalate` is the important field. A misfit that can be resolved *within* a
directory is resolved there and never reaches the root. Only genuinely
cross-cutting items propagate upward, so the volume reaching the top grows far
slower than the repo does.

**b. Two passes, at every level.** Bottom-up builds the content; top-down
revises each summary with its role relative to siblings and parent. The
top-down pass carries only the parent's rollup block downward — again constant
size per hop, not proportional to the tree.

## 2. Retrieval without a vector DB (Phase 4)

I originally called the vector DB overkill *because the repo was small*. That
reason does not survive the scale change, so here is one that does.

Adjudication asks: "this symbol looks stranded — where should it live?" That
needs a **candidate set with high recall**, not a precise ranking. The precision
comes from the LLM that adjudicates. So retrieval only has to get the right
directory into a list of ~10, and two free signals do that well:

1. **Include graph** (from each unit's `depends_on`). If a symbol's dependencies
   cluster in directory X, X is a candidate. This is exact, not probabilistic,
   and it is the single strongest signal for C++.
2. **BM25** over directory rollup blocks. Cheap, no model, no download.

Embeddings would add recall only on vocabulary mismatch, and would cost a
multi-GB model download plus an embedding pass over every summary — which is
itself an O(repo) serial bottleneck on CPU. Bad trade.

The retriever is pluggable (`candidates(misfit) -> [dir, ...]`). If a repo turns
out to need semantic recall, swap in embeddings behind that interface without
touching anything else.

Adjudication is then **one bounded call per misfit**: the misfit, its unit's
summary, and ~10 candidate directory rollups. Constant context. The number of
calls grows with findings, not with repo size — and `escalate` filtering keeps
that sublinear.

## 3. Computable priority (Phase 5)

Ranking by asking an agent "which of these 400 findings matter most" does not
scale and is not reproducible. Score arithmetically instead:

```
priority = (severity × confidence × blast_radius) / effort

  severity      1 | 3 | 9        from the misfit record
  confidence    0.0-1.0          Phase 4 adjudication
  blast_radius  reverse include-graph fan-in of the affected unit
  effort        log2(lines to move)
```

`blast_radius` comes free from the include graph already collected: a stranded
helper that 60 files depend on outranks one nobody imports. Agents then write
prose for the top N, rather than ranking everything.

## 3b. Vendored detection is a UNIT property, not a file property

Twice on Bedrock, third-party code was half-excluded because the marker sits in
only one file of a pair:

- `libstuff/qrf.{c,h}` — both carry the SQLite public-domain blessing, but the
  hand-written exclusion list named only the `.c`. An agent annotated the
  header before the content sniff caught it.
- `test/lib/tpunit++.{hpp,cpp}` — the `.hpp` carries an external MIT header and
  was excluded; the `.cpp` carries no header at all and was annotated. The unit
  ended up split across the boundary.

The fix is to propagate: **if any file in a unit is vendored, the whole unit
is.** Detect per file, then decide per unit. On a large repo this matters much
more than here — a vendored package typically carries its licence in one or two
files out of hundreds, so per-file detection silently admits most of it.

The `tpunit++` case also shows the judgement is not always binary. That `.cpp`
has local modifications (`exitFlag`, `_verboseOutput`, thread-local test-name
tracking), so it is a modified fork rather than pristine third-party code.
Where a unit straddles the line, prefer excluding it and say so in the report,
rather than generating refactoring advice for code the team does not own. Here
it produced zero findings either way, so nothing turned on it.

## 3c. The analysis contaminates its own searches

This pass writes two kinds of artifact into the repo it is analysing: a
`/* SUMMARY */` block in every source file, and machine-readable findings under
`.arch/`. Both name symbols in prose. Any later grep therefore finds our own
output and counts it as evidence.

Measured during verification, per claim, phantom hits by source:

| Claim | Real code hits | In SUMMARY blocks | In `.arch/` |
|---|---|---|---|
| `SVERSION` dead | 2 | 5 | 24 |
| simdjson consumer | 1 | 3 | 40 |
| `commandPortSuppressionReasons` | 1 | 1 | 6 |

`.arch/` is the larger contaminant, by a wide margin — worse than the in-file
annotations everyone thinks to exclude. This already caused one real error: a
dead-test scan missed `LibStuffTest::testUpperLower` because the SUMMARY block
mentioning it looked like a call site.

Rules that follow:

1. Any tool or agent grepping the repo after Phase 2 must exclude BOTH the
   `/* SUMMARY */` line ranges and the `.arch/` tree.
2. Verification should classify every hit as real-code / in-annotation /
   in-artifact and report the split, rather than reporting a bare count.
3. Prefer running verification against a pristine checkout (`git show
   <pre-annotation-ref>:<path>`) where practical.

At App scale this gets worse, not better: ~8,800 annotated files and a far
larger `.arch/` tree mean a naive grep is mostly self-echo.

## 4. Cache and resume

Every unit record is keyed by `sha256(file contents)` in `.arch/cache/`. A
re-run skips unchanged units entirely. This makes the second run on a large repo
near-free, and makes a large run resumable across sessions — necessary when a
repo needs more agent invocations than one session can hold.

Batch state lives in `.arch/state.json`: `pending | running | done` per batch.

## 5. Model tiering

Leaf annotation is a well-specified local task against an explicit spec — the
bulk of the calls, and the cheapest to serve. Adjudication and synthesis need
the strong model.

| Phase | Model | Calls at Bedrock scale | Calls at 50k files |
|---|---|---|---|
| 2. Annotate | sonnet | ~22 | ~7,000 |
| 3. Roll up | sonnet | ~27 | ~5,000 |
| 4. Adjudicate | opus | ~1-15 | ~hundreds |
| 5. Report | opus | ~2 | ~10 |

The tiering is what makes the large runs affordable; the cache is what makes
them repeatable.
