# libstuff/JSON

## 1. Theme

**This directory is Bedrock's in-house JSON document model: one owned tree
type (`JSON::Value`) plus the parse/serialize/utility machinery built around
it on top of rapidjson.**

It is not a thin rapidjson wrapper offered incidentally — it is the thing
the rest of the repo (and consuming applications) reach for whenever they
need to hold, build, walk, merge, sanitize, or move JSON data. Parsing
(`Parser`/`SAXHandler`) and serialization (`Writer`) exist to get bytes into
and out of `Value`; `Utils` exists to manipulate `Value` trees once built;
`Serializable` exists to let other types declare a `Value`-shaped contract;
`Metrics` exists so an embedder can observe how much this all costs, without
this package depending back on that embedder.

## 2. Contents

| Unit | Files | Lines | Role |
|---|---|---|---|
| Metrics | Metrics.h, Metrics.cpp | 48 | Process-wide observer hook: reports parse/serialize timing and doc size to an optional callback. |
| Parser | Parser.h, Parser.cpp | 69 | Parses a JSON string into a `Value` tree; strict `read()` (throws) and permissive `readUnsafe()`. |
| SAXHandler | SAXHandler.h, SAXHandler.cpp | 269 | Implements rapidjson's SAX callback interface to build a `Value` tree incrementally as the reader streams tokens. |
| Serializable | Serializable.h | 52 | CRTP base that compile-time-enforces a `toJSON()`/`fromJSON()` contract on a derived aggregate, no vtable. |
| Utils | Utils.h, Utils.cpp | 402 | Static helpers: key replace/strip/patch, path↔object conversion, transport-safe string sanitizing, shared singleton constants. |
| Value | Value.h, Value.cpp | 2630 | Core `JSON::Value` tagged-union node, its exception hierarchy, `KeyValue`, and array/object range adapters. |
| Writer | Writer.h, Writer.cpp | 350 | Serializes a `Value` tree to a string (compact/pretty) or drives incremental construction over a rapidjson writer. |

No subdirectories.

## 3. Coherence

The seven units form one coherent package around a single shared type,
`JSON::Value` — every unit either builds it (Parser, SAXHandler), consumes
it (Writer, Utils), documents a contract to produce/consume it
(Serializable), or instruments the traffic through it (Metrics). Nothing
here is orphaned from that center.

The weakest fit is **Serializable**: its own location_fit score (4) already
flags that the CRTP trick is generic C++, not JSON-specific, and it
currently has no in-repo consumer. It is grouped here reasonably — the
contract it encodes (`toJSON`/`fromJSON`) is JSON-shaped even if the
template machinery isn't — but it is the one file you could imagine living
in a generic `libstuff` traits header instead. Not worth moving on the
evidence available (no consumer to indicate what it should sit next to);
worth a second look once something actually derives from it.

**Value.h/.cpp** is the one file whose own name_fit score (3) undersells its
contents: it carries the exception hierarchy (`Error`, `TypeError`,
`NotFound`, `InvalidArgument`), `KeyValue`, and the array/object range
adapters, none of which are named after `Value`. That's a naming-quality
observation, not a coherence problem — all of it is `Value`-adjacent
machinery and belongs in this directory regardless of what the file is
called.

## 4. Misfits

Five misfits were flagged by children, all of them design/naming smells
internal to this package rather than placement problems — none point
outward to a better home in another directory. All five are resolved here.

- **`Utils::recursiveReplaceJSONKeys`** doc comment (med) — written entirely
  in one caller's domain terms (`bankAccounts.additionalData`,
  `apiResult`, `assetReport`) and names an external test not present in
  this repo, contradicting the package's application-agnostic framing.
  **Resolved locally**: this is a documentation defect, not a location
  defect — the algorithm is generic and stays in `Utils`; the comment
  should be rewritten in JSON::Value-generic terms (merge/replace-by-key
  semantics) with the caller-specific example dropped or moved to a call
  site elsewhere.

- **`Value::mergeDeep(useSQLiteMergeBehavior)`** parameter naming (med) — a
  generic Value API names a specific downstream consumer in its signature,
  when the behavior it selects is really RFC 7386 JSON Merge Patch
  semantics. **Resolved locally**: rename the parameter/flag to describe
  the semantic difference (e.g. `nullMeansDelete` or an explicit merge-mode
  enum) rather than the consumer that currently relies on it. No code
  needs to move.

- **`JSON::logStackTraceOnEnsureTypeFailure`** (low) — a thread_local
  debug/logging toggle declared inline in the core value-type header,
  unrelated to the JSON data model itself. **Resolved locally**: this
  directory already has a purpose-built home for exactly this kind of
  cross-cutting diagnostic concern — `Metrics.h`/`Metrics.cpp`. Move the
  flag there alongside the observer hook rather than leaving it embedded in
  `Value`.

- **`Value::startTime` / `Value::logSlowConstructor`** (low) — per-instance
  constructor-timing instrumentation adds a `chrono::time_point` to every
  `Value`, including scalars that never use it, to catch occasional slow
  construction. **Resolved locally**: same reasoning as above — this is
  what `Metrics` exists for. Prefer routing slow-construction detection
  through the existing observer mechanism (or a static/thread-local
  timer scoped to construction, reported via `reportMetrics`) instead of
  a per-instance field paid by every `Value`.

- **`friend class SAXHandler`** on `Value` (low) — grants `SAXHandler`
  direct access to `Value`'s private storage for fast construction; the
  source comment itself flags this as "Coupling++". **Resolved locally**:
  both classes live in this same directory and the coupling is already
  named and deliberate in the source — nothing to escalate. Worth
  revisiting only if `Value`'s internal storage layout changes.

## ROLLUP block

<!-- ROLLUP
theme: Bedrock's in-house JSON document model (Value) plus the parse, serialize, utility, contract, and metrics machinery built around it on rapidjson.
exports: [JSON::Value, JSON::Parser (read/readUnsafe), JSON::Writer (serialize/serializePretty), JSON::Utils (tree merge/strip/sanitize helpers), JSON::Serializable<Derived> (toJSON/fromJSON contract), JSON metrics observer hook (setMetricsObserver/reportMetrics)]
depends_on_dirs: [libstuff]
depended_on_by: []
misfit_count: {high: 0, med: 2, low: 3}
resolved_locally: 5
escalate: []
-->
