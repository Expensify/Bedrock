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

## 5. Role in the system

**What this directory owns that `libstuff` proper does not.** `libstuff`'s own
summary describes itself as a foundation layer undercut by a 4612-line
catch-all (`libstuff.cpp`/`.h`) that duplicates functionality dedicated
sibling units already own. `libstuff/JSON` is the type-safe, tree-shaped JSON
model: a real `JSON::Value` node you can hold, walk, and mutate in memory,
built by a real parser (`Parser`/`SAXHandler`) and serialized by a real
writer (`Writer`), with `Utils` operating on that tree and `Serializable`
documenting a contract against it. Nothing in the parent's catch-all offers
that — the catch-all's JSON support (`SToJSON`, `SComposeJSONObject`,
`SComposeJSONArray`, `SParseJSONObject`, `SParseJSONArray`, and their private
`_SParseJSONValue`/`_SParseJSONObject`/`_SParseJSONArray`/`_SParseJSONString`
helpers) is a second, independent JSON implementation: string-in/string-out,
built around `STable` (a flat, single-level string-to-string map) rather
than a real tree, with no typed scalars, no nested-array-of-objects support
beyond what a `list<string>` of pre-serialized fragments can fake, and no
shared exception hierarchy.

**The boundary does leak, and it leaks toward the parent, not from it.**
Confirmed by grep across the repo: `libstuff`'s catch-all `_SParseJSONString`
(named in this task's brief) is real, and it is not a lone helper — it's one
private function in a five-function parse engine
(`_SParseJSONValue`/`_SParseJSONObject`/`_SParseJSONArray`/`_SParseJSONString`
feeding the public `SParseJSONObject`/`SParseJSONArray`) that duplicates
exactly what `Parser`/`SAXHandler` in this directory already do, plus a
parallel compose side (`SToJSON`, `SComposeJSONObject`, `SComposeJSONArray`)
duplicating `Writer`. Worse: this is not dead legacy code sitting next to the
new package unused — it is the JSON implementation actually driving Bedrock's
own production JSON traffic today. `BedrockServer.cpp`, `BedrockCore.cpp`,
`BedrockCommand.cpp`, `plugins/Jobs.cpp`, and `sqlitecluster/SQLiteNode.cpp`
all call `SParseJSONObject`/`SComposeJSONObject`/`SParseJSONArray` from the
catch-all; not one of them includes anything under `libstuff/JSON/` or
references `JSON::Value`. Meanwhile `JSON::Value`, `JSON::Parser`, and
`JSON::Writer` are referenced *only* by this package's own tests
(`test/tests/JSON*Test.cpp`) — grep finds zero production call sites for
`JSON::Value` anywhere else in the tree. So today there are two live JSON
engines in this codebase, and the older, less capable one — living entirely
in the parent's catch-all — is the one Bedrock itself actually runs on.

This is not simply an oversight to fix by relocating code, though: `JSON/README.md`
describes a deliberate staged-migration design. This package is built as a
separate archive, `libjson.a`, whose strong symbols are excluded from
Bedrock's own dynamic symbol table (`-Wl,--exclude-libs,libjson.a`) precisely
so that an *embedding application* which already links its own private JSON
implementation can adopt Bedrock without a symbol collision, then later link
`libjson.a` directly and retire its private copy. Only two symbols are meant
to stay exported from Bedrock itself: `JSON::setMetricsObserver` and
`JSON::reportMetrics` (enforced by the `checkjsonsymbols` build target) —
which is also why `Metrics.cpp` is described as linked into `libstuff.a`
directly rather than folded invisibly into `libjson.a`. So the package's
primary intended consumer is outward, past this repo's boundary, not
`BedrockServer`/`BedrockCore`/`plugins`. That reframes the duplication: it
isn't obviously a bug in this package, but it does mean the parent's
catch-all is not merely dead weight worth deleting in place — decommissioning
it requires migrating live internal call sites (`BedrockServer.cpp`,
`BedrockCore.cpp`, `BedrockCommand.cpp`, `plugins/Jobs.cpp`,
`sqlitecluster/SQLiteNode.cpp`) onto `JSON::Value`, which is real,
cross-cutting migration work, not a location fix this directory can resolve
alone. See the escalate entry below.

**Is `libstuff/JSON` the model for the rest of `libstuff`?** The reading
mostly holds, with one caveat. As a *shape*, yes: one coherent package built
around a single owned type (`JSON::Value`), its own directory, a README that
states its boundary and build contract explicitly (most other `libstuff`
units don't have one) — exactly the decomposition target the parent's own
summary gestures at when it flags the catch-all for breakup. The caveat is
adoption, not shape: a model subdirectory that no internal code actually
uses is a model for *structure*, not yet for *behavior*. If `libstuff` is
decomposed along lines like this directory, the JSON slice of that
decomposition is already done structurally — what remains is the migration
described above, and until that happens this directory is proof-of-concept
for the target shape rather than evidence the target shape is fully load-bearing.

## 6. Inbound expectations

Per the parent's rollup, `libstuff` exports `JSON::Value` "via JSON/" as one
of its named exports — but the parent rollup and the (empty) siblings list
handed to this pass name no sibling directory that actually consumes it, and
the repo-wide check above confirms why: nothing in `sqlitecluster`,
`plugins`, `BedrockServer.cpp`, `BedrockCore.cpp`, `BedrockCommand.cpp`, or
any other in-repo production code includes `libstuff/JSON/*.h` or references
`JSON::Value`/`JSON::Parser`/`JSON::Writer`/`JSON::Utils`. The only in-repo
consumers are this package's own tests (`test/tests/JSONParserTest.cpp`,
`JSONTest.cpp`, `JSONUtilsTest.cpp`, `JSONValueTest.cpp`).

The real inbound dependent, per `README.md`, is external: applications that
link `libjson.a` directly, plus the process-wide metrics hook consumed by
whatever embeds Bedrock. What must stay stable outward, in order of how
publicly it is committed:

- **`JSON::setMetricsObserver` / `JSON::reportMetrics`** — the only two
  symbols the build (`checkjsonsymbols`) actually enforces stay exported from
  the Bedrock binary itself. This is the one surface with a machine-checked
  stability guarantee; changing its signature breaks the build, not just a
  caller.
- **`JSON::Value`'s full public surface** (construction, accessors, the
  exception hierarchy, `ArrayValue`/`ObjectValue` iteration adapters) and
  **`JSON::Parser`/`JSON::Writer`'s entry points** — owed to applications
  that link `libjson.a` per the staged-deployment path the README describes.
  There is no in-repo caller to regression-test this against, which is
  exactly the risk a purely internal view can't see: a change here that
  breaks no test in this repo could still break an external embedder that
  has already linked `libjson.a`. This is the inadequate-visibility case the
  spec anticipates — Pass B here cannot confirm what an external embedder
  actually calls, only what the README promises it can call.
- **The RapidJSON header dependency** (`externalLib/rapidjson/include`) is
  itself an outward contract per the README: consumers linking the archive
  must add it to their include path. That's a build-integration expectation
  this directory owes, not just a header include.

Nothing currently relies on this package that this package fails to provide;
the gap runs the other way — the package provides a stable typed-JSON surface
that the codebase which hosts it does not yet use.

## 7. Misfits revisited

The four Pass A misfits resolved locally (`recursiveReplaceJSONKeys` doc
comment, `mergeDeep`'s `useSQLiteMergeBehavior` naming,
`logStackTraceOnEnsureTypeFailure`, `startTime`/`logSlowConstructor`,
`friend class SAXHandler`) still hold as resolved-locally under the wider
view — none of them turn out to be sibling-boundary questions, and nothing
about the parent's catch-all changes their internal disposition.

One new escalate item follows directly from the parent context this pass
adds: the catch-all's JSON engine. It was not visible as a misfit in Pass A
because Pass A cannot see the parent's files; it is visible now only because
the parent's rollup named the catch-all and this pass could check where its
JSON support actually lives. It is marked `escalate`, not
`resolved-locally`, because resolving it means migrating call sites in
`BedrockServer.cpp`, `BedrockCore.cpp`, `BedrockCommand.cpp`,
`plugins/Jobs.cpp`, and `sqlitecluster/SQLiteNode.cpp` — files this
directory cannot see or move, and a decision `libstuff`'s own decomposition
pass is better placed to sequence.

## ROLLUP block

<!-- ROLLUP
theme: Bedrock's in-house JSON document model (Value) plus the parse, serialize, utility, contract, and metrics machinery built around it on rapidjson.
exports: [JSON::Value, JSON::Parser (read/readUnsafe), JSON::Writer (serialize/serializePretty), JSON::Utils (tree merge/strip/sanitize helpers), JSON::Serializable<Derived> (toJSON/fromJSON contract), JSON metrics observer hook (setMetricsObserver/reportMetrics)]
depends_on_dirs: [libstuff]
depended_on_by: [test/tests (JSONParserTest/JSONTest/JSONUtilsTest/JSONValueTest - the only in-repo consumers found), external applications linking libjson.a (per JSON/README.md's staged-deployment build boundary; not visible from inside this repo) - no other in-repo directory (libstuff's own catch-all, sqlitecluster, plugins, BedrockServer/BedrockCore/BedrockCommand) references JSON::Value/Parser/Writer/Utils]
misfit_count: {high: 0, med: 2, low: 3}
resolved_locally: 5
escalate:
  - item: "libstuff's catch-all JSON engine (SToJSON, SComposeJSONObject/Array, SParseJSONObject/Array, and private _SParseJSONValue/_SParseJSONObject/_SParseJSONArray/_SParseJSONString) duplicates this package's Parser/Writer and is the engine Bedrock's own production code (BedrockServer.cpp, BedrockCore.cpp, BedrockCommand.cpp, plugins/Jobs.cpp, sqlitecluster/SQLiteNode.cpp) actually calls; JSON::Value has no in-repo production callers"
    from: libstuff/libstuff.cpp,libstuff.h (parent's catch-all, not this directory)
    why: retiring the duplicate means migrating call sites this directory cannot see or move; sequencing that migration is a decision for whoever decomposes libstuff, not resolvable from libstuff/JSON alone
    suggested_home: libstuff (as part of its own catch-all decomposition) - target is routing those call sites through JSON::Parser/JSON::Writer and retiring SParseJSON*/SComposeJSON*/SToJSON
-->
