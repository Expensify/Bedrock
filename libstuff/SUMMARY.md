# libstuff

## 1. Theme

**libstuff is Bedrock's dependency-free foundation layer: the string/table/
exception/logging vocabulary every other directory is built on, plus five
largely-independent utility stacks (networking, SQL-adjacent typed values,
async/threading, JSON, and process diagnostics) that all sit on top of it.**

Its job is to be the thing every other directory in the repo can depend on
without depending back — a non-blocking `poll()`-loop socket/TLS/HTTPS stack
for talking to the network, typed SQL result/value/parameter types and a
generic HTTP-like wire format for talking to SQLite and to peers, safe
background-thread primitives for work that must not block the poll loop, an
in-house JSON document model (`JSON/`), and process-wide logging/crash
handling. In practice one 4612-line file (`libstuff.h/.cpp`) never finished
being cut along those lines: it still carries a full second SQL execution
engine, a full second HTTP grammar, crypto wrappers, raw socket primitives,
gzip, and an alternate logging transport — mostly *duplicating* dedicated
units that already exist a few files away in this same directory.

## 2. Contents

26 units (pre-clustered into 5 groups) plus one subdirectory.

| Child | Kind | Units | Role |
|---|---|---|---|
| `raii-auto-clock` (cluster) | 3 units | AutoScopeOnPrepare, AutoTimer/AutoTimerTime, SPerformanceTimer | RAII scope-guards and start/stop timing instrumentation |
| `sqlite-header-wire` (cluster) | 8 units | SData, SDeburr, SQResult, SQResultFormatter, SQValue, SQliteParameter, STime, `libstuff` itself | Typed SQL values/result sets + HTTP-like wire format, and the catch-all file's buried duplicates of both |
| `socket-shared-providing` (cluster) | 9 units | STCPManager(+Socket/Port), SSSLState, SHTTPSProxySocket, SStandaloneHTTPSManager, SHTTPSManager, SSocketPool, SMultiHostSocketPool, SFastBuffer, SSynchronizedQueue, SRandom | Non-blocking poll()-loop socket/TLS/HTTPS networking stack |
| `thread-back-logger` (cluster) | 4 units | SThread, SRingBuffer, SFluentdLogger, SResolver | Background-thread primitives + their two consumers (Fluentd shipping, async DNS) |
| `trace-wide-stack` (cluster) | 2 units | SLog, SSignal | Process-wide log-mask + crash/signal stack-trace capture |
| `JSON/` (subdirectory) | 7 units | Value, Parser, SAXHandler, Writer, Utils, Serializable, Metrics | Bedrock's in-house JSON document model on rapidjson — self-contained, fully resolved, nothing propagates up |

## 3. Coherence

The five clusters genuinely belong in one directory in the sense that matters
for this repo: all of it is dependency-light-or-should-be, all of it is
consumed by nearly everything above libstuff, and none of it is
application-specific *by design* (where it currently is, is the problem — see
Misfits). But they are five separate concerns wearing one directory, held
together mainly by convention and history rather than by shared code:
networking (`socket-shared-providing`) shares nothing with the SQL/wire types
(`sqlite-header-wire`) except that `SData` carries `SQliteParameter`s;
diagnostics (`trace-wide-stack`) and async/threading (`thread-back-logger`)
are each self-contained; the RAII/timing cluster is smaller still and one of
its three members (`AutoScopeOnPrepare`) does not belong here at all (see
below). `JSON/` is the one child that is cleanly self-contained — a real
subpackage, not a cluster of loose files, and already fully resolved.

The directory's biggest coherence problem isn't any one cluster, it's
`libstuff.h/.cpp` itself, which the clustering algorithm correctly could not
assign to one theme because it isn't one thing: it is simultaneously "the
directory's shared core" (SString/STable/SException/logging+assert macros —
genuinely belongs everywhere) and "an unsorted bin" (a second SQL engine, a
second HTTP grammar, crypto, raw sockets, gzip, an alternate syslog
transport — each of which duplicates a dedicated unit that already exists
elsewhere in this same directory). That second part is not a clustering
artifact to shrug off; it's the main finding of this rollup.

## 4. Misfits

Consolidating all five clusters' escalations. The clusters could each see
their own members but not each other — from here, several "suggested home:
not in this cluster, unconfirmed" targets are directly visible and can be
closed out now.

**Pass B update:** with the parent's rollup in hand — the stated layer order
`libstuff -> sqlitecluster -> plugins -> root` and its fix rule ("sink the
shared primitive down to the lowest layer both sides can use, rather than
reaching upward") — two of the five items originally escalated turn out to be
decidable without anyone's wider view after all, and move down into Resolved
locally below. The other three (`AutoScopeOnPrepare`, `SData`'s simdjson
padding, `SSignal`'s `kill()` call) are exactly the escalations root's own
rollup is silent on for two of them and explicit on for the third — see each
entry and §5 for what that silence does and doesn't tell us.

**Resolved locally (10) — target confirmed to exist in this directory:**

- **SParseHTTP/SComposeHTTP/SParseURI/SParseHost family** (`libstuff.cpp`) —
  sqlite-header-wire flagged this as a full HTTP wire-grammar duplicating
  `SData`'s purpose, with suggested home "SHTTPSManager ... unconfirmed."
  `SHTTPSManager` is confirmed to exist in `socket-shared-providing`, in this
  same directory. **Resolved: move into `SHTTPSManager.h/.cpp`** (or split
  into a new `SHTTPMessage` unit alongside it if `SHTTPSManager` itself
  shouldn't own parsing) — either way the destination is within libstuff.
- **Crypto wrappers** (`SAESEncrypt/SAESDecrypt/SHashSHA1/SHashSHA256/
  SEncodeBase64/SDecodeBase64/SHMACSHA1/SHMACSHA256`, `libstuff.cpp`) —
  self-contained, no dependency on the rest of the file. No `SCrypto` unit
  exists anywhere visible across all five clusters. **Resolved: this is a
  "carve out a new unit" question, not a "which directory" question** —
  extract to a new `libstuff/SCrypto.h/.cpp`.
- **Direct-syslog-socket pool** (`SLogSocketFD/SSyslogSocketDirect/
  SSyslogNoop`, `libstuff.cpp`) — sqlite-header-wire suggested `SLog.cpp`,
  unconfirmed from within that cluster. `SLog.cpp` is confirmed to exist in
  `trace-wide-stack`, in this same directory. **Resolved: merge into
  `SLog.cpp`** as its alternate transport.
- **Raw socket/poll primitives** (`S_socket/S_close/S_accept/S_recvfrom/
  S_recvappend/S_sendconsume/S_poll/SFDset/SFDAnySet`, `libstuff.cpp`) —
  sqlite-header-wire named `STCPManager` as the only real caller, unconfirmed
  from that cluster. `STCPManager` is confirmed to exist in
  `socket-shared-providing`. **Resolved: move into `STCPManager.h/.cpp`**,
  the file that already owns the poll-loop socket model these serve.
- **SGZip/SGUnzip** (`libstuff.cpp`) — self-contained, no dependency on
  surrounding code, no suggested home identified by the cluster. **Resolved:
  no cross-directory question here** — extract to its own
  `libstuff/SGZip.h/.cpp` unit; it's generic-enough utility code that fits
  the directory, it just needs to stop living inside the catch-all.
- **`SHTTPSManager.cpp`'s unused includes of `BedrockServer.h` and
  `sqlitecluster/SQLiteNode.h`** — socket-shared-providing escalated this as
  "touches files outside this cluster's scope to confirm," but the unit data
  it's carrying already states neither header's symbols are referenced in the
  file. **Resolved: this is a mechanical deletion**, not a layering decision —
  drop both `#include`s from `SHTTPSManager.cpp`.
- **SRandom clustering** — flagged by socket-shared-providing as fitting the
  *directory* but not that cluster's socket/poll theme. Confirmed: nothing
  about `SRandom` (a static `mt19937_64` wrapper) belongs with networking, and
  nothing elsewhere in libstuff claims it either. **Resolved: leave the unit
  where it is (`libstuff/SRandom.h/.cpp`); it's a directory-organization/
  cluster-labeling artifact, not a code misfit** — worth regrouping under a
  general-utilities label next time clustering runs, not a code change.
- **`SLog.cpp` / `SSignal.cpp` have no dedicated headers** — trace-wide-stack
  flagged this as "a directory-wide header-convention question" beyond its
  own scope. It is squarely a libstuff-level question and needs no outside
  view: both files currently publish their API through `libstuff.h`'s
  catch-all instead of their own header, unlike every other separated unit in
  this directory (and `STime` has the same problem, per sqlite-header-wire's
  notes, already resolved within that cluster). **Resolved: give both
  `SLog.h` and `SSignal.h` their own headers**, moving their declarations out
  of `libstuff.h`.
- **[Pass B] AutoTimer name collision** (`libstuff/AutoTimer.h/.cpp` vs. an
  unrelated `AutoTimer` in `BedrockCore.h`) — escalated in Pass A for lack of
  visibility into `BedrockCore.h`. Root's own rollup, which *can* see both
  sides, does not list this collision among its own escalations — meaning
  root either already resolved it on its end or found the two don't actually
  collide (different namespace/TU). Either way, nothing about the fix
  requires libstuff to wait: libstuff's copy can be renamed unilaterally (it's
  a small, locally-used RAII timer, not part of any documented external
  contract) without needing BedrockCore.h to change in lockstep. **Resolved:
  rename `libstuff`'s `AutoTimer` to something more specific (e.g.
  `SScopeTimer`)** so the ambiguity is gone from this side regardless of what
  root does with its own copy.
- **[Pass B] `SHTTPSManager` (class) depends on `BedrockPlugin.h`** —
  escalated in Pass A as inverting libstuff's dependency direction on the
  application layer. Root's rollup now names the general fix for exactly this
  shape of problem (sqlitecluster/SQLite.cpp reaching up into
  plugins/Compression.h is the case that produced the rule): *sink the shared
  primitive down to the lowest layer both sides can use, rather than reaching
  upward*. Applied here, the "shared primitive" isn't `BedrockPlugin` itself —
  it's whatever narrow contract `SHTTPSManager` actually needs from it (almost
  certainly a callback/registration surface for routing a completed HTTPS
  response back into command processing). `SStandaloneHTTPSManager`, already
  living in the same file with no such dependency, is proof the networking
  logic itself doesn't need it. **Resolved: replace the concrete
  `BedrockPlugin.h` include with a generic virtual hook or `std::function`
  callback interface defined in `SHTTPSManager.h` itself** — a libstuff-side
  change with no coordination needed to decide *that* it should happen (root
  will need to update its own derived classes to implement the new interface,
  but that is root's normal cost of consuming libstuff, not a reason to keep
  this escalated). This is the one item Pass A had marked escalate purely for
  lack of a naming for the general principle — now that root has named it,
  the decision itself no longer needs a wider view.

**Escalate (3) — still cross the libstuff/sqlitecluster boundary and genuinely
need a view outside libstuff:**

- **AutoScopeOnPrepare** (`libstuff/AutoScopeOnPrepare.h/.cpp`) — `#include`s
  `sqlitecluster/SQLite.h` directly and exists only to scope a SQLite-specific
  callback. **[Pass B]** `sqlitecluster` is now visible as a sibling and its
  own rollup lists no naming conflict or objection, so the destination is
  confirmed, not merely guessed — but unlike `SHTTPSManager`, there is no
  generic half to sink down: `AutoScopeOnPrepare` is 100% SQLite-specific, so
  the fix is a straight relocation *out of* libstuff, not a local interface
  change. Since libstuff is already the floor of the stack (`libstuff ->
  sqlitecluster -> plugins -> root`), reaching up can only be fixed by moving
  the file up to the layer that needs it — an action outside this directory,
  so this stays an escalate entry even though the decision itself is now
  final. **Confirmed destination: move `AutoScopeOnPrepare.h/.cpp` into
  `sqlitecluster`.**
- **`SData::deserialize` simdjson padding logic** (`libstuff/SData.cpp`) — a
  generic message container over-allocating 32 bytes to satisfy one
  downstream parser's requirement. **[Pass B]** Root's own Pass A rollup
  escalates this identical item, verbatim in spirit, after seeing the *entire*
  subtree (libstuff, sqlitecluster, plugins, test, benchmarks) — and still
  finds no simdjson consumer of `SData` anywhere. That is stronger evidence
  than this pass could produce alone: it means either the real consumer lives
  outside this repository entirely (a separate service parsing shipped `SData`
  payloads with simdjson) or the padding is speculative and should be removed.
  Neither libstuff nor root can settle which from what's visible in this
  subtree — this one may simply not be resolvable from inside the repo at all.
- **`_SSignal_StackTrace` calls `SQLiteNode::KILLABLE_SQLITE_NODE->kill()`**
  (`libstuff/SSignal.cpp`) — a generic crash handler reaching directly into
  `sqlitecluster` to kill peer connections on crash. **[Pass B]** Unlike
  `AutoScopeOnPrepare`, this one *does* have a generic half that could sink
  down per root's rule: `SSignal` already owns "process-wide crash/signal
  handling" as its whole remit, so a generic callback-registration hook
  (`SSignal::setCrashKillFunction(std::function<void()>)` or similar) fits
  naturally inside libstuff, with `sqlitecluster` registering
  `SQLiteNode::kill()` into it at startup instead of `SSignal.cpp` including
  `SQLiteNode.h` and calling it directly. That would remove the upward
  `#include` entirely. It stays escalate rather than resolved-locally because
  confirming it needs to know how `SQLiteNode`'s lifecycle/shutdown actually
  works today — sqlitecluster's one-line rollup entry doesn't carry that, and
  Pass B's bounded fan-in means this pass cannot go read `SQLiteNode.cpp` to
  check. **Suggested split: generic hook API in `libstuff/SSignal`, concrete
  registration call in `sqlitecluster`** — same shape of fix as
  `SHTTPSManager`, one layer further out.

**Note on `JSON/`:** its own rollup reports `misfit_count: {high:0, med:2,
low:3}`, `resolved_locally: 5`, `escalate: []` — every misfit it found was
closed within `JSON/` itself. Nothing from it enters this directory's ledger.

## 5. Role in the system

Root has now named the intended stack order: **libstuff -> sqlitecluster ->
plugins -> root**. libstuff is the base — the parent's own numbers put its
blast radius at 68 of 150 units, the largest of any directory, which is what
"foundation" means in practice: nearly everything else in the repo either
sits directly on libstuff or sits on something that does.

**What libstuff owns that no sibling does.** None of the four siblings
duplicate libstuff's actual remit — they consume it, they don't reimplement
it:

- `sqlitecluster` owns the *replicated SQLite engine itself* (the transaction/
  journal handle, leader/follower consensus, peer wire protocol) — a specific
  application of libstuff's generic wire format and socket stack, not a
  competing version of it.
- `plugins` owns command-verb implementations (`peek()`/`process()` and their
  SQLite schemas) — pure consumers of both libstuff and sqlitecluster.
- `test` and `benchmarks` own harnesses and coverage, not primitives — though
  see §6, both have already produced generic utilities that arguably belong
  here instead.

So libstuff's ownership is real and undisputed: the string/table/exception/
logging core, the poll()-loop networking stack, SQL-adjacent typed
values/wire format, background-thread primitives, the JSON document model,
and process-wide diagnostics. Nothing upstream claims any of this.

**The boundary with `sqlitecluster` specifically.** In the intended stack
this boundary should be one-directional — sqlitecluster depends on libstuff,
never the reverse — and mostly it is. But three places currently leak across
it, and seeing sqlitecluster's own rollup for the first time changes what can
be said about each:

- **`AutoScopeOnPrepare`** and **`_SSignal_StackTrace`'s `kill()` call** are
  both libstuff-side code reaching *up* into sqlitecluster headers/types
  (`SQLite.h`, `SQLiteNode`). Both are confirmed real (sqlitecluster's rollup
  raises no objection to either landing near it), and both now have a fix
  shape thanks to root's rule — but they differ in kind: `AutoScopeOnPrepare`
  is pure relocation (no generic half to keep), while `SSignal`'s case
  is a sink-down-the-primitive candidate, structurally the same shape as
  `SHTTPSManager`'s but one layer further out, where this pass still can't
  see enough of `SQLiteNode`'s lifecycle to finish the design. Both remain
  escalated (§4).
- **`SData`'s simdjson padding** is a different kind of leak: it isn't a
  reach into sqlitecluster's *types*, it's a shape decision made for a
  consumer that may not exist in *either* directory — root's independent,
  whole-subtree confirmation of the same dead end (§4) suggests the boundary
  question here might be moot rather than unresolved.
- **`SHTTPSManager` depending on `BedrockPlugin.h`** is not a sqlitecluster
  boundary issue at all — it reaches two layers up, past sqlitecluster,
  straight to root. It no longer needs to stay escalated (§4): root's rule
  gives a fix libstuff can execute unilaterally (define the generic hook
  here), which is exactly the resolution the parent's naming of the pattern
  was for.

Net: the libstuff/sqlitecluster boundary is real and mostly holds. Where it
leaks, the leaks are all in one direction (libstuff reaching up), never the
reverse — consistent with libstuff genuinely being the floor.

## 6. Inbound expectations

**Dependency is universal, not selective.** Every sibling this pass can now
see depends on libstuff directly: `benchmarks` (times libstuff functions
directly), `plugins`, `sqlitecluster`, and `test` all list `libstuff` in
their own `depends_on_dirs`, and root does too. There is no directory in this
subtree that doesn't build on libstuff somewhere. That universality is what
makes the catch-all-file problem (§1, §3) more than cosmetic: every one of
those consumers is, at minimum, exposed to `libstuff.h`'s inflated surface
even if they only want `SString`.

**The stable surface siblings actually rely on** is the union of the five
clusters' real exports, not the catch-all's contents: `SString`/`STable`/
`SException`/logging+assert macros, `SData` (the wire format sqlitecluster's
command/peer protocol and plugins' commands are built on), `SQResult`/
`SQValue`/`SQliteParameter`, the `STCPManager`/`SHTTPSManager`/`SSSLState`
networking stack, `SLog`/`SSignal`, `SThread`/`SRingBuffer`/`SFluentdLogger`/
`SResolver`, and `JSON::Value`. Nothing a sibling's rollup names as a
dependency is missing from this list — no sibling calls out an expectation
libstuff fails to meet.

**What's exposed by accident.** Two distinct things, both real:

1. The catch-all file's buried duplicates (SQL execution, HTTP grammar,
   crypto, raw sockets, gzip, an alternate syslog transport — §1, §4) are
   all currently reachable through `libstuff.h`, meaning every one of those
   universal dependents transitively gets a much wider surface than the five
   clusters actually intend to export. Nothing in any sibling's rollup uses
   any of these duplicates today (none list them as an export they consume),
   which is itself evidence they're accidental, not load-bearing.
2. Until the `SHTTPSManager`/`BedrockPlugin.h` fix (§4) lands, any consumer
   that merely wants libstuff's networking stack — including, in principle,
   `test` or `benchmarks`, neither of which has any business knowing about
   the application layer — transitively drags in `BedrockPlugin.h`. That is
   the sharpest version of "exposed by accident" this rollup found: libstuff
   promises to be the thing every directory can depend on *without depending
   back*, and this one header currently breaks that promise for anyone who
   includes it.

**Inbound pressure, not yet arrived.** Two siblings have already escalated
generic utilities toward libstuff that don't exist here yet: `plugins`
flagged `BedrockPlugin_Cache::LRUMap` (generic LRU), the sqlite3-CLI arg/error
wrapper in `DB.h/.cpp`, and `scopedDisableNoopMode` (generic SQLite-noop RAII
guard) as candidates for `libstuff`; `test` flagged `fileAppend`/
`fileLockAndLoad` and the generic `ostream` container-printing operators in
`PrintEquality.h` the same way. None of these are misfits *of* libstuff today
— they don't exist here — so they don't inflate this directory's
`misfit_count`, but they are the expected next arrivals under root's
sink-down rule, and root's own rollup does not treat any of them as settled.
Expect libstuff to grow, not shrink, once those land.

## ROLLUP

<!-- ROLLUP
theme: Bedrock's dependency-free foundation layer - the base of the stack (libstuff -> sqlitecluster -> plugins -> root), string/table/exception/logging core, poll()-loop networking, SQL-adjacent typed values and wire format, async threading primitives, an in-house JSON model, and process diagnostics - undercut by a 4612-line catch-all file that still duplicates several of its own dedicated units.
exports: [libstuff-core (SString/STable/SException/SStopwatch/logging+assert macros), SData (generic HTTP-like wire message), SQResult/SQValue/SQliteParameter (typed SQL result set + bindable parameter), STCPManager/SHTTPSManager/SSSLState (poll()-loop socket/TLS/HTTPS stack), SLog/SSignal (process-wide logging + crash/signal handling), SThread/SRingBuffer/SFluentdLogger/SResolver (safe background-thread primitives and consumers), JSON::Value (in-house JSON document model, via JSON/)]
depends_on_dirs: [sqlitecluster (AutoScopeOnPrepare.h, SSignal.cpp - both flagged for removal, see escalate), root (SHTTPSManager.h -> BedrockPlugin.h - resolvable locally, see misfits), .]
depended_on_by: [root, benchmarks, plugins, sqlitecluster, test]
misfit_count: {high: 0, med: 7, low: 6}
resolved_locally: 10
escalate:
  - item: AutoScopeOnPrepare
    from: libstuff/AutoScopeOnPrepare.h,cpp
    why: includes sqlitecluster/SQLite.h directly and exists solely to scope a SQLite-specific callback; sqlitecluster's own rollup confirms no conflict, so the destination is now certain, but relocating it is still an action outside this directory
    suggested_home: sqlitecluster (confirmed)
  - item: SData::deserialize simdjson padding logic
    from: libstuff/SData.cpp
    why: over-allocates for one downstream simdjson parser's requirement; root's own Pass A rollup independently escalates this same item after seeing the full subtree and still finds no consumer, so the consumer likely lives outside this repo or the padding should be removed
    suggested_home: wherever SData payloads are simdjson-parsed - not visible anywhere in this repo's subtree per both libstuff and root
  - item: "_SSignal_StackTrace's direct call to SQLiteNode::KILLABLE_SQLITE_NODE->kill()"
    from: libstuff/SSignal.cpp
    why: a generic libstuff crash handler reaching directly into the sqlitecluster layer; unlike AutoScopeOnPrepare this has a genuine generic half (a crash-time callback hook) that could sink into libstuff/SSignal per root's rule, but confirming the split needs visibility into SQLiteNode's shutdown/lifecycle that sqlitecluster's one-line rollup doesn't carry
    suggested_home: "split - generic hook API in libstuff/SSignal, concrete kill() registration in sqlitecluster"
-->
