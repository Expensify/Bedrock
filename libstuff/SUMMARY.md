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

**Resolved locally (8) — target confirmed to exist in this directory:**

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

**Escalate (5) — genuinely need a view above libstuff:**

- **AutoScopeOnPrepare** (`libstuff/AutoScopeOnPrepare.h/.cpp`) — `#include`s
  `sqlitecluster/SQLite.h` directly and exists only to scope a SQLite-specific
  callback. Suggested home `sqlitecluster` is a sibling directory this pass
  cannot see into (no `sqlitecluster/SUMMARY.md` in view) — the root needs to
  confirm there's a real landing spot there before this moves.
- **AutoTimer name collision** (`libstuff/AutoTimer.h/.cpp` vs. an unrelated
  `AutoTimer` in `BedrockCore.h`) — same name, different purpose, genuinely
  confusable; resolving needs visibility into wherever `BedrockCore.h` rolls
  up, which is outside libstuff.
- **`SData::deserialize` simdjson padding logic** (`libstuff/SData.cpp`) — a
  generic message container over-allocating 32 bytes to satisfy one
  downstream parser's requirement. No unit in any of libstuff's five clusters
  does simdjson parsing of `SData` payloads, so the actual consumer — and
  therefore the right owner of this padding decision — is outside this
  directory's visible children.
- **`SHTTPSManager` (class) depends on `BedrockPlugin.h`** — inverts
  libstuff's expected dependency direction on the application layer above it
  (`SStandaloneHTTPSManager`, in the same file, proves the coupling isn't
  load-bearing for the networking logic itself). Whether the fix is a thinner
  adapter living in Bedrock proper or something else is a layering call the
  root must make.
- **`_SSignal_StackTrace` calls `SQLiteNode::KILLABLE_SQLITE_NODE->kill()`**
  (`libstuff/SSignal.cpp`) — a generic crash handler reaching directly into
  `sqlitecluster` to kill peer connections on crash. Same shape as the
  `SHTTPSManager`/`BedrockPlugin` issue: a layering violation only the root
  can settle, since it involves a directory this pass doesn't see.

**Note on `JSON/`:** its own rollup reports `misfit_count: {high:0, med:2,
low:3}`, `resolved_locally: 5`, `escalate: []` — every misfit it found was
closed within `JSON/` itself. Nothing from it enters this directory's ledger.

## ROLLUP

<!-- ROLLUP
theme: Bedrock's dependency-free foundation layer - string/table/exception/logging core, poll()-loop networking, SQL-adjacent typed values and wire format, async threading primitives, an in-house JSON model, and process diagnostics - undercut by a 4612-line catch-all file that still duplicates several of its own dedicated units.
exports: [libstuff-core (SString/STable/SException/SStopwatch/logging+assert macros), SData (generic HTTP-like wire message), SQResult/SQValue/SQliteParameter (typed SQL result set + bindable parameter), STCPManager/SHTTPSManager/SSSLState (poll()-loop socket/TLS/HTTPS stack), SLog/SSignal (process-wide logging + crash/signal handling), SThread/SRingBuffer/SFluentdLogger/SResolver (safe background-thread primitives and consumers), JSON::Value (in-house JSON document model, via JSON/)]
depends_on_dirs: [sqlitecluster, .]
depended_on_by: []
misfit_count: {high: 0, med: 7, low: 6}
resolved_locally: 8
escalate:
  - item: AutoScopeOnPrepare
    from: libstuff/AutoScopeOnPrepare.h,cpp
    why: includes sqlitecluster/SQLite.h directly and exists solely to scope a SQLite-specific callback; confirming the landing spot needs sqlitecluster's own view
    suggested_home: sqlitecluster
  - item: AutoTimer name collision
    from: libstuff/AutoTimer.h,cpp
    why: a second, unrelated AutoTimer class exists in BedrockCore.h outside libstuff's visibility; resolving the collision needs a view spanning both
    suggested_home: null
  - item: SData::deserialize simdjson padding logic
    from: libstuff/SData.cpp
    why: over-allocates for one downstream simdjson parser's requirement, but no consumer of SData that does simdjson parsing is visible anywhere in libstuff
    suggested_home: wherever SData payloads are simdjson-parsed (not visible from libstuff)
  - item: SHTTPSManager (class, depends on BedrockPlugin.h)
    from: libstuff/SHTTPSManager.h
    why: inverts libstuff's expected dependency direction on the application layer above it; SStandaloneHTTPSManager in the same file proves the coupling isn't load-bearing
    suggested_home: near BedrockPlugin, or a thinner adapter in Bedrock proper leaving libstuff plugin-free
  - item: "_SSignal_StackTrace's direct call to SQLiteNode::KILLABLE_SQLITE_NODE->kill()"
    from: libstuff/SSignal.cpp
    why: a generic libstuff crash handler reaching directly into the sqlitecluster layer; a layering decision the root must settle
    suggested_home: null
-->
