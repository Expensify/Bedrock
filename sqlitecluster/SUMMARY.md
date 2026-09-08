# sqlitecluster/

## 1. Theme

`sqlitecluster` is Bedrock's replicated-database engine: it turns a plain
`sqlite3` handle into a node in a leader/follower cluster. It owns the
transaction/journal lifecycle on top of SQLite, the peer-to-peer replication
and leader-election state machine, the wire-level command representation that
moves through the cluster, and the handle-pooling/callback plumbing that lets
a host server (Bedrock) embed all of that. Nothing here knows about specific
Bedrock commands or HTTP-level concerns — its job is consensus and durable,
ordered replication of arbitrary transactions, not any particular workload.

## 2. Contents

| Unit | Files | Lines | Role |
|---|---|---|---|
| SQLite | SQLite.h/.cpp | 2374 | Wraps a sqlite3 handle: transactions, journal bookkeeping, commit-order mutex, query caching, conflict reporting, authorizer/rewrite hooks. |
| SQLiteNode | SQLiteNode.h/.cpp | 2964 | Distributed leader/follower node: peer connections, election state machine (SEARCHING…LEADING/FOLLOWING), transaction replication. |
| SQLitePeer | SQLitePeer.h/.cpp | 425 | One other cluster node as seen from this node: socket, replication/consensus state, standup voting. |
| SQLiteClusterMessenger | SQLiteClusterMessenger.h/.cpp | 430 | Escalates a command from this node to leader/peer/all-peers over a pooled TCP socket and fills in the response. |
| SQLiteCommand | SQLiteCommand.h/.cpp | 154 | Base wire-format command: request/response, cross-node id, completion flag, escalation timing; base of BedrockCommand. |
| SQLiteCore | SQLiteCore.h/.cpp | 86 | Commit/rollback of a wrapped SQLite&, gated on still being leader; optional plugin-notification hook on prepare. |
| SQLitePool | SQLitePool.h/.cpp | 179 | Bounded pool of SQLite handles for worker threads, with an RAII scoped-handle wrapper. |
| SQLiteServer | SQLiteServer.h | 21 | Pure-virtual callback interface a SQLiteNode uses to reach back into its host server. |
| SQLiteUtils | SQLiteUtils.h/.cpp | 44 | Static helper: random table-unique int64 ID generation. |

## 3. Coherence

Eight of the nine units form a tight, singular subsystem: SQLite (the handle),
SQLiteNode/SQLitePeer (the replication/consensus engine), SQLiteCommand/
SQLiteClusterMessenger (how a command travels the cluster), SQLiteCore
(commit gating), SQLitePool (handle lifecycle), and SQLiteServer (the
embedding contract) all depend on each other directly and none of them make
sense outside this directory. SQLiteUtils is the loose one: a single static
`getRandomID` method in its own "Utils" class (name_fit 3, its own unit noted
"a one-function Utils class invites unrelated helpers to accrete"). It is not
a trenchcoat-second-directory situation — one small, correctly-scoped
function — but it is the weakest fit and the first place accreting cruft
would land.

## 4. Misfits

Two units (SQLite and SQLiteNode) independently reported the same problem
from their own vantage point: both `#include <plugins/Compression.h>` and
call `BedrockPlugin_Compression::compress/decompress` (SQLite.cpp, to
compress/decompress journal entries and register SQLite UDFs; SQLiteNode.cpp,
in `_handleBeginTransaction` and `_recvSynchronize`, to decompress replicated
journal entries). At the directory level this is one finding, not two: the
replication engine — which otherwise depends only on `libstuff` — reaches
upward into `plugins/`, a directory of application-level Bedrock command
plugins that is supposed to depend on `sqlitecluster`, not the reverse. That
two independent units converge on the identical inverted dependency means
it's load-bearing, not an accident in one file, and the fix (pulling a
compression primitive down into `libstuff` or a `sqlitecluster`-level helper
that `plugins/Compression` itself builds on) is a decision about the
boundary between these two directories — it needs a view that can see both
sides, i.e. escalated above `sqlitecluster`.

Everything else consolidates to items whose better home is inside this
directory:

- **SQLite::enableRewrite/setRewriteHandler** and **SQLite::setUpdateNoopMode**
  (both documented as existing only for mocked/`mockRequest` command testing,
  yet implemented as first-class state on the core DB class) — resolved
  here: the better home is a test-support subclass or a separate
  mock-support file inside `sqlitecluster`, not the core `SQLite` class
  itself.
- **Commented-out HC-Tree slow-commit diagnostic block** in `SQLite::commit()`
  — resolved here: delete it or gate it behind a runtime flag in place.
- **SQLite::SharedData::writeLock** (exists specifically for the BlockWrites
  command, per its own comment, but is a generic-looking member of the
  cross-handle `SharedData`) — resolved here: keep it in `SharedData` (all
  handles genuinely need to share it) but rename/document it as
  command-motivated rather than general-purpose.
- **SQLiteCommand::preprocessRequest's commandExecuteTime deprecation branch**
  — resolved here: split the deprecation-specific handling out of the
  generic preprocessing method, within the same file.
- **SQLiteNode::KILLABLE_SQLITE_NODE / NODE_KILLED** (a global static
  pointer/flag for process-wide signal handling to reach into one node
  instance) — resolved here: fold into a small dedicated lifecycle/signal
  helper inside `sqlitecluster` rather than loose globals on the node class.
- **SQLiteNode::_priority / _syncPeer** ("Remove"-commented, linked to open
  issues 208449/208439) — resolved here: already tracked design debt: no new
  action needed beyond what the linked issues cover.
- **SQLitePeer.h stale "see friend class declaration above" comment** (no
  friend is declared in the file) — resolved here: fix or delete the stale
  comment.

Pass B does not change any of the above resolutions. It does sharpen the one
escalated item — see §5.

## 5. Role in the system

The intended layering is `libstuff -> sqlitecluster -> plugins -> root`.
sqlitecluster is the layer that turns libstuff's bare `sqlite3` handle and
networking primitives into a consensus-replicated database — nothing else in
the tree does distributed leader election or owns the journal/transaction
lifecycle, so that half of "role in the system" is unambiguous and no sibling
contests it.

**Boundary with libstuff:** clean from this side — sqlitecluster only
consumes libstuff (sqlite3.h, SQliteParameter, SQResult, SPerformanceTimer,
SDeburr, libstuff.h, STCPManager, SSynchronizedQueue, SRandom) and exports
nothing back. The leak on *this* boundary runs the other way: libstuff's own
rollup escalates `AutoScopeOnPrepare` (a libstuff file that `#include`s
`sqlitecluster/SQLite.h` to scope a SQLite-specific callback) and
`_SSignal_StackTrace`'s direct call to `SQLiteNode::KILLABLE_SQLITE_NODE->kill()`
— i.e., libstuff already expects sqlitecluster to be the landing spot for
both. That is consistent with this directory's role: `AutoScopeOnPrepare`
belongs here (it's exactly the SQLite-prepare-scoped callback machinery
`SQLiteCore`'s own notification hook already does), and the `KILLABLE_SQLITE_NODE`
static is already a sqlitecluster-owned mechanism (see §4) that a lower layer
is reaching into rather than sqlitecluster reaching out — the inversion is
libstuff's to fix, not this directory's, but this directory is the correct
target for both.

**Boundary with plugins — the one that leaks from this side:** the intended
order has plugins depending on sqlitecluster, never the reverse, and plugins'
own rollup agrees (its `depends_on_dirs` is `[libstuff, sqlitecluster]` only —
it does not believe sqlitecluster depends on it). But `SQLite.cpp` and
`SQLiteNode.cpp` both `#include <plugins/Compression.h>` and call
`BedrockPlugin_Compression::compress/decompress` directly to (de)compress
journal entries. This is a real, active dependency the other side's own
rollup doesn't record — sqlitecluster escalated it in Pass A; plugins did not
flag it, because from inside `plugins/Compression.h` looks like an ordinary,
correctly-placed plugin with no outside reach of its own.

**Concrete fix, applying root's rule (sink the shared primitive to the
lowest layer both sides can use):** the piece both directories actually need
is a dictionary-based zstd compress/decompress over a raw byte buffer — that
has no inherent dependency on SQL, UDF registration, or the plugin/command
framework. That primitive belongs in **libstuff** (e.g.
`libstuff/SCompress.h/.cpp`), as a peer to libstuff's other self-contained
algorithmic units (SDeburr, SReplace) rather than anything SQLite- or
plugin-aware.
- **sqlitecluster keeps:** the call sites in `SQLite.cpp`
  (compress/decompress journal entries, register the UDFs at the storage
  layer) and `SQLiteNode.cpp` (`_handleBeginTransaction`,
  `_recvSynchronize`) — but calling libstuff's primitive directly.
  `#include <plugins/Compression.h>` and every `BedrockPlugin_Compression`
  reference are removed from both files; sqlitecluster's dependency on
  `plugins` goes to zero, matching the intended order.
- **plugins keeps:** `BedrockPlugin_Compression` as the SQL-facing surface —
  registering `compress()`/`decompress()` as SQLite UDFs, owning the
  `zstdDictionaries` table schema and startup dictionary loading, and
  exposing the static helpers for non-SQL command callers — all now built on
  top of the same libstuff primitive rather than being its only
  implementation.
- This is still an escalation, not a local resolution: it requires editing a
  third directory (libstuff) this agent does not own, so root has to
  arbitrate it, but the destination is now concrete rather than open.

## 6. Inbound expectations

What plugins, test, and root actually rely on sqlitecluster for is already
exported correctly: `SQLiteNode`, `SQLitePeer`, `SQLiteCommand`,
`SQLiteClusterMessenger`, `SQLitePool`, `SQLiteServer`, `SQLiteCore` all
appear in the consuming siblings' own dependency lists with no gap visible
from here.

`SQLiteServer` is the one sanctioned channel for sqlitecluster to reach
*upward* into its host without a compile-time dependency (it's a pure-virtual
interface the host implements: `onNodeLogin`, `notifyStateChangeToPlugins`,
`blockCommandPort`). The Compression dependency bypasses this sanctioned
channel entirely — it reaches for a concrete plugin header and a concrete
class instead of going through the interface built for exactly this purpose.
That's a second way to describe the same violation: sqlitecluster already
has an upward-callback mechanism; Compression just doesn't use it, and
sinking the primitive to libstuff (§5) is preferred over routing compression
through `SQLiteServer` since compression isn't inherently plugin-owned state.

Nothing else a sibling depends on appears missing or accidentally exposed;
the plugins/Compression reach is the only outbound-facing problem this
directory has.

<!-- ROLLUP
theme: Replicated-SQLite engine — the transaction/journal handle, the leader/follower consensus node, and the wire-format/pooling plumbing that carries commands and connections through the cluster.
exports: [SQLite, SQLiteNode, SQLitePeer, SQLiteCommand, SQLiteClusterMessenger, SQLitePool, SQLiteServer]
depends_on_dirs: [libstuff, plugins, root (BedrockCommand.h, BedrockServer.h)]
depended_on_by: [plugins, test, root]
misfit_count: {high: 1, med: 2, low: 6}
resolved_locally: 8
escalate:
  - item: "SQLite.cpp and SQLiteNode.cpp both include plugins/Compression.h and call BedrockPlugin_Compression::compress/decompress to (de)compress journal entries"
    from: sqlitecluster/SQLite.cpp, sqlitecluster/SQLiteNode.cpp
    why: the replication engine, which otherwise depends only on libstuff, depends upward on an application-level Bedrock command plugin — an inverted layering that two independent units converge on, so it is structural rather than a one-off; plugins' own rollup does not record sqlitecluster as a dependent, confirming this reach is unacknowledged from the other side
    suggested_home: "libstuff (e.g. libstuff/SCompress.h/.cpp) for a raw dictionary-based zstd compress/decompress primitive with no SQL or plugin awareness; sqlitecluster calls it directly and drops the plugins/Compression.h include entirely, while plugins/Compression keeps the UDF registration, zstdDictionaries schema, and dictionary-loading built on top of the same primitive. Requires editing libstuff, a directory neither sqlitecluster nor plugins owns, so root must arbitrate the actual move."
-->
