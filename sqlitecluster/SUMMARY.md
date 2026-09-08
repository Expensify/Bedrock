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

<!-- ROLLUP
theme: Replicated-SQLite engine — the transaction/journal handle, the leader/follower consensus node, and the wire-format/pooling plumbing that carries commands and connections through the cluster.
exports: [SQLite, SQLiteNode, SQLitePeer, SQLiteCommand, SQLiteClusterMessenger, SQLitePool, SQLiteServer]
depends_on_dirs: [libstuff, plugins, root (BedrockCommand.h, BedrockServer.h)]
depended_on_by: []
misfit_count: {high: 1, med: 2, low: 6}
resolved_locally: 8
escalate:
  - item: "SQLite.cpp and SQLiteNode.cpp both include plugins/Compression.h and call BedrockPlugin_Compression::compress/decompress to (de)compress journal entries"
    from: sqlitecluster/SQLite.cpp, sqlitecluster/SQLiteNode.cpp
    why: the replication engine, which otherwise depends only on libstuff, depends upward on an application-level Bedrock command plugin — an inverted layering that two independent units converge on, so it is structural rather than a one-off
    suggested_home: a libstuff- or sqlitecluster-level compression primitive that plugins/Compression itself builds on, instead of the reverse; deciding the exact seam requires a view above sqlitecluster that also sees plugins/
-->
