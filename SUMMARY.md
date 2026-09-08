# Bedrock — root

## Theme

Root is Bedrock's application layer: the server object, the command
pipeline it drives commands through, the plugin contract that layer
exposes to `plugins/`, and the process entry point that wires all of it
together at startup. Everything here answers "what does it mean to *be*
a Bedrock node" — as distinct from `libstuff/` (dependency-free
utilities), `sqlitecluster/` (replicated-SQLite mechanics), and
`plugins/` (the command handlers themselves). If `sqlitecluster` is the
engine and `plugins` are the cargo, root is the vehicle body: it owns
the threads, the queues, the ports, the plugin registry, and the
shutdown state machine that make the other two layers into a running
server.

This is also the end of the rollup chain: every misfit escalated by
`benchmarks/`, `libstuff/`, `plugins/`, `sqlitecluster/`, and `test/`
lands here with nowhere further to go, alongside root's own 12 units'
findings. This document resolves all of it it can and names the rest as
genuinely open.

## Contents

| Name | Kind | Lines | Role |
|---|---|---|---|
| `BedrockServer` | unit | 2972 | Top-level per-node server: sync/worker threads, queues, ports, plugins, DB pool, shutdown state machine; implements `SQLiteServer` |
| `BedrockCommand` | unit | 879 | Per-request object carrying a command through prePeek/peek/process/postProcess |
| `BedrockCore` | unit | 575 | Runs one command's lifecycle against a specific SQLite db/transaction |
| `BedrockCommandQueue` | unit | 315 | Thread-safe, time-ordered queue of commands |
| `BedrockBlockingCommandQueue` | unit | 354 | `BedrockCommandQueue` subclass with per-key sliding-window rate limiting, for the blocking commit thread |
| `BedrockPlugin` | unit | 212 | Abstract plugin base class + global plugin-factory registry |
| `BedrockConflictManager` | unit | 91 | Tallies which command touches which tables on commit conflict, for profiling |
| `ConflictLockGuard` | unit | 126 | RAII guard over a shared, LRU-pruned, refcounted mutex pool keyed by conflict identifier |
| `main` | unit | 457 | Process entry point: args, one-shot maintenance flags, plugin loading, main poll loop |
| `bedrockVersion` | unit | 12 | `VERSION` macro from the build's git revision — actually used |
| `VMTouch` | unit | 232 | Page-cache inspection/pre-warm utility, adapted from a third-party tool |
| `version` | unit | 4 | Dead placeholder `SVERSION` macro, unreferenced anywhere |
| `benchmarks/` | dir | — | Standalone harness timing individual libstuff functions |
| `libstuff/` | dir | — | Dependency-free foundation: strings, networking, SQL-adjacent types, threading, JSON, logging |
| `plugins/` | dir | — | `BedrockPlugin_*` command-handler implementations (Cache, Compression, DB, Jobs, MySQL) |
| `sqlitecluster/` | dir | — | Replicated-SQLite engine: transaction/journal handle, consensus node, wire/pooling plumbing |
| `test/` | dir | — | Test tree root: harness, single-node suite, multi-node cluster-integration suite |

## Coherence

Ten of the twelve root units cohere tightly: `BedrockServer`,
`BedrockCommand`, `BedrockCore`, `BedrockCommandQueue` +
`BedrockBlockingCommandQueue`, `BedrockPlugin`, `BedrockConflictManager`,
`ConflictLockGuard`, `main`, and `bedrockVersion` are all direct
participants in the same request lifecycle and the same process
lifecycle — no other directory could plausibly hold them.

Two units don't fit:

- **`VMTouch`** has no Bedrock dependency at all beyond `libstuff/libstuff.h`
  — it is a generic OS/filesystem utility that happens to sit at root
  instead of next to the rest of libstuff's reusable helpers. This is
  the same class of misfit libstuff itself would flag if it could see
  it, just sitting on the wrong side of the boundary.
- **`version.h`** is confirmed-dead code (its `SVERSION` macro is
  referenced nowhere), superseded by `bedrockVersion.h`'s `VERSION`.
  It doesn't cohere with anything because it isn't doing anything.

Neither rises to "this is really two directories" — the flat root is
one coherent layer with two strays, not a trenchcoat.

## Misfits

27 items reach this rollup: 13 from root's own 12 units, plus 14
consolidated from the escalate lists of `benchmarks/` (1), `libstuff/`
(5), `plugins/` (4), `sqlitecluster/` (1), and `test/` (3). As the last
stop, each gets a concrete resolution here, or is named as a genuine
open question. 23 resolve locally; 4 are marked `escalate` below because
they require either a human/product call or information this pass
cannot see.

### Resolved locally (23)

**Root's own (11 of 13 resolved):**

- `BedrockBlockingCommandQueue::IdentifierState/StateMap` (low) — generic
  string-keyed rate limiter with no `BedrockCommand` dependency, trapped
  as private nested machinery. → extract to `libstuff` as a reusable
  rate limiter.
- `BedrockCommand::GrowOnlyList<T>` (low) — generic append-only container,
  no command dependency. → move to `libstuff`.
- `BedrockCommand.cpp: finalizeTimingInfo` (low) — a sizeable
  timing/logging routine bundled into the command class. This is a
  style preference, not a layering problem; leave as-is or split into a
  private helper at root's own discretion, no cross-directory move
  needed.
- `BedrockCore::AutoScopeRewrite` (low) — file-local class missing
  `static`/anonymous-namespace. → mechanical fix, add `static`.
- `BedrockPlugin::verifyAttributeInt64/Size/Bool/Date`, `isValidDate`
  (med) — static helpers operating only on `SData`, no plugin-lifecycle
  dependency. → move to `libstuff/SData.h`.
- `BedrockServer::__quiesceLock/__quiesceShouldUnlock/__quiesceThread`
  (high) — reserved double-underscore identifiers with external
  linkage, holding state that belongs to `BedrockServer`. → rename
  (drop the reserved prefix) and fold into `BedrockServer` as private
  members; mechanical, not architectural.
- `BedrockServer::commandPortSuppressionReasons` (med) — confirmed dead:
  doc comment references a type that doesn't exist, member is never
  read or written. → delete.
- `BedrockServer::_control` (med) — one ~250-line function owns tuning
  knobs for several independent subsystems (conflict retry/page-lock
  policy, blocking-queue rate limits, DB quiesce, socket-thread caps,
  sync priority). → decompose by delegating each knob to its owning
  subsystem (`BedrockConflictManager`, `BedrockBlockingCommandQueue`,
  the quiesce mechanism itself) instead of `BedrockServer` reaching into
  all of them directly.
- `VMTouch` (whole unit, low) → move to `libstuff/VMTouch.h`/`.cpp`.
- `main.cpp: VacuumDB/BackupDB/BACKUP_DIR` (low) — the one hardcoded
  path in an otherwise fully-args-configurable entry point. → make
  `BACKUP_DIR` a flag like everything else in `main.cpp`.
- `main.cpp: loadPlugins` (med) — self-contained dlopen/dlsym
  plugin-registration subsystem living as a free function in
  `main.cpp`. → move into `BedrockPlugin.h`/`.cpp`, next to
  `g_registeredPluginList`, which it populates.
- `version.h` (whole file, med) — confirmed dead code. → delete.

**Consolidated from children (12 of 14 resolved):**

- `benchmarks/main.cpp`'s `--baseline` git stash/checkout/rebuild
  orchestration (low) → extract into an external script (e.g.
  `benchmarks/run-baseline-compare.sh`) that runs the plain benchmark
  binary twice and diffs results; keep the binary itself free of
  source-control operations.
- `libstuff/AutoScopeOnPrepare` (med) — includes `sqlitecluster/SQLite.h`
  directly for one SQLite-specific callback. → move to `sqlitecluster`,
  where its only dependency already lives.
- `libstuff/AutoTimer` name collision with `BedrockCore::AutoTimer`
  (med) — two unrelated classes, same name, different layers. → rename
  `libstuff::AutoTimer` (e.g. `SScopedTimer`) since it's the
  generic/reusable one; leave `BedrockCore::AutoTimer` as the
  command-specific nested type it already is.
- `libstuff/SHTTPSManager` depending on `BedrockPlugin.h` (med) —
  inverts libstuff's expected dependency direction; `SStandaloneHTTPSManager`
  in the same file proves the coupling isn't load-bearing. → move
  `SHTTPSManager` out of `libstuff` to root, beside `BedrockPlugin`
  (which it already depends on); keep `SStandaloneHTTPSManager` in
  `libstuff` as the actually-generic sibling. See "Layering pattern"
  below.
- `libstuff/SSignal.cpp`'s call into
  `SQLiteNode::KILLABLE_SQLITE_NODE->kill()` (med) — a generic crash
  handler reaching directly into `sqlitecluster`. → replace the direct
  call with a callback registration point in `libstuff/SSignal`
  (a `void(*)()` or `std::function` hook that `sqlitecluster` installs
  at startup), so `libstuff` stops naming `SQLiteNode` at all. See
  "Layering pattern" below.
- `plugins/Cache.h: LRUMap` (low) — fully generic string-keyed LRU
  tracker, trapped private to one plugin. → move to `libstuff`.
- `plugins/DB.h,.cpp: Sqlite3QRFSpecWrapper/parseSQLite3Args/generateErrorContextMessage`
  (med) — generic sqlite3-CLI arg/error layer wrapping `libstuff/qrf.h`.
  → move to `libstuff/qrf.h` or a new unit alongside it.
- `plugins/Jobs.cpp: scopedDisableNoopMode` (low) — generic SQLite-noop
  RAII guard, file-local and unreachable by other plugins. → move to
  `libstuff` or `sqlitecluster/SQLite.h`.
- `sqlitecluster/SQLite.cpp` and `SQLiteNode.cpp` including
  `plugins/Compression.h` to (de)compress journal entries (high) — the
  replication engine depending upward on an application-level command
  plugin. → extract a thin, Bedrock-agnostic compression primitive into
  `libstuff`; have both `sqlitecluster` and `plugins/Compression` build
  on it, rather than `sqlitecluster` depending on the plugin. See
  "Layering pattern" below.
- `test/clustertest/testplugin/TestPlugin.cpp: fileAppend/fileLockAndLoad`
  (low) — generic flock-guarded file I/O helpers, no plugin-specific
  logic. → move to `libstuff`.
- `test/lib/PrintEquality.h`'s `operator<<` for `list`/`set`/`map`/`optional`
  (low) — generic container-printing utilities already built on
  libstuff's `SComposeList`. → move to a shared stream-formatting
  header in `libstuff`.

### Escalated — genuinely open (4)

These are marked `escalate` with `suggested_home: null` because they
need either a human/product decision or information no summary in this
subtree can supply (per spec: say so rather than reading around it).

1. **`BedrockCommand::getMethodName`'s `returnValueList` legacy hack**
   (`BedrockCommand.cpp`) — the code comments it as support for "one
   legacy plugin format (Auth's old Get)". But `plugins/SUMMARY.md`'s
   export list names only five plugins — Cache, Compression, DB, Jobs,
   MySQL — no Auth. From what's visible in this pass, it cannot be
   confirmed whether an Auth plugin still exists anywhere in the repo.
   If it's gone, this is dead code to delete from the generic base
   command class; if it still exists (outside the five plugins/ has),
   the hack belongs there instead. **This is also a gap in
   `plugins/SUMMARY.md`**: it does not mention Auth at all, one way or
   the other, which is exactly the "child rollup inadequate for a
   judgement" case the spec calls out.
2. **`libstuff/SData.cpp: SData::deserialize`'s simdjson padding logic**
   — over-allocates for a downstream simdjson parser's requirement, but
   no consumer of `SData` that does simdjson parsing is visible in
   `libstuff`, `plugins`, `sqlitecluster`, `test`, or `benchmarks`. The
   consumer may live outside this subtree entirely (a closed-source or
   downstream integration), or the logic may be speculative/dead.
   Needs someone with visibility into actual deployments to say which.
3. **`plugins/Jobs.cpp: upgradeDatabase`'s Expensify-specific index
   literals** (`jobsPriorityNextRunManualSmartScan*`,
   `jobsManualSmartscanReceiptID`, `jobsPriorityNextRunWWWProd/WWWStag`)
   — deployment-specific literals baked into otherwise-generic
   job-queue schema DDL. Whether Bedrock should grow a customization
   seam (e.g. a config-driven index list) to keep the generic engine
   free of one deployer's naming, or whether this is accepted as
   intentional given Bedrock and Expensify are developed together, is a
   product/organizational call this pass cannot make.
4. **`test/tests/QueryTest.cpp: QueryTest::testPercentile`** — tests a
   compiled-in SQLite percentile/median aggregate extension that has no
   visible owning unit anywhere in the subtree: it isn't in
   `libstuff/SUMMARY.md`'s or `sqlitecluster/SUMMARY.md`'s exports, and
   no other child mentions it. Before deciding whether this test even
   belongs in `test/tests`, someone needs to confirm where (or whether)
   that extension is actually registered.

## Layering pattern

Three of the escalate-turned-resolved items above are the same failure
mode recurring at two different seams:

- `libstuff` (blast radius 68, the foundation) depends upward on
  `sqlitecluster` (`AutoScopeOnPrepare`, `SSignal`'s call into
  `SQLiteNode`) **and** on `BedrockPlugin` (`SHTTPSManager`).
- `sqlitecluster` (the replication engine) depends upward on
  `plugins/Compression`.

The pattern is **upward borrowing of a small piece of higher-layer
functionality instead of sinking a shared primitive downward**: a
lower layer needs one narrow thing — a kill hook, a scope-guard, a
compression call — that happens to already be implemented in a layer
above it, so it just includes that layer's header rather than the
shared primitive being factored down to (or below) the lower layer and
both sides building on it. Every instance in this pass follows the
same shape, which is what makes it structural rather than three
unrelated one-offs.

The intended dependency order — consistent with `class_hierarchy.md`
and with how `libstuff/SUMMARY.md`, `plugins/SUMMARY.md`, and
`sqlitecluster/SUMMARY.md` each describe themselves — is:

```
libstuff  (foundation, depends on nothing in this repo)
   ↑
sqlitecluster  (depends only on libstuff)
   ↑
plugins  (depends on libstuff + sqlitecluster)
   ↑
root / "Bedrock proper"  (BedrockServer, BedrockCommand, BedrockPlugin;
                           depends on all three below)
```

Every violation found in this pass runs the wrong direction across
this stack. The fix in each case is the same: identify the minimal
generic primitive actually needed (a kill-hook interface, a compression
function, a scope guard), sink it to the lowest layer both sides can
depend on — usually `libstuff` — and have the higher-layer thing rebuild
on top of that shared primitive. Concrete instances and proposed
homes are listed under "Resolved locally" above; the general direction
should be treated as a standing rule for future work in `libstuff` and
`sqlitecluster`, not just the three cases visible today.

## Drift from class_hierarchy.md

`class_hierarchy.md`'s diagram covers a narrow, specific slice: the
`STCPManager` family (`STCPManager`/`STCPNode`/`SQLiteServer`
/`SStandaloneHTTPSManager`) and how `SQLiteNode` and `BedrockServer`
hang off it. It does not attempt to depict the command pipeline at all
— `BedrockCommand`, `BedrockCommandQueue`, `BedrockCore`, `BedrockPlugin`,
`ConflictLockGuard`, `BedrockConflictManager` are simply out of its
scope, not drifted from it. Within the slice it does cover:

- **`SQLiteServer` → `BedrockServer`**: matches. `BedrockServer`'s own
  unit record confirms it "implement[s] the `SQLiteServer` interface
  `SQLiteNode` uses to call back into it" — exactly the abstract-interface
  boundary the diagram and its notes describe.
- **TODO: un-nest `STCPManager`'s nested classes** (`Socket`, `Port`) —
  cannot be confirmed resolved or outstanding from what this pass can
  see. `libstuff/SUMMARY.md` bundles `STCPManager`/`SHTTPSManager`
  /`SSSLState` together in its exports but does not say whether
  `Socket`/`Port` are still nested. Nothing in libstuff's escalate list
  flags this as fixed or as a live misfit either. **Absent evidence
  either way, treat the TODO as still outstanding** — but confirming it
  precisely would require reading `libstuff/STCPManager.h` directly,
  which this pass's rules forbid. That is worth flagging as a real
  limit of the bottom-up process: a hand-maintained TODO like this one
  can silently go stale (in either direction) with no summary ever
  surfacing the change.
- **TODO: collapse `STCPNode`/`SQLiteNode`** — also still outstanding,
  and the split is structurally reinforced rather than healed:
  `sqlitecluster/SUMMARY.md` exports `SQLiteNode` but does not mention
  `STCPNode` at all, implying `STCPNode` still lives up in `libstuff`
  while `SQLiteNode` lives down in `sqlitecluster` — i.e. the "one thing
  in two halves" the note describes is now also split across the
  libstuff/sqlitecluster directory boundary, not just a class boundary
  within one file. Collapsing them, if it happens, will need to either
  pick one directory or introduce a seam between them; that is a bigger
  change than the note's original phrasing suggests.
- **New drift the diagram doesn't mention**: `SHTTPSManager` (as
  opposed to the diagram's `SStandaloneHTTPSManager`) is part of the
  same `STCPManager` family and depends on `BedrockPlugin.h` — a
  dependency that reaches clean past `SQLiteServer`/`BedrockServer` to
  the plugin layer. The diagram's clean three-branch tree from
  `STCPManager` doesn't have a place for this at all. This is the same
  item flagged under "Layering pattern" above; it is worth stating
  separately here because it is drift the maintained diagram doesn't
  even acknowledge as a possibility, not just an unfinished TODO.

## Is the flat root a problem?

No, not as a coherence problem — see "Coherence" above: ten of twelve
units are one tight layer (the Bedrock server/command/plugin-contract
code), and grouping them under a `bedrock/` subdirectory would not
change any dependency or boundary, only presentation. `sqlitecluster/`
and `plugins/` are grouped because each holds a *family* of similar,
swappable things (multiple plugins, multiple cluster-protocol
concerns); root's units aren't a family of similar things, they're one
system's distinct organs (server, command, queue, plugin-contract,
entry point) that don't repeat or swap out. Flat is an accurate
reflection of that.

What the flat root *does* do is make the two genuine misfits
(`VMTouch`, `version.h`) less visible than they'd be inside a grouped
directory, where a stray generic-utility file or a stray dead file
would stand out against its siblings. That's a minor discoverability
cost, not a structural one. If the directory grows further, introducing
a `bedrock/` subdirectory for the ten cohesive units (mirroring
`plugins/` and `sqlitecluster/`) would be a reasonable, low-risk,
purely-cosmetic move — but nothing in this pass makes it urgent.

<!-- ROLLUP
theme: Bedrock's application layer — the per-node server, the command pipeline it drives, the plugin contract plugins/ implements against, and the process entry point — plus final resolution point for every misfit escalated from the whole subtree.
exports: [BedrockServer, BedrockCommand, BedrockCore, BedrockCommandQueue/BedrockBlockingCommandQueue, BedrockPlugin, BedrockConflictManager/ConflictLockGuard, main() entry point]
depends_on_dirs: [libstuff, sqlitecluster, plugins]
depended_on_by: []
misfit_count: {high: 3, med: 12, low: 12}
resolved_locally: 23
escalate:
  - item: "BedrockCommand::getMethodName's 'returnValueList' legacy Auth hack"
    from: BedrockCommand.cpp
    why: "references an Auth plugin that does not appear in plugins/SUMMARY.md's export list (only Cache, Compression, DB, Jobs, MySQL); cannot confirm from visible summaries whether Auth still exists or this is dead code"
    suggested_home: null
  - item: "SData::deserialize simdjson padding logic"
    from: libstuff/SData.cpp
    why: "no consumer of SData that does simdjson parsing is visible anywhere in this subtree (libstuff, plugins, sqlitecluster, test, benchmarks); the consumer may not exist in this repo at all"
    suggested_home: null
  - item: "BedrockPlugin_Jobs::upgradeDatabase Expensify-specific index literals"
    from: plugins/Jobs.cpp
    why: "deployment-specific job-name literals baked into generic job-queue schema DDL; whether Bedrock needs a customization seam here or should accept this as intentional is a product/organizational decision, not an engineering one"
    suggested_home: null
  - item: QueryTest::testPercentile
    from: test/tests/QueryTest.cpp
    why: "tests a compiled-in SQLite percentile/median aggregate extension with no owning unit visible anywhere in the subtree; need to confirm the extension's existence and location before this test's home can be decided"
    suggested_home: null
-->
