# test/clustertest/testplugin

## Theme

This directory gives the cluster-test binary a way to exercise Bedrock server
code paths that are otherwise unreachable from outside the process. It does
this by implementing `TestPlugin`, a `BedrockPlugin` loaded only in test
builds, whose commands deliberately trigger crashes, slow queries, HTTPS
timeouts, escalation, and prepare/commit-hook behavior on demand.

## Contents

| Child | Lines | What it is |
|---|---|---|
| `ExternPointer.cpp` | 2 | A single uninitialized global pointer, deliberately dereferenced out of bounds by TestPlugin's `generatesegfault*` commands to crash the server on demand. |
| `TestPlugin.{h,cpp}` | 961 | The `BedrockPlugin_TestPlugin` itself: a test HTTPS manager, a `TestPluginCommand` dispatcher covering dozens of test scenarios, and supporting helpers/state for those scenarios. |

No subdirectories.

## Coherence

Both children serve the same job — giving clustertest hooks into internal
server behavior — and belong together. `ExternPointer.cpp` is a one-symbol
satellite of `TestPlugin.cpp` (which `extern`-declares it) rather than an
independent unit; splitting it into its own translation unit is the only
thing that makes it look separate from the plugin it serves.

## Misfits

1. **`__pointerToFakeIntArray` as its own translation unit** (low,
   resolved-locally). Its only user is `TestPlugin.cpp`. Better home: fold it
   into `TestPlugin.cpp` (or a `TestPlugin`-local header) as a static/file-local
   variable instead of a separate `.cpp`. Resolved here — stays in this
   directory, just not as a standalone file.

2. **`BedrockPlugin_TestPlugin::arbitraryData` / `dataLock`** (low,
   resolved-locally). Ad hoc static cross-command scratch storage that
   bypasses normal command/database plumbing. This is intentional test-only
   convenience (cross-command state for scenarios like `testescalate`), not a
   design accident, and has no better home outside a test plugin. Accepted as
   a known quirk of this directory.

3. **`fileAppend` / `fileLockAndLoad`** (low, escalate). Generic
   flock-guarded file-append/read helpers with no relation to plugin or
   command dispatch logic, defined as plain global functions in
   `TestPlugin.cpp`. Nothing about them is test-plugin-specific — they're
   general-purpose file I/O utilities that happen to live here because only
   `testescalate` currently needs them. Their better home is a shared utility
   location, which is outside this directory's remit to decide. (Pass B
   revises the suggested destination — see §5/§6.)

## 5. Role in the system

This directory owns the *server-side* half of clustertest's crash/timeout/
escalation test surface: it is a `BedrockPlugin` compiled into the cluster
test binary, and its only defined interface outward is the set of command
`methodLine`s it registers (`generatesegfault*`, `testescalate`, HTTPS-timeout
commands, prepare/commit-hook commands, and the rest of `TestPluginCommand`'s
dispatch table). Its heaviest — in fact its only confirmed — consumer is
`test/clustertest/tests`: nearly every unit there that exercises a crash,
timeout, escalation, or commit-hook scenario is driving a command this plugin
implements. Checked directly with a targeted grep (sibling rollups don't
carry symbol-level command lists, so this required stepping outside the
bounded inputs): no file under `test/clustertest/tests` `#include`s
`TestPlugin.h`/`TestPlugin.cpp`. The boundary is wire-protocol only, exactly
as a plugin/client split should be, and it does not leak.

The parent's rollup separately flags `BedrockClusterTester.h` (currently
living directly in `test/clustertest`) as a candidate to move to `test/lib`,
since it is generic `ClusterTester<BedrockTester>` composition with no
remaining clustertest-specific logic once this directory's own misfit is
fixed. That move is orthogonal to this directory: `testplugin` never
includes or references `BedrockClusterTester`, so relocating it changes
nothing here.

## 6. Inbound expectations

Given the parent and sibling context, this directory owes outward exactly one
thing: a stable command surface. `test/clustertest/tests` depends on specific
command names existing and behaving as documented (crash-on-demand, an
HTTPS send that never completes, escalation ordering via `testescalate`,
commit-hook interaction) — that dependency is the intended integration
point, not an accident. Auditing every command name the sibling's 36 units
expect against every command this plugin implements would require reading
that whole subtree, which is out of bounds for this pass; that check
belongs to whoever holds both children's contents (the parent).

Revisiting item 4 above in light of that: **`fileAppend` / `fileLockAndLoad`**'s
suggested destination should change. Pass A proposed `libstuff` as "a shared
utility location" without evidence anyone else needs file-locking helpers —
a repo-wide grep confirms `flock()`/`LOCK_EX`/`LOCK_UN` appear nowhere else
in the repository outside this one file. `libstuff` is also, per context now
available to this pass, itself being decomposed into smaller pieces, which
makes it the wrong direction to add a brand-new test-only utility to right
now. These two functions serve exactly one test scenario (`testescalate`,
in this very directory) and are pure test convenience, not production
infrastructure. `test/lib` already plays the "shared test-support utility"
role in this tree (`BedrockTester`, `TestHTTPS`, `PortMap`, `tpunit++`) and
is the more honest destination — escalating there instead of into a
shrinking production library.

<!-- ROLLUP
theme: Test-only BedrockPlugin (TestPlugin) that exposes internal server code paths as commands so the clustertest binary can exercise crashes, timeouts, escalation, and commit hooks.
exports: [BedrockPlugin_TestPlugin, TestPluginCommand, TestHTTPSManager, generatesegfault* crash commands, testescalate ordering helpers]
depends_on_dirs: [libstuff, sqlitecluster, .]
depended_on_by: [test/clustertest/tests]
misfit_count: {high: 0, med: 0, low: 3}
resolved_locally: 2
escalate:
  - item: fileAppend / fileLockAndLoad
    from: test/clustertest/testplugin/TestPlugin.cpp
    why: generic flock-guarded file I/O helpers with no plugin-specific logic, used by exactly one test scenario (testescalate) and confirmed by grep to appear nowhere else in the repo; libstuff is a production library now being decomposed, not a fit for a new test-only addition
    suggested_home: test/lib
-->
