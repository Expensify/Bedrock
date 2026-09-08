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
   location (e.g. `libstuff`), which is outside this directory's remit to
   decide.

<!-- ROLLUP
theme: Test-only BedrockPlugin (TestPlugin) that exposes internal server code paths as commands so the clustertest binary can exercise crashes, timeouts, escalation, and commit hooks.
exports: [BedrockPlugin_TestPlugin, TestPluginCommand, TestHTTPSManager, generatesegfault* crash commands, testescalate ordering helpers]
depends_on_dirs: [libstuff, sqlitecluster, .]
depended_on_by: []
misfit_count: {high: 0, med: 0, low: 3}
resolved_locally: 2
escalate:
  - item: fileAppend / fileLockAndLoad
    from: test/clustertest/testplugin/TestPlugin.cpp
    why: generic flock-guarded file I/O helpers with no plugin-specific logic, only used by one test scenario
    suggested_home: libstuff
-->
