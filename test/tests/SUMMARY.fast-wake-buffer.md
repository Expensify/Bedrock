# Cluster: fast-wake-buffer (test/tests)

## What these units have in common

Weakly, not strongly. All three are libstuff-level unit tests -- they exercise
low-level networking/buffering primitives (`libstuff/*`) rather than
BedrockCommand or plugin behavior, and all depend on `test/lib/BedrockTester.h`
for fixture support. But the specific mechanism differs per unit, and the
cluster label itself ("fast-wake-buffer") looks like it was assembled from
scattered token hits rather than a single shared concept:

- **AsyncResolveTest** and **SRingBufferTest** both genuinely test a
  wake/poll handshake: async state transitions (RESOLVING/CONNECTING ->
  settled; producer/consumer shutdown) driven by an explicit poll loop. This
  is the real "wake" thread running through the cluster.
- **FastHTTPParsing** contributes the "fast"/"buffer" tokens (it tests
  `SFastBuffer`) but has no wake/poll/async dimension at all -- it's a
  synchronous predicate test (`startsWithHTTPRequest()`) over buffer content.

So: two of three units (AsyncResolveTest, SRingBufferTest) share a real theme
(async wake/poll semantics on a producer/consumer or connection state
machine). FastHTTPParsing shares only superficial name-token overlap
(fast/buffer) and does not fit the wake theme -- it fits the *directory*
(test/tests, libstuff coverage) fine, just not this cluster's actual
throughline.

## Units

| Unit | Lines | Tests | Fits cluster theme? |
|---|---|---|---|
| `AsyncResolveTest.cpp` | 206 | Deferred DNS resolution in `STCPManager::Socket`/`SResolver`: sync fast path, deferred path, failure/wake behavior, resolution lifetime independent of caller (tpunit fixture `AsyncResolve`, 8 tests). | Yes -- core wake/poll theme. |
| `FastHTTPParsing.cpp` | 107 | `SFastBuffer::startsWithHTTPRequest()` across line endings, split buffers, and reset after an SData deserialize/consumeFront cycle. | No -- synchronous, no wake/poll dimension; token overlap only. |
| `SRingBufferTest.cpp` | 339 | `SRingBuffer` template: push/pop transitions, capacity limits, FIFO order, concurrent producers, producer/consumer shutdown-and-wake handshake (12 tests). | Yes -- core wake/poll theme. |

## Misfits

- **Cluster-fit misfit (this cluster, not the directory):** `FastHTTPParsing`
  does not share the wake/poll theme the other two units actually have in
  common. It belongs in `test/tests` (location_fit 5/5 per its own unit
  record) -- it just isn't thematically kin to `AsyncResolveTest` /
  `SRingBufferTest` beyond superficial name-token overlap. Resolvable locally:
  no action needed beyond noting the cluster label overstates the grouping.
- **Unit-level misfit (carried up from AsyncResolveTest's own record):** the
  fixture struct and global instance are named `AsyncResolve`/`__AsyncResolve`,
  inconsistent with the file name `AsyncResolveTest.cpp` and with the
  `Foo`/`FooTest`/`__FooTest` naming convention every sibling test in this
  batch follows (`FastHTTPParsing`, `SRingBufferTest`). Severity low.
  Resolvable locally -- rename the struct/instance inside
  `test/tests/AsyncResolveTest.cpp` to match convention.

No misfit in this cluster needs escalation outside `test/tests`; both items
above are fixable in place.

## Note on input sufficiency

This is a 3-unit cluster with per-unit `misfits`, `name_fit`, `location_fit`,
and `naming_quality` scores already computed -- sufficient for the judgements
above. No gaps.

<!-- ROLLUP
theme: libstuff-level unit tests for socket/buffer primitives — genuinely wake/poll-themed for two of three units (async DNS resolution, ring-buffer producer/consumer handshake), with one unit (FastHTTPParsing) sharing only name-token overlap, not the wake/poll mechanism
exports: [async DNS resolution test coverage (SResolver/STCPManager), HTTP-request-boundary detection test coverage (SFastBuffer), SRingBuffer concurrency/shutdown test coverage, wake/poll-driven test-loop pattern (pollUntilSettled) reusable by other async fixtures]
depends_on_dirs: [libstuff, test/lib]
depended_on_by: []
misfit_count: {high: 0, med: 0, low: 1}
resolved_locally: 1
escalate: []
-->
