# 12 — Bugs, vulnerabilities and maintenance hazards (living document)

**Priority:** P3 (Dan, 2026-09-07) — "any other bug or vulnerability you come across"
**Status:** open, appended during every unit

Each entry records: what it is, `file:line`, the failure scenario, severity, and which
build it affects (vendored Aug-28 drop / check-in `eedd80c1a9749300` / both / Bedrock
itself).

Severity scale: **high** = silent wrong behaviour or data risk; **medium** = lost
capability, performance cliff, or a trap that will bite on the next change; **low** =
cosmetic or defensive.

---

## #1 — `SQLITE_MUTEX_ALERT_MILLISECONDS=20` is a dead flag

**Severity:** medium
**Affects:** Bedrock build configuration
**Location:** `Makefile:18` (`AMALGAMATION_FLAGS`)

The flag is defined in Bedrock's build and referenced **nowhere else in existence**:

- Not in the upstream checkout: `grep -rn MUTEX_ALERT` over the whole
  `hctree-bedrock-lcd-ex @ eedd80c1a9749300` tree → **0 matches**.
- Not in the vendored amalgamation: `grep -c MUTEX_ALERT libstuff/sqlite3.c` → **0**.
- Not in Bedrock's own C++: the only hit anywhere in the repo is the `Makefile` line that
  defines it.

So the compiler defines a macro that no `#ifdef` ever tests. Whatever mutex-contention
alerting this once switched on is **not compiled into production**.

**Why it matters beyond tidiness:** this flag exists because someone previously cared
about mutex hold times — exactly the failure mode expected on a 384-CPU host (P4). The
diagnostic that would tell us which mutex is hot has been silently lost, probably during
an upstream re-vendor that dropped the patch implementing it. We are flying blind on the
single most likely scaling bottleneck.

**Action:** decide whether to (a) delete the flag as dead, or (b) recover the original
mutex-alert patch and re-apply it. Given P4, (b) is more valuable. **Question for Dan:**
was there ever a local mutex-alert patch to `sqlite3.c`, and does anyone have it? It is
not in the current amalgamation, and `00-provenance.md` establishes the amalgamation
carries no local patches at all — so if it existed, it is already gone.

---

## #2 — Amalgamation regeneration procedure is unrecorded, and one option is load-bearing

**Severity:** medium
**Affects:** Bedrock maintenance
**Location:** `Makefile:18`; no documented regeneration step anywhere in the repo

`SQLITE_ENABLE_UPDATE_DELETE_LIMIT` must be passed at **amalgamation-generation** time
(`./configure --enable-update-limit`), because Lemon bakes the grammar into `parse.c`.
Bedrock lists it among `AMALGAMATION_FLAGS`, which are otherwise `-D` compile flags — so
the name suggests it is sufficient to pass it to the compiler. It is not.

**Failure scenario:** an engineer re-vendors SQLite from a fresh checkout, runs
`./configure && make sqlite3.c` (the obvious commands), and commits the result. The build
succeeds with no warning. `-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT` is still on the compile
line and still does nothing useful, because the generated parser no longer has the rules.
Every `UPDATE … LIMIT` / `DELETE … LIMIT` statement in Bedrock and its plugins begins
failing as a **syntax error** at runtime — not at build time.

Detected here because the first generation attempt produced 13 spurious `parse.c` hunks
and a dropped `#define SQLITE_UDL_CAPABLE_PARSER 1`.

**Action:** commit the exact regeneration recipe (see `00-provenance.md`) into the repo
next to `libstuff/sqlite3.c`, and/or add a build-time assertion that
`SQLITE_UDL_CAPABLE_PARSER` is defined whenever `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` is.
The latter turns a silent runtime regression into a compile error and costs three lines.

---

## #3 — `-cacheSize` help text contradicts its actual default; real default is 50 MiB on a 6 TB-RAM host

**Severity:** medium (performance + misleading documentation)
**Affects:** Bedrock, all engines
**Location:** `main.cpp:250`, `main.cpp:329`, `sqlitecluster/SQLite.cpp:295-297`,
`Makefile:18`, `configs/bedrock.conf:12`

The documented and actual defaults disagree, and the actual one is very small for the
hardware.

- `main.cpp:250` — help text: `-cacheSize <kb>  number of KB to allocate for a page cache
  (defaults to 1GB)`
- `main.cpp:329` — `SETDEFAULT("-cacheSize", SToStr(0));` — the real default is **0**
- `sqlitecluster/SQLite.cpp:295` — `if (_cacheSize) {` … so **0 means the `PRAGMA
  cache_size` is never issued at all**
- therefore the effective default is the compile-time
  `SQLITE_DEFAULT_CACHE_SIZE=-51200` (`Makefile:18`), i.e. **50 MiB** per connection

So the help promises 1 GB, the code delivers 50 MiB, and nothing reconciles them. The
shipped sample config makes it smaller still: `configs/bedrock.conf:12` sets
`CACHE_SIZE="-cacheSize 10000"` → **10 MiB**.

**Why it matters:** on a 384-CPU / 6 TB-RAM host this is the difference between a page
cache that holds the working set and one that thrashes. It is also multiplied by the
connection-pool size (`_dbPoolSize`, `BedrockServer.cpp:115`), so the right number is not
obvious — but 10–50 MiB per connection is almost certainly far below optimum here.

**Not yet verified:** the production config is not in this repo, so production may pass an
explicit `-cacheSize`. **Question for Dan:** what does production actually set? If it
inherits the default, this is likely the cheapest available win (P4).

**Action regardless:** fix the help text, or better, make the default match it.

---

*(further entries appended as units proceed)*
