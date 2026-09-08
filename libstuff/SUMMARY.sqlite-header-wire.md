# Cluster: sqlite-header-wire (libstuff)

## 1. What these units have in common

Six of the eight units are genuinely one theme: **typed values and result sets built
around SQLite, plus the wire formats that carry them**. `SQValue` mirrors SQLite's
dynamic column typing; `SQResult`/`SQResultFormatter` hold and render a query's
output; `SQliteParameter` is a bound SQL parameter with its own serialize/deserialize
so it can travel *inside* an `SData` header value — `SData` being the generic
HTTP-like message (method line + header table + body) that is both real HTTP and
Bedrock's internal command wire format. `SDeburr` is a looser fit: it's a text
utility, but its reason for living in this cluster is that its primary consumer is
a SQLite UDF (`DEBURR()`), registered via `sqlite3_create_function_v2`.

`STime` (free functions for timestamps) has no real thematic tie to "sqlite" or
"header/wire" — it appears to be swept in because its API is declared inside
`libstuff.h` rather than a header of its own, and `libstuff.cpp` separately
implements SQL timestamp macros (`SCURRENT_TIMESTAMP`, `STIMESTAMP`, etc.). That's
a thin, incidental link, not a shared purpose.

`libstuff` itself (4612 lines, the repo's junk drawer) is here because it contains
*buried duplicates* of both cluster themes: a full SQLite execution engine
(`SQuery`, `SQVerifyTable*`, `SQ`/`SQList`) alongside the dedicated `SQResult`/
`SQValue`/`SQliteParameter` types it should probably be using, and a complete HTTP
wire-format grammar (`SParseHTTP`, `SComposeHTTP`, `SParseURI`, cookie/chunked
handling) alongside `SData`. That overlap is real and is the correct reason for
`libstuff` landing in this cluster — but the bulk of `libstuff`'s content (crypto,
sockets, files, JSON, regex, signals, logging) has nothing to do with
sqlite-header-wire at all; it's here only because the file wasn't cut up along
those lines yet.

## 2. One line per unit

| Unit | Files | Lines | Role |
|---|---|---|---|
| `SData` | SData.h/.cpp | 283 | Generic HTTP-like message (method line, header table, body); doubles as Bedrock's internal command wire format |
| `SDeburr` | SDeburr.h/.cpp | 277 | Unicode→ASCII text normalization, exposed as the `DEBURR()` SQLite UDF |
| `SQResult` | SQResult.h/.cpp | 389 | In-memory SQLite result set: typed rows/columns, by-name access, JSON (de)serialize |
| `SQResultFormatter` | SQResultFormatter.h/.cpp | 588 | Renders an `SQResult` into six sqlite3-shell text formats (column/csv/tabs/json/quote/list) |
| `SQValue` | SQValue.h/.cpp | 230 | Tagged value type mirroring SQLite's dynamic column typing, implicitly string-convertible |
| `SQliteParameter` | SQliteParameter.h/.cpp | 174 | Single typed, named, bindable SQL parameter with wire-safe serialize/deserialize (rides inside `SData` headers) |
| `STime` | STime.cpp (no own header) | 138 | Microsecond-timestamp free functions: now/format/parse/calendar helpers |
| `libstuff` | libstuff.h/.cpp | 4612 | Project-wide catch-all: core types (`SString`,`STable`,`SException`,`SStopwatch`,`SRECompiledRegex`), logging/assert macros, and free functions for strings, HTTP, JSON, sockets, files, crypto, and SQL execution |

## 3. Misfits

### Cluster-composition (does this unit belong in *this cluster*)

- **`STime`** — no real connection to sqlite or wire formats; its only tie to the
  cluster is that it's declared in `libstuff.h` rather than its own header. It
  reads as a clustering artifact, not a thematic member. It still fits the
  `libstuff` *directory* fine — just not this cluster.
- **`SDeburr`** — a stretch fit: it's a Unicode/string utility whose only sqlite
  connection is being registered as a UDF. Belongs in the directory; borderline for
  the cluster label.
- **`libstuff`** — correctly included for the slice of it that duplicates this
  cluster's themes (SQL execution, HTTP wire parsing), but the great majority of
  its 4612 lines (crypto, sockets, files, JSON, regex, signals, logging) is
  unrelated to sqlite-header-wire. Its presence in this cluster should not be read
  as "libstuff is a sqlite/wire file" — it's a junk drawer that happens to also
  contain sqlite/wire code.

### Code-level findings carried from unit data

These come from the per-unit analysis in the input and are worth the parent's
attention even though they're not about cluster-fit:

- **High** — `libstuff.cpp`'s `SQuery`/`SQVerifyTable*`/`SQ`/`SQList` form a full
  SQLite execution engine (busy-retry loop, named-parameter binding, corruption
  detection, slow-query logging) living in the generic utility file, while
  `SQResult`, `SQValue`, and `SQliteParameter` already exist as dedicated units in
  this same cluster/directory. This is the cluster's clearest "the split already
  started, finish it" case.
- **Med** — `libstuff.cpp`'s `SParseHTTP`/`SComposeHTTP`/`SParseURI`/`SParseHost`
  family is a complete HTTP wire-format grammar sitting beside `SData`, which
  exists for exactly this. Suggested home (`SHTTPSManager`) is not in this
  cluster, so the actual owner can't be confirmed from here.
- **Med** — `libstuff.cpp`'s crypto wrappers (`SAESEncrypt`/`SHashSHA*`/
  `SEncodeBase64`/`SHMACSHA*`) are self-contained mbedtls wrappers with no
  dependency on anything else in the file — candidates for a new `SCrypto` unit,
  but that unit doesn't exist anywhere visible in this cluster.
- **Med** — `libstuff.cpp`'s direct-syslog-socket pool
  (`SLogSocketFD`/`SSyslogSocketDirect`/`SSyslogNoop`) is a full alternate logging
  transport sitting amid unrelated code, even though `SLog.cpp` already exists in
  the directory as the dedicated logging-support unit (not part of this cluster,
  so can't confirm this is a straightforward merge).
- **Med** — `SQResultFormatter`'s `formatColumn` hides a generic UTF-8
  display-width utility (`utf8Next`/`isCombining`/`isWide`/`displayWidth`) as local
  lambdas; it has no dependency on `SQResult` and belongs in `SString` — which
  *is* in this same directory (`libstuff.h`), so this one is resolvable locally.
- **Low** — `libstuff.cpp` socket/poll primitives (`S_socket`, `S_poll`, etc.)
  whose only real caller is `STCPManager`, not part of this cluster.
- **Low** — `SGZip`/`SGUnzip` in `libstuff.cpp`: self-contained gzip code,
  unrelated to the string/HTTP/SQL utilities around it, no obvious target.
- **Low** — `libstuff.cpp`'s leading-underscore helpers (`_SParseHTTP_GetUpToNext`,
  `_SParseJSONValue`, etc.) read as file-private by convention but are not
  `static` or in an anonymous namespace — real external-linkage symbols that could
  collide across translation units. A local fix, not a relocation.
- **Low** — `SData::deserialize` unconditionally over-allocates 32 bytes on any
  JSON-looking header value to satisfy simdjson's padding requirement — a generic
  message container shaping itself around one downstream parser.
- **Low** — `SData::create` is marked deprecated in favor of the constructor but
  still public.
- **Low** — `SQResultFormatter::FORMAT_OPTIONS::nullvalue`/`::separator` are
  declared but never read by any `formatXXX()` — dead configuration surface.
- **Low** — `STime`'s public API is declared in `libstuff.h`'s catch-all rather
  than a dedicated `STime.h`, unlike every other already-separated libstuff unit
  (`SData`, `SQResult`, etc.).

## Report note

This input is sufficient for the judgements above. The one limit worth flagging
upward: several suggested homes (`SHTTPSManager`, a hypothetical `SCrypto` unit,
`SLog.cpp`, `STCPManager`) are named by the unit data but are not part of this
cluster, so this pass cannot confirm whether they already absorb this
functionality or are equally overloaded — that check belongs to whoever has the
full-directory view.

<!-- ROLLUP
theme: SQLite-adjacent typed values/result sets and the HTTP-like wire format that carries them, plus the libstuff catch-all's buried duplicates of both
exports: [SData, SQResult, SQResultFormatter, SQValue, SQliteParameter, SDeburr (DEBURR UDF), STime, libstuff-core (SString/STable/SException/logging+assert macros)]
depends_on_dirs: [libstuff]
depended_on_by: []
misfit_count: {high: 1, med: 4, low: 7}
resolved_locally: 6
escalate:
  - item: SParseHTTP/SComposeHTTP/SParseURI/SParseHost family
    from: libstuff/libstuff.cpp
    why: full HTTP wire-format grammar duplicating SData's purpose, inside the generic utility file
    suggested_home: SHTTPSManager or a new SHTTPMessage unit (not in this cluster - unconfirmed)
  - item: SAESEncrypt/SAESDecrypt/SHashSHA1/SHashSHA256/SEncodeBase64/SDecodeBase64/SHMACSHA1/SHMACSHA256
    from: libstuff/libstuff.cpp
    why: self-contained crypto wrappers with no dependency on the rest of the file
    suggested_home: a new SCrypto unit (does not appear to exist yet)
  - item: direct-syslog-socket pool (SLogSocketFD/SSyslogSocketDirect/SSyslogNoop)
    from: libstuff/libstuff.cpp
    why: a complete alternate logging transport sitting amid unrelated HTTP/JSON/SQL/crypto code
    suggested_home: SLog.cpp (dedicated logging unit, not in this cluster)
  - item: S_socket/S_close/S_accept/S_recvfrom/S_recvappend/S_sendconsume/S_poll/SFDset/SFDAnySet
    from: libstuff/libstuff.cpp
    why: raw socket/poll primitives whose only real caller is STCPManager
    suggested_home: STCPManager (not in this cluster)
  - item: SGZip/SGUnzip
    from: libstuff/libstuff.cpp
    why: gzip compression unrelated to the surrounding string/HTTP/SQL utilities
    suggested_home: null
  - item: SData::deserialize simdjson padding logic
    from: libstuff/SData.cpp
    why: a generic message container shaping its allocation to one downstream JSON parser's requirement
    suggested_home: wherever the simdjson parsing of these values actually happens (not in this cluster)
-->
