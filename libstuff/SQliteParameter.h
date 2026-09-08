/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SQliteParameter.h
 * Path:    libstuff/SQliteParameter.h
 * Pair:    SQliteParameter.cpp
 *
 * INTENT
 *   A single typed value bindable to a named sqlite3 prepared-statement parameter
 *   (`:name`/`@name`/`$name`), plus a wire-safe serialize/deserialize pair so bound
 *   parameters can travel as a plain string inside an SData header value.
 *
 * OBJECTS
 *   SQliteParameter - Type enum class (Null/Int64/Double/Text/Blob); public type/
 *     value fields; static factories null/i/d/text/blob; serialize/deserialize;
 *     uriEncodeParamName/uriDecodeParamName for embedding the placeholder name
 *     (which starts with a reserved punctuation character) in an SData header name.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; sits with the other SQLite-adjacent S-types (SQResult, SQValue) in libstuff.
 *
 * NAMING QUALITY
 *   Consistent. Public fields left undecorated (no `_`) per the public-data-member
 *   convention used across this class family (SQValue, SQResult).
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once
#include <cstdint>
#include <string>

using namespace std;

// A single value that can be bound to a sqlite3 prepared-statement parameter. SQuery and SQLite::write/
// read/etc. consume a `map<string, SQliteParameter>` whose keys are the named placeholders that appear
// in the SQL — `:name`, `@name`, or `$name`. Positional `?` and `?NNN` placeholders are not supported;
// the key must include the prefix character (e.g. `":id"`, not `"id"`) so it matches what `sqlite3_bind_
// parameter_index()` looks up.
//
// Construct via the static factory methods (`i`/`d`/`text`/`blob`/`null`); the type discriminator and
// value fields are public so SQuery can dispatch on Type without virtual calls.
class SQliteParameter {
public:
    enum class Type { Null, Int64, Double, Text, Blob };

    Type type = Type::Null;
    int64_t intValue = 0;
    double doubleValue = 0.0;
    string stringValue;

    static SQliteParameter null();
    static SQliteParameter i(int64_t v);
    static SQliteParameter d(double v);
    static SQliteParameter text(string v);
    static SQliteParameter blob(string v);

    // Serialize to a single string suitable for stuffing into an SData header value. Format is a single
    // type-tag byte followed by an encoded payload: 'N' (null, empty payload); 'I' (int64, decimal); 'D'
    // (double, %.17g for round-trip exactness); 'T' / 'B' (text / blob, base64 — needed because SData
    // values are line-oriented and the bytes may contain newlines or NULs).
    string serialize() const;

    // Inverse of serialize(). Returns Null on any format error rather than throwing, since the inputs come
    // from the wire and a malformed value should bind as NULL rather than crash the receiver.
    static SQliteParameter deserialize(const string& encoded);

    // Encode a parameter name (including its leading `:` / `@` / `$` prefix) so it can ride safely in an
    // SData header name. The placeholder prefix is a colon for the common case, and SParseHTTP splits
    // headers on the first `:` — so any param name placed verbatim into a header name would corrupt the
    // wire format. Encoding replaces `:`, `@`, `$`, and `#` with `#XX` where XX is the uppercase hex of
    // the byte. The companion decode reverses it.
    static string uriEncodeParamName(const string& name);
    static string uriDecodeParamName(const string& encoded);
};
