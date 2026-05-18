#pragma once
#include <cstdint>
#include <string>
#include <utility>

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

    static SQliteParameter null()
    {
        return {};
    }

    static SQliteParameter i(int64_t v)
    {
        SQliteParameter p;
        p.type = Type::Int64;
        p.intValue = v;
        return p;
    }

    static SQliteParameter d(double v)
    {
        SQliteParameter p;
        p.type = Type::Double;
        p.doubleValue = v;
        return p;
    }

    static SQliteParameter text(string v)
    {
        SQliteParameter p;
        p.type = Type::Text;
        p.stringValue = move(v);
        return p;
    }

    static SQliteParameter blob(string v)
    {
        SQliteParameter p;
        p.type = Type::Blob;
        p.stringValue = move(v);
        return p;
    }

    // Serialize to a single string suitable for stuffing into an SData header value. Format is a single
    // type-tag byte followed by an encoded payload: 'N' (null, empty payload); 'I' (int64, decimal); 'D'
    // (double, %.17g for round-trip exactness); 'T' / 'B' (text / blob, base64 — needed because SData
    // values are line-oriented and the bytes may contain newlines or NULs).
    string serialize() const;

    // Inverse of serialize(). Returns Null on any format error rather than throwing, since the inputs come
    // from the wire and a malformed value should bind as NULL rather than crash the receiver.
    static SQliteParameter deserialize(const string& encoded);
};
