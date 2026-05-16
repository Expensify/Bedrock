#pragma once
#include <cstdint>
#include <string>
#include <utility>

using namespace std;

// A single value that can be bound to a sqlite3 prepared-statement parameter (the `?` placeholders in SQL).
// Construct via the static factory methods; the type discriminator and value fields are public so SQuery
// can dispatch on Type without virtual calls.
//
// Lives in libstuff alongside SQuery because the bind-aware SQuery overloads consume it. A `using` alias
// inside the SQLite class exposes it as SQLite::Parameter for ergonomic access from callers.
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
        SQliteParameter p; p.type = Type::Int64;  p.intValue = v;            return p;
    }

    static SQliteParameter d(double v)
    {
        SQliteParameter p; p.type = Type::Double; p.doubleValue = v;         return p;
    }

    static SQliteParameter text(string v)
    {
        SQliteParameter p; p.type = Type::Text;   p.stringValue = move(v); return p;
    }

    static SQliteParameter blob(string v)
    {
        SQliteParameter p; p.type = Type::Blob;   p.stringValue = move(v); return p;
    }
};
