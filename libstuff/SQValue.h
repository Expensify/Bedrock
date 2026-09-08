/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SQValue.h
 * Path:    libstuff/SQValue.h
 * Pair:    SQValue.cpp
 *
 * INTENT
 *   A tagged-value type mirroring SQLite's dynamic column typing (NONE,
 *   INTEGER, REAL, TEXT, BLOB), with implicit conversion to/from `string`
 *   so it can be used as a drop-in replacement wherever legacy code expects
 *   a string-typed value.
 *
 * OBJECTS
 *   SQValue (class) - TYPE enum class; constructors from int64_t, double,
 *     const char-pointer, and string (with explicit TEXT/BLOB tag); operator string();
 *     friend operator+/== / != (string, char*, and SQValue combinations);
 *     empty()/size(); friend operator<<.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; the `Q` naming matches other SQLite-adjacent types (SQResult, SQuery).
 *
 * NAMING QUALITY
 *   Consistent. Private members (integer/real/text) all coexist regardless
 *   of `type` rather than a union, but this is a deliberate simplicity choice.
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once
#include <string>
using namespace std;

class SQValue {
public:

    // Each value is typed to one of SQLite's types.
    enum class TYPE
    {
        NONE, // because NULL is overloaded.
        INTEGER,
        REAL,
        TEXT,
        BLOB,
    };

    // Construct from NULL or any of the supported SQLite types.
    // TEXT and BLOB are treated internally the same, so if you construct from a plain
    // string object, you get TEXT. If you want BLOB, you need to pass the type BLOB.
    SQValue();
    SQValue(int64_t val);
    SQValue(double val);
    SQValue(const char* val);
    explicit SQValue(const string& val);
    explicit SQValue(TYPE t, const string& val);

    // We have a *whole bunch* of string utility functions for conferting typed data
    // back to strings. All existing code expects strings and so we allow this to work as a string everywhere.

    // Cast to string (essentially, serializes to existing legacy format)
    operator string() const;

    // Support concatenation with strings.
    friend string operator+(string lhs, const SQValue& rhs);
    friend string operator+(const SQValue& lhs, const string& rhs);
    friend string operator+(const char* lhs, const SQValue& rhs);
    friend string operator+(const SQValue& lhs, const char* rhs);
    friend string operator+(const SQValue& lhs, const SQValue& rhs);

    // Support comparison with strings.
    friend bool operator==(const SQValue& lhs, const string& rhs);
    friend bool operator==(const string& lhs, const SQValue& rhs);
    friend bool operator==(const SQValue& lhs, const char* rhs);
    friend bool operator==(const char* lhs, const SQValue& rhs);

    // Support comparison with another SQValue.
    friend bool operator==(const SQValue& lhs, const SQValue& rhs);
    friend bool operator!=(const SQValue& lhs, const SQValue& rhs);

    // Calling either of these acts like the aame function call on `string`.
    bool empty() const;
    size_t size() const;

    // Allow serialization as as string.
    friend ostream& operator<<(ostream& os, const SQValue& v);

private:

    // Type of data currently stored. There's no mechanism to change this once created aside from the assignment operator.
    TYPE type;

    // One of these should be set (or none, if type is NONE).
    // Both TEXT and BLOB use `text`.
    int64_t integer{0};
    double real{0.0};
    string text;
};
