
#pragma once
#include <string>
using namespace std;

class SQValue {
public:

    // Each value is typed to one of SQLite's types.
    enum class TYPE {
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
    friend bool operator==(const SQValue& a, const string& b);
    friend bool operator==(const string& a, const SQValue& b);
    friend bool operator==(const SQValue& a, const char* b);
    friend bool operator==(const char* a, const SQValue& b);

    // Support comparison with another SQValue.
    friend bool operator==(const SQValue& a, const SQValue& b);
    friend bool operator!=(const SQValue& a, const SQValue& b);

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
