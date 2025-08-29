
#pragma once

#include <string>
#include <vector>
using namespace std;

class SQValue {
public:
    enum class TYPE {
        NONE, // because NULL is overloaded.
        INTEGER,
        REAL,
        TEXT,
        BLOB,
    };

    operator string() const;

    // Constructors;
    SQValue();
    SQValue(int64_t val);
    SQValue(double val);
    SQValue(const char* val);
    explicit SQValue(const string& val);
    explicit SQValue(TYPE t, const string& val);

    SQValue& operator=(const string& val);
    SQValue& operator=(string&& val);
    SQValue& operator=(const char* val);

    // Allow string concatenation.
    friend string operator+(string lhs, const SQValue& rhs);
    friend string operator+(const SQValue& lhs, string rhs);
    friend string operator+(const char* lhs, const SQValue& rhs);
    friend string operator+(const SQValue& lhs, const char* rhs);
    friend string operator+(const SQValue& lhs, const SQValue& rhs);

    // And string comparison
    friend bool operator==(const SQValue& a, const string& b);
    friend bool operator==(const string& a, const SQValue& b);
    friend bool operator==(const SQValue& a, const char* b);
    friend bool operator==(const char* a, const SQValue& b);

    friend bool operator==(const SQValue& a, const SQValue& b);
    friend bool operator!=(const SQValue& a, const SQValue& b) { return !(a == b); }

    bool empty() const;
    size_t size() const;

    friend ostream& operator<<(ostream& os, const SQValue& v);

private:
    TYPE type;

    // Treat these as a union.
    int64_t integer{0};
    double real{0.0};
    string text;
    // Not used, we simply store this in `text` as they're stored the same way.
    // This is left here to make it clear it's not accidentally omitted.
    // string blob;
};