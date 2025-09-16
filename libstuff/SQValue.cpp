#include <cmath>
#include <libstuff/SQValue.h>
#include <libstuff/sqlite3.h>

// All of our connstructor bodies are emtpy, we just use member initializer lists.
SQValue::SQValue() : type(SQValue::TYPE::NONE) {}
SQValue::SQValue(int64_t val) : type(SQValue::TYPE::INTEGER), integer(val) {}
SQValue::SQValue(double val) : type(SQValue::TYPE::REAL), real(val) {}
SQValue::SQValue(const char* val) : type(SQValue::TYPE::TEXT), text(val ? val : "") {}
SQValue::SQValue(const string& val) : type(SQValue::TYPE::TEXT), text(val) {}
SQValue::SQValue(TYPE t, const string& val) : type(t), text(val) {}

SQValue::operator string() const {
    switch (type) {
        case TYPE::TEXT:
        case TYPE::BLOB:
            return text;
        case TYPE::INTEGER:
            return std::to_string(integer);
        case TYPE::REAL:
            // This is the same format that the sqlite3 shell uses internally.
            char buf[64];
            sqlite3_snprintf(sizeof(buf), buf, "%!.15g", real);
            return string(buf);
        case TYPE::NONE:
        default:
            return "";
    }
}

string operator+(string lhs, const SQValue& rhs) {
    lhs += static_cast<string>(rhs);
    return lhs;
}

string operator+(const SQValue& lhs, const string& rhs) {
    return static_cast<string>(lhs) + rhs;
}

string operator+(const char* lhs, const SQValue& rhs) {
    return string(lhs) + static_cast<string>(rhs);
}

string operator+(const SQValue& lhs, const char* rhs) {
    return static_cast<string>(lhs) + string(rhs);
}

string operator+(const SQValue& lhs, const SQValue& rhs) {
    return static_cast<string>(lhs) + static_cast<string>(rhs);
}

bool operator==(const SQValue& lhs, const string& rhs) {
    return static_cast<string>(lhs) == rhs;
}

bool operator==(const string& lhs, const SQValue& rhs) {
    return lhs == static_cast<string>(rhs);
}

bool operator==(const SQValue& lhs, const char* rhs) {
    return static_cast<string>(lhs) == string(rhs ? rhs : "");
}

bool operator==(const char* lhs, const SQValue& rhs) {
    return string(lhs ? lhs : "") == static_cast<string>(rhs);
}

bool operator==(const SQValue& lhs, const SQValue& rhs) {
    // Types must match
    if (lhs.type != rhs.type) {
        return false;
    }

    switch (lhs.type) {
        case SQValue::TYPE::NONE:
            // Consider two NONEs equal
            return true;

        case SQValue::TYPE::INTEGER:
            return lhs.integer == rhs.integer;

        case SQValue::TYPE::REAL:
        {
            // Comparing floating-point values with strict equality is brittle because of tiny rounding differences,
            // so we treat numbers as equal when they are "close enough".
            // There's an annoyingly long detailed article here on why this is:
            // https://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/
            //
            // Algorithm used here: |a - b| <= absTolerance + relTolerance * max(|a|, |b|)
            //
            // absTolerance = 1e-12 (0.000000000001): ignores tiny differences when values are close to zero.
            // relTolerance = 1e-9 (one-in-a-billion): roughly requires 9 matching decimal digits. Because very large floating point values
            // have the same amount of precision, very large values with the same amount of precision loss will scale to more that what
            // we've set `absTolerance` to, and this accounts for that as well.
            const double absTolerance = 1e-12;
            const double relTolerance = 1e-9;
            return fabs(lhs.real - rhs.real) <= absTolerance + relTolerance * max(fabs(lhs.real), fabs(rhs.real));
        }

        case SQValue::TYPE::TEXT:
        case SQValue::TYPE::BLOB:
            return lhs.text == rhs.text;
    }
}

bool operator!=(const SQValue& lhs, const SQValue& rhs) {
    return !(lhs == rhs);
}

bool SQValue::empty() const {
    if (type == TYPE::NONE) {
        return true;
    }
    if ((type == TYPE::TEXT || type == TYPE::BLOB) && text.empty()) {
        return true;
    }
    return false;
}

size_t SQValue::size() const {
    return static_cast<string>(*this).size();
}

ostream& operator<<(ostream& os, const SQValue& v) {
    os << static_cast<string>(v);
    return os;
}
