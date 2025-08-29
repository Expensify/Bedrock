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

bool operator==(const SQValue& a, const string& b) {
    return static_cast<string>(a) == b;
}

bool operator==(const string& a, const SQValue& b) {
    return a == static_cast<string>(b);
}

bool operator==(const SQValue& a, const char* b) {
    return static_cast<string>(a) == string(b ? b : "");
}

bool operator==(const char* a, const SQValue& b) {
    return string(a ? a : "") == static_cast<string>(b);
}

bool operator==(const SQValue& a, const SQValue& b) {
    // Types must match
    if (a.type != b.type) {
        return false;
    }

    switch (a.type) {
        case SQValue::TYPE::NONE:
            // Consider two NONEs equal
            return true;

        case SQValue::TYPE::INTEGER:
            return a.integer == b.integer;

        case SQValue::TYPE::REAL:
        {
            // Floating point equality is fuzzy.
            double aVal = a.real;
            double bVal = b.real;
            double difference = aVal - bVal;
            if (difference < 0) {
                difference = -difference;
            }

            const double absTol = 1e-12;
            const double relTol = 1e-9;

            double absA = aVal;
            if (absA < 0) {
                absA = -absA;
            }
            double absB = bVal;
            if (absB < 0) {
                absB = -absB;
            }
            double scaleFactor = (absA > absB) ? absA : absB;

            return difference <= absTol + relTol * scaleFactor;
        }

        case SQValue::TYPE::TEXT:
        case SQValue::TYPE::BLOB:
            return a.text == b.text;
    }

    return false;
}

bool operator!=(const SQValue& a, const SQValue& b) {
    return !(a == b);
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
