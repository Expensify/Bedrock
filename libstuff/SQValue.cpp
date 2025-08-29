#include <libstuff/SQValue.h>
#include <libstuff/sqlite3.h>

SQValue& SQValue::operator=(const string& val) {
    type = TYPE::TEXT;
    text = val;
    return *this;
}

SQValue& SQValue::operator=(string&& val) {
    type = TYPE::TEXT;
    text = std::move(val);
    return *this;
}

SQValue::SQValue(const char* val) : type(SQValue::TYPE::TEXT), text(val ? val : "") {}

SQValue& SQValue::operator=(const char* val) {
    type = TYPE::TEXT;
    text = val ? val : "";
    return *this;
}

SQValue::SQValue() : type(SQValue::TYPE::NONE) {
}

SQValue::SQValue(int64_t val) : type(SQValue::TYPE::INTEGER), integer(val) {
}

SQValue::SQValue(double val) : type(SQValue::TYPE::REAL), real(val) {
}

SQValue::SQValue(const string& val) : type(SQValue::TYPE::TEXT), text(val) {
}

SQValue::SQValue(TYPE t, const string& val) : type(t), text(val) {
}

string operator+(string lhs, const SQValue& rhs) {
    lhs += static_cast<string>(rhs);
    return lhs;
}

string operator+(const SQValue& lhs, string rhs) {
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

        case SQValue::TYPE::REAL: {
            // Reasonable floating-point equality: combined abs/rel tolerance
            double da = a.real;
            double db = b.real;
            double diff = da - db;
            if (diff < 0) diff = -diff;

            const double absTol = 1e-12;
            const double relTol = 1e-9;

            double aad = da; if (aad < 0) aad = -aad;
            double abd = db; if (abd < 0) abd = -abd;
            double scale = (aad > abd) ? aad : abd;

            return diff <= absTol + relTol * scale;
        }

        case SQValue::TYPE::TEXT:
        case SQValue::TYPE::BLOB:
            return a.text == b.text;
    }

    return false;
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

SQValue::operator string() const {
    switch (type) {
        case TYPE::TEXT:
        case TYPE::BLOB:
            return text;
        case TYPE::INTEGER:
            return std::to_string(integer);
        case TYPE::REAL:
            char buf[64];
            sqlite3_snprintf(sizeof(buf), buf, "%!.15g", real);
            return string(buf);
        case TYPE::NONE:
        default:
            return "";
    }
}

ostream& operator<<(ostream& os, const SQValue& v) {
    // Reuse the string conversion which already formats REAL reasonably.
    os << static_cast<string>(v);
    return os;
}