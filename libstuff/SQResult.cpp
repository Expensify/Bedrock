#include <libstuff/libstuff.h>
#include "SQResult.h"
#include "libstuff/SQResultFormatter.h"
#include <libstuff/sqlite3.h>
#include <stdexcept>
#include <cmath>

SQResultRow::ColVal& SQResultRow::ColVal::operator=(const string& val) {
    type = TYPE::TEXT;
    text = val;
    return *this;
}

SQResultRow::ColVal& SQResultRow::ColVal::operator=(string&& val) {
    type = TYPE::TEXT;
    text = std::move(val);
    return *this;
}

SQResultRow::ColVal::ColVal(const char* val) : type(SQResultRow::ColVal::TYPE::TEXT), text(val ? val : "") {}

SQResultRow::ColVal& SQResultRow::ColVal::operator=(const char* val) {
    type = TYPE::TEXT;
    text = val ? val : "";
    return *this;
}

SQResultRow::SQResultRow(SQResult& result, size_t count) : result(&result) {
    data.resize(count);
}

SQResultRow::SQResultRow() : result(nullptr) {
}

SQResultRow::ColVal::ColVal() : type(SQResultRow::ColVal::TYPE::NONE) {
}

SQResultRow::ColVal::ColVal(int64_t val) : type(SQResultRow::ColVal::TYPE::INTEGER), integer(val) {
}

SQResultRow::ColVal::ColVal(double val) : type(SQResultRow::ColVal::TYPE::REAL), real(val) {
}

SQResultRow::ColVal::ColVal(const string& val) : type(SQResultRow::ColVal::TYPE::TEXT), text(val) {
}

SQResultRow::ColVal::ColVal(TYPE t, const string& val) : type(t), text(val) {
}

string operator+(string lhs, const SQResultRow::ColVal& rhs) {
    lhs += static_cast<string>(rhs);
    return lhs;
}

string operator+(const SQResultRow::ColVal& lhs, string rhs) {
    return static_cast<string>(lhs) + rhs;
}

string operator+(const char* lhs, const SQResultRow::ColVal& rhs) {
    return string(lhs) + static_cast<string>(rhs);
}

string operator+(const SQResultRow::ColVal& lhs, const char* rhs) {
    return static_cast<string>(lhs) + string(rhs);
}

string operator+(const SQResultRow::ColVal& lhs, const SQResultRow::ColVal& rhs) {
    return static_cast<string>(lhs) + static_cast<string>(rhs);
}

bool operator==(const SQResultRow::ColVal& a, const string& b) {
    return static_cast<string>(a) == b;
}

bool operator==(const string& a, const SQResultRow::ColVal& b) {
    return a == static_cast<string>(b);
}

bool operator==(const SQResultRow::ColVal& a, const char* b) {
    return static_cast<string>(a) == string(b ? b : "");
}

bool operator==(const char* a, const SQResultRow::ColVal& b) {
    return string(a ? a : "") == static_cast<string>(b);
}

bool operator==(const SQResultRow::ColVal& a, const SQResultRow::ColVal& b) {
    // Types must match
    if (a.type != b.type) {
        return false;
    }

    switch (a.type) {
        case SQResultRow::ColVal::TYPE::NONE:
            // Consider two NONEs equal
            return true;

        case SQResultRow::ColVal::TYPE::INTEGER:
            return a.integer == b.integer;

        case SQResultRow::ColVal::TYPE::REAL: {
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

        case SQResultRow::ColVal::TYPE::TEXT:
        case SQResultRow::ColVal::TYPE::BLOB:
            return a.text == b.text;
    }

    return false;
}

bool SQResultRow::ColVal::empty() const {
    if (type == TYPE::NONE) {
        return true;
    }
    if ((type == TYPE::TEXT || type == TYPE::BLOB) && text.empty()) {
        return true;
    }
    return false;
}

size_t SQResultRow::ColVal::size() const {
    return static_cast<string>(*this).size();
}

ostream& operator<<(ostream& os, const SQResultRow::ColVal& v) {
    // Reuse the string conversion which already formats REAL reasonably.
    os << static_cast<string>(v);
    return os;
}

void SQResultRow::push_back(const string& s) {
    data.push_back(ColVal(ColVal::TYPE::TEXT, s));
}

vector<SQResultRow::ColVal>::iterator SQResultRow::end() {
    return data.end();
}

vector<SQResultRow::ColVal>::const_iterator SQResultRow::end() const {
    return data.end();
}

vector<SQResultRow::ColVal>::const_iterator SQResultRow::begin() const {
    return data.begin();
}

bool SQResultRow::empty() const {
    return data.empty();
}

size_t SQResultRow::size() const {
    return data.size();
}

string SQResultRow::at(size_t index) {
    return data.at(index);
}

SQResultRow::ColVal& SQResultRow::get(size_t index) {
    return data.at(index);
}

const string SQResultRow::at(size_t index) const {
    return data.at(index);
}

SQResultRow::ColVal::operator string() const {
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

string SQResultRow::operator[](const size_t& rowNum) {
    try {
        return data.at(rowNum);
    } catch (const out_of_range& e) {
        SINFO("SQResultRow::operator[] out of range", {{"rowNum", to_string(rowNum)}});
        STHROW_STACK("Out of range");
    }
}
const string SQResultRow::operator[](const size_t& rowNum) const {
    try {
        return data.at(rowNum);
    } catch (const out_of_range& e) {
        SINFO("SQResultRow::operator[] out of range", {{"rowNum", to_string(rowNum)}});
        STHROW_STACK("Out of range");
    }
}

SQResultRow& SQResultRow::operator=(const SQResultRow& other) {
    data = other.data;
    result = other.result;
    return *this;
}

string SQResultRow::operator[](const string& key) {
    if (result) {
        for (size_t i = 0; i < result->headers.size(); i++) {

            // If the headers have more entries than the row (they really shouldn't), break early instead of segfaulting.
            if (i >= data.size()) {
                break;
            }

            if (result->headers[i] == key) {
                return (*this)[i];
            }
        }
    }
    STHROW_STACK("No column named " + key);
}

const string SQResultRow::operator[](const string& key) const {
    if (result) {
        for (size_t i = 0; i < result->headers.size(); i++) {

            // If the headers have more entries than the row (they really shouldn't), break early instead of segfaulting.
            if (i >= data.size()) {
                break;
            }

            if (result->headers[i] == key) {
                return (*this)[i];
            }
        }
    }
    STHROW_STACK("No column named " + key);
}

SQResultRow::operator vector<string>() const {
    vector<string> out(data.size());
    for (size_t i = 0; i < data.size(); i++) {
        out[i] = data[i];
    }
    return out;
}

string SQResult::serializeToJSON() const {
    return SQResultFormatter::format(*this, SQResultFormatter::FORMAT::JSON);
}

string SQResult::serializeToText() const {
    return SQResultFormatter::format(*this, SQResultFormatter::FORMAT::COLUMN);
}

string SQResult::serialize(const string& format) const {
    // Output the appropriate type
    if (SIEquals(format, "json"))
        return serializeToJSON();
    else
        return serializeToText();
}

bool SQResult::deserialize(const string& json) {
    // Reset ourselves to start
    clear();

    // If there are any problems, clean up whatever we've parsed
    try {
        // Verify we have the basic components
        STable content = SParseJSONObject(json);
        if (!SContains(content, "headers")) {
            STHROW("Missing 'headers'");
        }
        if (!SContains(content, "rows")) {
            STHROW("Missing 'rows'");
        }

        // Add the headers
        list<string> jsonHeaders = SParseJSONArray(content["headers"]);
        headers.insert(headers.end(), jsonHeaders.begin(), jsonHeaders.end());

        // Add the rows
        list<string> jsonRows = SParseJSONArray(content["rows"]);
        rows.resize(jsonRows.size());
        int rowIndex = 0;
        for (string& jsonRowStr : jsonRows) {
            // Get the row and make sure it has the right number of columns
            list<string> jsonRow = SParseJSONArray(jsonRowStr);
            if (jsonRow.size() != headers.size()) {
                STHROW("Incorrect number of columns in row");
            }

            // Insert the values
            SQResultRow& row = rows[rowIndex++];
            row.insert(row.end(), jsonRow.begin(), jsonRow.end());
        }

        // Success!
        return true;
    } catch (const SException& e) {
        SDEBUG("Failed to deserialize JSON-encoded SQResult (" << e.what() << "): " << json);
    }

    // Failed, reset and report failure
    clear();
    return false;
}

bool SQResult::empty() const {
    return rows.empty();
}

size_t SQResult::size() const {
    return rows.size();
}

void SQResult::clear() {
    headers.clear();
    rows.clear();
}

const SQResultRow& SQResult::operator[](size_t rowNum) const {
    try {
        return rows.at(rowNum);
    } catch (const out_of_range& e) {
        SINFO("SQResult::operator[] out of range", {{"rowNum", to_string(rowNum)}});
        STHROW_STACK("Out of range");
    }
}

SQResult& SQResult::operator=(const SQResult& other) {
    headers = other.headers;
    rows = other.rows;
    for (auto& row : rows) {
        row.result = this;
    }
    return *this;
}

vector<SQResultRow>::const_iterator SQResult::begin() const {
    return rows.begin();
}

vector<SQResultRow>::const_iterator SQResult::end() const {
    return rows.end();
}

vector<SQResultRow>::const_iterator SQResult::cbegin() const {
    return rows.cbegin();
}

vector<SQResultRow>::const_iterator SQResult::cend() const {
    return rows.cend();
}

void SQResult::emplace_back(SQResultRow&& row) {
    rows.emplace_back(move(row));
}
