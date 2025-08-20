#include <libstuff/libstuff.h>
#include "SQResult.h"
#include <stdexcept>

SQResultRow::SQResultRow(SQResult& result, size_t count) : vector<string>(count), result(&result) {
}

SQResultRow::SQResultRow() : vector<string>(), result(nullptr) {
}

void SQResultRow::push_back(const string& s) {
    vector<string>::push_back(s);
}

string& SQResultRow::operator[](const size_t& rowNum) {
    try {
        return at(rowNum);
    } catch (const out_of_range& e) {
        SINFO("SQResultRow::operator[] out of range", {{"rowNum", to_string(rowNum)}});
        STHROW_STACK("Out of range");
    }
}
const string& SQResultRow::operator[](const size_t& rowNum) const {
    try {
        return at(rowNum);
    } catch (const out_of_range& e) {
        SINFO("SQResultRow::operator[] out of range", {{"rowNum", to_string(rowNum)}});
        STHROW_STACK("Out of range");
    }
}

SQResultRow& SQResultRow::operator=(const SQResultRow& other) {
    vector<string>::operator=(other);
    result = other.result;
    return *this;
}

string& SQResultRow::operator[](const string& key) {
    if (result) {
        for (size_t i = 0; i < result->headers.size(); i++) {

            // If the headers have more entries than the row (they really shouldn't), break early instead of segfaulting.
            if (i >= size()) {
                break;
            }

            if (result->headers[i] == key) {
                return (*this)[i];
            }
        }
    }
    STHROW_STACK("No column named " + key);
}

const string& SQResultRow::operator[](const string& key) const {
    if (result) {
        for (size_t i = 0; i < result->headers.size(); i++) {

            // If the headers have more entries than the row (they really shouldn't), break early instead of segfaulting.
            if (i >= size()) {
                break;
            }

            if (result->headers[i] == key) {
                return (*this)[i];
            }
        }
    }
    STHROW_STACK("No column named " + key);
}

string SQResult::serializeToJSON() const {
    // Just output as a simple object
    // **NOTE: This probably isn't super fast, but could be easily optimized
    //         if it ever became necessary.
    STable output;
    output["headers"] = SComposeJSONArray(headers);
    vector<string> jsonRows;
    for (size_t c = 0; c < rows.size(); ++c)
        jsonRows.push_back(SComposeJSONArray(rows[c]));
    output["rows"] = SComposeJSONArray(jsonRows);
    return SComposeJSONObject(output);
}

string SQResult::serializeToText() const {
    // Match the native format of sqlite3.
    vector<size_t> maxLengths(headers.size());
    for (size_t i = 0; i < headers.size(); i++) {
        maxLengths[i] = headers[i].size();
    }
    for (size_t i = 0; i < rows.size(); i++) {
        for (size_t j = 0; j < rows[i].size(); j++) {
            if (rows[i][j].length() > maxLengths[j]) {
                maxLengths[j] = rows[i][j].length();
            }
        }
    }

    // Create the output string
    string output;

    // Append the headers.
    for (size_t i = 0; i < headers.size(); i++) {
        string entry = headers[i];
        entry.resize(maxLengths[i], ' ');
        if (i != 0) {
            output += "  ";
        }
        output += entry;
    }
    output += "\n";

    // Now create the separator line.
    for (size_t i = 0; i < maxLengths.size(); i++) {
        string sep;
        sep.resize(maxLengths[i], '-');
        if (i != 0) {
            output += "  ";
        }
        output += sep;
    }
    output += "\n";

    // Finally, do each row.
    for (size_t i = 0; i < rows.size(); i++) {
        for (size_t j = 0; j < rows[i].size(); j++) {
            string entry = rows[i][j];
            entry.resize(maxLengths[j], ' ');
            if (j != 0) {
                output += "  ";
            }
            output += entry;
        }
        output += "\n";
    }

    // Done.
    return output;
}

string SQResult::serializeTextDelimited(char delimiter) const {
    auto needsQuoting = [&](const string& s) -> bool {
        if (s.find('"') != string::npos) return true;
        if (s.find('\n') != string::npos) return true;
        if (s.find('\r') != string::npos) return true;
        if (s.find(delimiter) != string::npos) return true;
        return false;
    };

    auto quoteField = [&](const string& s) -> string {
        if (!needsQuoting(s)) return s;
        string out;
        out.reserve(s.size() + 2);
        out.push_back('"');
        for (char c : s) {
            if (c == '"') {
                // CSV-style escape: double the quote
                out.push_back('"');
                out.push_back('"');
            } else {
                out.push_back(c);
            }
        }
        out.push_back('"');
        return out;
    };

    string output;

    // Write headers if present
    if (!headers.empty()) {
        for (size_t i = 0; i < headers.size(); ++i) {
            if (i) output.push_back(delimiter);
            output += quoteField(headers[i]);
        }
        output.push_back('\n');
    }

    // Write rows
    for (const auto& row : rows) {
        size_t cols = headers.empty() ? row.size() : headers.size();
        for (size_t j = 0; j < cols; ++j) {
            if (j) output.push_back(delimiter);
            string field = (j < row.size()) ? row[j] : string();
            output += quoteField(field);
        }
        output.push_back('\n');
    }

    return output;
}

string SQResult::serializeToQuote() const {
    auto isNumeric = [](const string& s) -> bool {
        if (s.empty()) return false;
        size_t i = 0;
        // optional sign
        if (s[i] == '+' || s[i] == '-') {
            if (++i >= s.size()) return false;
        }
        bool anyDigits = false;
        // integer part
        while (i < s.size() && isdigit(static_cast<unsigned char>(s[i]))) {
            anyDigits = true;
            ++i;
        }
        // decimal part
        if (i < s.size() && s[i] == '.') {
            ++i;
            while (i < s.size() && isdigit(static_cast<unsigned char>(s[i]))) {
                anyDigits = true;
                ++i;
            }
        }
        if (!anyDigits) return false;
        // exponent part
        if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
            ++i;
            if (i < s.size() && (s[i] == '+' || s[i] == '-')) ++i;
            bool expDigits = false;
            while (i < s.size() && isdigit(static_cast<unsigned char>(s[i]))) {
                expDigits = true;
                ++i;
            }
            if (!expDigits) return false;
        }
        return i == s.size();
    };

    auto quoteSQL = [](const string& s) -> string {
        string out;
        out.reserve(s.size() + 2);
        out.push_back('\'');
        for (char c : s) {
            if (c == '\'') {
                // SQL escape by doubling the quote
                out.push_back('\'');
                out.push_back('\'');
            } else {
                out.push_back(c);
            }
        }
        out.push_back('\'');
        return out;
    };

    const char delimiter = ','; // sqlite default separator (respects our CSV/TSV pattern)
    string output;

    // Emit headers if present (consistent with our CSV/TSV behavior)
    if (!headers.empty()) {
        for (size_t i = 0; i < headers.size(); ++i) {
            if (i) output.push_back(delimiter);
            output += quoteSQL(headers[i]);
        }
        output.push_back('\n');
    }

    for (const auto& row : rows) {
        size_t cols = headers.empty() ? row.size() : headers.size();
        for (size_t j = 0; j < cols; ++j) {
            if (j) output.push_back(delimiter);
            string field = (j < row.size()) ? row[j] : string();

            // Literal NULL (unquoted) if exactly "NULL"
            if (field == "NULL") {
                output += "NULL";
                continue;
            }

            // Numbers are bare (ASCII text, no quotes)
            if (isNumeric(field)) {
                output += field;
                continue;
            }

            // Everything else is a SQL string literal
            output += quoteSQL(field);
        }
        output.push_back('\n');
    }

    return output;
}

string SQResult::serializeToCSV() const {
    return serializeTextDelimited(',');
}

string SQResult::serializeToTSV() const {
    return serializeTextDelimited('\t');
}

string SQResult::serialize(SQResult::FORMAT format) const {
    switch (format) {
        case FORMAT::SQLITE3:
            return serializeToText();
        case FORMAT::CSV:
            return serializeToCSV();
        case FORMAT::TSV:
            return serializeToTSV();
        case FORMAT::JSON:
            return serializeToJSON();
        case FORMAT::QUOTE:
            return serializeToQuote();
    }
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
            vector<string>& row = rows[rowIndex++];
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
