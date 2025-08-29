#include <libstuff/libstuff.h>
#include "SQResult.h"
#include "libstuff/SQResultFormatter.h"
#include <stdexcept>

SQResultRow::SQResultRow(SQResult& result, size_t count) : result(&result) {
    data.resize(count);
}

SQResultRow::SQResultRow() : result(nullptr) {
}

void SQResultRow::push_back(const string& s) {
    data.push_back(SQValue(SQValue::TYPE::TEXT, s));
}

vector<SQValue>::iterator SQResultRow::end() {
    return data.end();
}

vector<SQValue>::const_iterator SQResultRow::end() const {
    return data.end();
}

vector<SQValue>::const_iterator SQResultRow::begin() const {
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

SQValue& SQResultRow::get(size_t index) {
    return data.at(index);
}

const string SQResultRow::at(size_t index) const {
    return data.at(index);
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
