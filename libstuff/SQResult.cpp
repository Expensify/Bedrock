#include <libstuff/libstuff.h>
#include <libstuff/JSON/SAXHandler.h>
#include "SQResult.h"
#include "libstuff/SQResultFormatter.h"
#include <stdexcept>
#include <string_view>

SQResultRow::SQResultRow(SQResult& result, size_t count) : result(&result)
{
    data.resize(count);
}

SQResultRow::SQResultRow() : result(nullptr)
{
}

void SQResultRow::push_back(const string& s)
{
    data.push_back(SQValue(SQValue::TYPE::TEXT, s));
}

vector<SQValue>::iterator SQResultRow::end()
{
    return data.end();
}

vector<SQValue>::const_iterator SQResultRow::end() const
{
    return data.end();
}

vector<SQValue>::const_iterator SQResultRow::begin() const
{
    return data.begin();
}

bool SQResultRow::empty() const
{
    return data.empty();
}

size_t SQResultRow::size() const
{
    return data.size();
}

string SQResultRow::at(size_t index)
{
    return data.at(index);
}

SQValue& SQResultRow::get(size_t index)
{
    return data.at(index);
}

const string SQResultRow::at(size_t index) const
{
    return data.at(index);
}

string SQResultRow::operator[](const size_t& rowNum)
{
    try {
        return data.at(rowNum);
    } catch (const out_of_range& e) {
        SINFO("SQResultRow::operator[] out of range", {{"rowNum", to_string(rowNum)}});
        STHROW_STACK("Out of range");
    }
}

const string SQResultRow::operator[](const size_t& rowNum) const
{
    try {
        return data.at(rowNum);
    } catch (const out_of_range& e) {
        SINFO("SQResultRow::operator[] out of range", {{"rowNum", to_string(rowNum)}});
        STHROW_STACK("Out of range");
    }
}

SQResultRow& SQResultRow::operator=(const SQResultRow& other)
{
    data = other.data;
    result = other.result;
    return *this;
}

string SQResultRow::operator[](const string& key)
{
    if (result) {
        const auto& headers = result->getHeaders();
        for (size_t i = 0; i < headers.size(); i++) {
            // If the headers have more entries than the row (they really shouldn't), break early instead of segfaulting.
            if (i >= data.size()) {
                break;
            }

            if (headers[i] == key) {
                return (*this)[i];
            }
        }
    }
    STHROW_STACK("No column named " + key);
}

const string SQResultRow::operator[](const string& key) const
{
    if (result) {
        const auto& headers = result->getHeaders();
        for (size_t i = 0; i < headers.size(); i++) {
            // If the headers have more entries than the row (they really shouldn't), break early instead of segfaulting.
            if (i >= data.size()) {
                break;
            }

            if (headers[i] == key) {
                return (*this)[i];
            }
        }
    }
    STHROW_STACK("No column named " + key);
}

string SQResultRow::operator[](const char* key)
{
    return static_cast<const SQResultRow&>(*this)[key];
}

const string SQResultRow::operator[](const char* key) const
{
    if (result && key) {
        const optional<size_t> index = result->findHeaderIndex(key);
        if (index.has_value() && index.value() < data.size()) {
            return (*this)[index.value()];
        }
    }
    STHROW_STACK("No column named " + string(key ? key : "(null)"));
}

SQResult::SQResult(const SQResult& other) : headers(other.headers), rows(other.rows)
{
    rebindRows();
}

SQResult::SQResult(vector<SQResultRow>&& rows, vector<string>&& headers)
    : headers(move(headers)), rows(move(rows))
{
    rebindRows();
}

void SQResult::rebindRows()
{
    for (auto& row : rows) {
        row.result = this;
    }
}

const vector<string>& SQResult::getHeaders() const
{
    return headers;
}

void SQResult::setHeaders(vector<string> newHeaders)
{
    lock_guard<mutex> lock(headerIndexMutex);
    headerIndexesByAddress.clear();
    headers = move(newHeaders);
}

optional<size_t> SQResult::findHeaderIndex(const char* key) const
{
    lock_guard<mutex> lock(headerIndexMutex);
    const auto cached = headerIndexesByAddress.find(key);
    if (cached != headerIndexesByAddress.end()) {
        return cached->second;
    }

    const string_view name(key);
    for (size_t index = 0; index < headers.size(); ++index) {
        if (headers[index] == name) {
            headerIndexesByAddress.emplace(key, index);
            return index;
        }
    }
    return nullopt;
}

SQResultRow::operator vector<string>() const {
    vector<string> out(data.size());
    for (size_t i = 0; i < data.size(); i++) {
        out[i] = data[i];
    }
    return out;
}

string SQResult::serializeToJSON() const
{
    return SQResultFormatter::format(*this, SQResultFormatter::FORMAT::JSON);
}

string SQResult::serializeToText() const
{
    return SQResultFormatter::format(*this, SQResultFormatter::FORMAT::COLUMN);
}

string SQResult::serialize(const string& format) const
{
    // Output the appropriate type
    if (SIEquals(format, "json")) {
        return serializeToJSON();
    } else {
        return serializeToText();
    }
}

namespace {
// SQLite emits columns as object members in SELECT order, including duplicate names.
// Convert each row into a JSON array while parsing so neither order nor duplicates
// are lost in JSON::Value's object map.
class SQLiteResultHandler : public JSON::SAXHandler {
public:
    vector<string> headers;

    bool StartObject()
    {
        const bool isRow = depth++ == 1;
        return isRow ? JSON::SAXHandler::StartArray() : JSON::SAXHandler::StartObject();
    }

    bool Key(const char* str, size_t length, bool copy)
    {
        if (depth == 2) {
            if (!finishedFirstRow) {
                headers.emplace_back(str, length);
            }
            return true;
        }
        return JSON::SAXHandler::Key(str, length, copy);
    }

    bool EndObject(size_t memberCount)
    {
        if (--depth == 1) {
            finishedFirstRow = true;
            return JSON::SAXHandler::EndArray(memberCount);
        }
        return JSON::SAXHandler::EndObject(memberCount);
    }

    bool StartArray()
    {
        // Only objects can be rows. Arrays nested inside column values are allowed.
        if (depth == 1) {
            return false;
        }
        ++depth;
        return JSON::SAXHandler::StartArray();
    }

    bool EndArray(size_t elementCount)
    {
        --depth;
        return JSON::SAXHandler::EndArray(elementCount);
    }

private:
    size_t depth = 0;
    bool finishedFirstRow = false;
};

string resultValue(const JSON::Value& value, bool emptyNull)
{
    if (value.isNull() && emptyNull) {
        return "";
    }
    return value.isString() ? value.getString() : value.serialize();
}
}

bool SQResult::deserialize(const string& json)
{
    clear();

    // JSON string streams stop at NUL; do not accept a valid prefix and ignore the rest.
    if (json.find('\0') != string::npos) {
        return false;
    }

    // If there are any problems, clean up whatever we've parsed.
    try {
        const size_t pos = json.find_first_not_of(" \t\r\n");
        if (pos == string::npos) {
            return false;
        }
        if (json[pos] == '{') {
            const JSON::Value content = JSON::Value::parse(json);
            if (!content.hasMember("headers") || !content["headers"].isArray()) {
                STHROW("Missing or invalid 'headers'");
            }
            if (!content.hasMember("rows") || !content["rows"].isArray()) {
                STHROW("Missing or invalid 'rows'");
            }

            vector<string> parsedHeaders;
            for (const auto& header : JSON::ConstArrayValue(content["headers"])) {
                parsedHeaders.push_back(resultValue(header, false));
            }
            setHeaders(move(parsedHeaders));

            for (const auto& jsonRow : JSON::ConstArrayValue(content["rows"])) {
                if (!jsonRow.isArray() || jsonRow.size() != headers.size()) {
                    STHROW("Incorrect number of columns in row");
                }
                rows.emplace_back();
                SQResultRow& row = rows.back();
                row.result = this;
                for (const auto& value : JSON::ConstArrayValue(jsonRow)) {
                    row.push_back(resultValue(value, false));
                }
            }
            return true;
        }
        if (json[pos] != '[') {
            return false;
        }

        SQLiteResultHandler handler;
        rapidjson::Reader reader;
        rapidjson::StringStream stream(json.c_str());
        if (reader.Parse(stream, handler).IsError()) {
            STHROW("Invalid JSON-encoded SQResult");
        }
        const unique_ptr<JSON::Value> content = handler.getValue();
        setHeaders(move(handler.headers));
        for (const auto& jsonRow : JSON::ArrayValue(*content)) {
            if (!jsonRow.isArray()) {
                STHROW("Invalid row in JSON-encoded SQResult");
            }
            rows.emplace_back();
            SQResultRow& row = rows.back();
            row.result = this;
            for (const auto& value : JSON::ConstArrayValue(jsonRow)) {
                // SQLite-style results historically represent SQL null as an empty string.
                row.push_back(resultValue(value, true));
            }
        }
        return true;
    } catch (const SException& e) {
        SDEBUG("Failed to deserialize JSON-encoded SQResult (" << e.what() << "): " << json);
    }

    clear();
    return false;
}

bool SQResult::empty() const
{
    return rows.empty();
}

size_t SQResult::size() const
{
    return rows.size();
}

void SQResult::clear()
{
    lock_guard<mutex> lock(headerIndexMutex);
    headerIndexesByAddress.clear();
    headers.clear();
    rows.clear();
}

const SQResultRow& SQResult::operator[](size_t rowNum) const
{
    try {
        return rows.at(rowNum);
    } catch (const out_of_range& e) {
        SINFO("SQResult::operator[] out of range", {{"rowNum", to_string(rowNum)}});
        STHROW_STACK("Out of range");
    }
}

SQResult& SQResult::operator=(const SQResult& other)
{
    if (this == &other) {
        return *this;
    }
    setHeaders(other.headers);
    rows = other.rows;
    rebindRows();
    return *this;
}

vector<SQResultRow>::const_iterator SQResult::begin() const
{
    return rows.begin();
}

vector<SQResultRow>::const_iterator SQResult::end() const
{
    return rows.end();
}

vector<SQResultRow>::const_iterator SQResult::cbegin() const
{
    return rows.cbegin();
}

vector<SQResultRow>::const_iterator SQResult::cend() const
{
    return rows.cend();
}

void SQResult::emplace_back(SQResultRow&& row)
{
    rows.emplace_back(move(row));
    rows.back().result = this;
}

void SQResult::resize(const size_t newSize)
{
    const size_t oldSize = rows.size();
    rows.resize(newSize);
    for (size_t index = oldSize; index < newSize; ++index) {
        rows[index].result = this;
    }
}
