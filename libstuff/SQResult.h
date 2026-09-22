#pragma once
#include <concepts>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <libstuff/SQValue.h>
using namespace std;

class SQResult;

class SQResultRow {
    friend class SQResult;

public:
    SQResultRow();
    SQResultRow(SQResult& result, size_t count = 0);
    SQResultRow(SQResultRow const&) = default;
    void push_back(const string& s);
    string operator[](const size_t& key);
    const string operator[](const size_t& key) const;

    // Integer literals, including 0, must select indexed access rather than a null C-string key.
    template<integral Index>
    string operator[](Index key)
    {
        return (*this)[static_cast<size_t>(key)];
    }

    template<integral Index>
    const string operator[](Index key) const
    {
        return (*this)[static_cast<size_t>(key)];
    }

    string operator[](const string& key);
    const string operator[](const string& key) const;

    /**
     * Look up a column using a key-address cache shared by all rows in the result.
     *
     * @param key Non-null, null-terminated key whose storage and contents remain unchanged until
     * the result is cleared, assigned, destroyed, or its headers are replaced using setHeaders().
     * String literals satisfy this contract. Rewriting or reusing key storage can return an
     * incorrect column; use the string overload for dynamically changing keys.
     * @return A string representation of the column value.
     * @throws SException if the key is null, the row has no result, or the column is absent from the row.
     */
    string operator[](const char* key);
    const string operator[](const char* key) const;
    vector<SQValue>::const_iterator begin() const;
    vector<SQValue>::iterator end();
    vector<SQValue>::const_iterator end() const;
    bool empty() const;
    size_t size() const;
    SQResultRow& operator=(const SQResultRow& other);
    string at(size_t index);
    SQValue& get(size_t index);
    const string at(size_t index) const;

    operator vector<string>() const;

private:
    SQResult* result = nullptr;
    vector<SQValue> data;
};

class SQResult {
    friend class SQResultRow;

public:
    SQResult() = default;
    SQResult(const SQResult& other);
    SQResult(vector<SQResultRow>&& rows, vector<string>&& headers);

    // Accessors
    bool empty() const;
    size_t size() const;
    const vector<string>& getHeaders() const;

    // Mutators
    void clear();
    void setHeaders(vector<string> newHeaders);
    void emplace_back(SQResultRow&& row);
    void resize(const size_t newSize);

    // Operators
    const SQResultRow& operator[](size_t rowNum) const;
    SQResult& operator=(const SQResult& other);

    // Serializers
    // TODO: Remove when not used in Auth, deprecated.
    string serializeToJSON() const;
    string serializeToText() const;
    string serialize(const string& format) const;

    // Deserializers
    bool deserialize(const string& json);

    // Iterator support for range-based for loops
    vector<SQResultRow>::const_iterator begin() const;
    vector<SQResultRow>::const_iterator end() const;
    vector<SQResultRow>::const_iterator cbegin() const;
    vector<SQResultRow>::const_iterator cend() const;

private:
    // Only setHeaders() may replace this after construction, so headerIndexesByAddress stays in sync.
    vector<string> headers;
    vector<SQResultRow> rows;
    mutable mutex headerIndexMutex;
    mutable unordered_map<const char*, size_t> headerIndexesByAddress;

    optional<size_t> findHeaderIndex(const char* key) const;
    void rebindRows();
};
