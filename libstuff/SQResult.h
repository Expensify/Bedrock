#pragma once
#include <string>
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
    string operator[](const string& key);
    const string operator[](const string& key) const;
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
public:
    // Attributes
    vector<string> headers;

    SQResult() = default;
    SQResult(SQResult const&) = default;
    SQResult(vector<SQResultRow>&& rows, vector<string>&& headers)
        : headers(move(headers)), rows(move(rows))
    {
    }

    // Accessors
    bool empty() const;
    size_t size() const;

    // Mutators
    void clear();
    void emplace_back(SQResultRow&& row);

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
    vector<SQResultRow> rows;
};
