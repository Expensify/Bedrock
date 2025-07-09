#pragma once
// Can't include libstuff.h here because it'd be circular.
#include <string>
#include <vector>
using namespace std;
class SQResult;

class SQResultRow : public vector<string> {
    friend class SQResult;
  public:
    SQResultRow();
    SQResultRow(SQResult& result, size_t count = 0);
    SQResultRow(SQResultRow const&) = default;
    void push_back(const string& s);
    string& operator[](const size_t& key);
    const string& operator[](const size_t& key) const;
    string& operator[](const string& key);
    const string& operator[](const string& key) const;
    SQResultRow& operator=(const SQResultRow& other);

  private:
    SQResult* result = nullptr;
};

class SQResult {
  public:
    // Attributes
    vector<string> headers;
    vector<SQResultRow> rows;

    SQResult() = default;
    SQResult(SQResult const&) = default;
    SQResult(vector<SQResultRow>&& rows, vector<string>&& headers)
        : headers(move(headers)), rows(move(rows)) {
    }

    // Accessors
    bool empty() const;
    size_t size() const;

    // Mutators
    void clear();
    void resize(size_t newSize);
    void emplace_back(const SQResultRow&& row);
    const SQResultRow& back() const;

    // Operators
    SQResultRow& operator[](size_t rowNum);
    const SQResultRow& operator[](size_t rowNum) const;
    SQResult& operator=(const SQResult& other);

    // Serializers
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
};
