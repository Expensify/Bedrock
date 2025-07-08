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

    SQResult() = default;
    SQResult(SQResult const&) = default;

    // Accessors
    bool empty() const;
    size_t size() const;
    const vector<SQResultRow>& getRows() const;

    // Mutators
    void clear();
    void resize(size_t newSize);
    void push_back(const SQResultRow& row);
    void emplace_back(const SQResultRow& row);
    SQResultRow& back();
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
    vector<SQResultRow>::iterator begin();
    vector<SQResultRow>::iterator end();
    vector<SQResultRow>::const_iterator begin() const;
    vector<SQResultRow>::const_iterator end() const;
    vector<SQResultRow>::const_iterator cbegin() const;
    vector<SQResultRow>::const_iterator cend() const;

  private:
    vector<SQResultRow> rows;
};
