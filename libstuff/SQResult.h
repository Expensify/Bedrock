#pragma once
// Can't include libstuff.h here because it'd be circular.
#include <string>
#include <vector>
using namespace std;
class SQResult;

class SQResultRow {
    friend class SQResult;
  public:

    class COLVAL {
      enum class TYPES {
        NONE, // because NULL is overloaded.
        INTEGER,
        REAL,
        TEXT,
        BLOB,
      };
      union DATA {
        int64_t none;
        int64_t integer;
        double real;
        string text;
        string blob;
      };
    };

    template <class InputIt>
    vector<string>::iterator insert(vector<string>::const_iterator pos, InputIt first, InputIt last) {
        return data.insert(pos, first, last);
    }

    SQResultRow();
    SQResultRow(SQResult& result, size_t count = 0);
    SQResultRow(SQResultRow const&) = default;
    void push_back(const string& s);
    string& operator[](const size_t& key);
    const string& operator[](const size_t& key) const;
    string& operator[](const string& key);
    const string& operator[](const string& key) const;
    vector<string>::const_iterator begin() const;
    vector<string>::iterator end();
    vector<string>::const_iterator end() const;
    bool empty() const;
    size_t size() const;
    SQResultRow& operator=(const SQResultRow& other);
    string& at(size_t index);
    const string& at(size_t index) const;

    operator const std::vector<std::string>&() const;

  private:
    SQResult* result = nullptr;
    vector<string> data;
};

class SQResult {
  public:
    // Attributes
    vector<string> headers;

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
    void emplace_back(SQResultRow&& row);

    // Operators
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

  private:
    vector<SQResultRow> rows;
};
