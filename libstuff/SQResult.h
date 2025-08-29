#pragma once
// Can't include libstuff.h here because it'd be circular.
#include <string>
#include <vector>
using namespace std;
class SQResult;

class SQResultRow {
    friend class SQResult;
  public:

    class ColVal {
      public:
        enum class TYPE {
            NONE, // because NULL is overloaded.
            INTEGER,
            REAL,
            TEXT,
            BLOB,
        };

      operator string() const;

      // Constructors;
      ColVal();
      ColVal(int64_t val);
      ColVal(double val);
      ColVal(const char* val);
      explicit ColVal(const string& val);
      explicit ColVal(TYPE t, const string& val);

      ColVal& operator=(const string& val);
      ColVal& operator=(string&& val);
      ColVal& operator=(const char* val);

      // Allow string concatenation.
      friend string operator+(string lhs, const ColVal& rhs);
      friend string operator+(const ColVal& lhs, string rhs);
      friend string operator+(const char* lhs, const ColVal& rhs);
      friend string operator+(const ColVal& lhs, const char* rhs);
      friend string operator+(const ColVal& lhs, const ColVal& rhs);

      // And string comparison
      friend bool operator==(const ColVal& a, const string& b);
      friend bool operator==(const string& a, const ColVal& b);
      friend bool operator==(const ColVal& a, const char* b);
      friend bool operator==(const char* a, const ColVal& b);

      friend bool operator==(const ColVal& a, const ColVal& b);
      friend bool operator!=(const ColVal& a, const ColVal& b) { return !(a == b); }

      bool empty() const;
      size_t size() const;

      friend ostream& operator<<(ostream& os, const ColVal& v);

      private:
          TYPE type;

          // Treat these as a union.
          int64_t integer{0};
          double real{0.0};
          string text;
          // Not used, we simply store this in `text` as they're stored the same way.
          // This is left here to make it clear it's not accidentally omitted.
          // string blob;
      };

    template <class InputIt>
    vector<ColVal>::iterator insert(vector<ColVal>::const_iterator pos, InputIt first, InputIt last) {
        return data.insert(pos, first, last);
    }

    SQResultRow();
    SQResultRow(SQResult& result, size_t count = 0);
    SQResultRow(SQResultRow const&) = default;
    void push_back(const string& s);
    string operator[](const size_t& key);
    const string operator[](const size_t& key) const;
    string operator[](const string& key);
    const string operator[](const string& key) const;
    vector<ColVal>::const_iterator begin() const;
    vector<ColVal>::iterator end();
    vector<ColVal>::const_iterator end() const;
    bool empty() const;
    size_t size() const;
    SQResultRow& operator=(const SQResultRow& other);
    string at(size_t index);
    ColVal& get(size_t index);
    const string at(size_t index) const;

    operator vector<string>() const;

  private:
    SQResult* result = nullptr;
    vector<ColVal> data;
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
