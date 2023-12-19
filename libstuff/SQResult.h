#pragma once
// Can't include libstuff.h here because it'd be circular.
#include <string>
#include <vector>
using namespace std;

class SQResult {
  public:
    // Attributes
    vector<string> headers;
    vector<vector<string>> rows;

    // Accessors
    bool empty() const;
    size_t size() const;

    // Mutators
    void clear();

    // Operators
    vector<string>& operator[](size_t rowNum);
    const vector<string>& operator[](size_t rowNum) const;

    string& cell(size_t row, const string& cellKey);
    const string& cell(size_t row, const string& cellKey) const;

    // Serializers
    string serializeToJSON() const;
    string serializeToText() const;
    string serialize(const string& format) const;

    // Deserializers
    bool deserialize(const string& json);
};
