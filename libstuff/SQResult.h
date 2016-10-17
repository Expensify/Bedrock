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
    inline bool empty() const { return rows.empty(); }
    inline size_t size() const { return rows.size(); }

    // Mutators
    inline void clear() {
        headers.clear();
        rows.clear();
    }

    // Operators
    inline vector<string>& operator[](size_t rowNum) { return rows[rowNum]; }
    inline const vector<string>& operator[](size_t rowNum) const { return rows[rowNum]; }

    // Serializers
    string serializeToJSON() const;
    string serializeToText() const;
    string serialize(const string& format) const;

    // Deserializers
    bool deserialize(const string& json);
};
