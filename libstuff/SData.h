#pragma once
#include <string>

#include <libstuff/libstuff.h>

using namespace std;

// --------------------------------------------------------------------------
// A very simple HTTP-like structure consisting of a method line, a table,
// and a content body.
// --------------------------------------------------------------------------
struct SData {
    // Public attributes
    string methodLine;
    STable nameValueMap;
    string content;

    // Constructors
    SData();

    // Initializes a new SData from a string. If the string provided is not
    // an entire HTTPs like message, the string is used as the methodLine.
    SData(const string& fromString);

    // Allow conversion from STable.
    SData(const STable& from);

    // Allow forwarding emplacements directly so SData can act like `std::map`.
    template <typename... Ts>
    pair<decltype(nameValueMap)::iterator, bool> emplace(Ts&&... args) {
        return nameValueMap.emplace(forward<Ts>(args)...);
    }

    // Operators
    // This version creates an entry, if necessary, and returns a reference
    string& operator[](const string& name);

    // This version takes care not to create an entry if none is present
    const string& operator[](const string& name) const;

    // Two templated versions of `set` are provided. One for arithmetic types, and one for other types (which must be
    // convertible to 'string'). These allow you to do the following:
    // SData.set("count", 7);
    // SData.set("name", "Tyler");
    // for all string and integer types.
    template <typename T>
    typename enable_if<is_arithmetic<T>::value, void>::type set(const string& key, const T item)
    {
        nameValueMap[key] = to_string(item);
    }
    template <typename T>
    typename enable_if<!is_arithmetic<T>::value, void>::type set(const string& key, const T item)
    {
        nameValueMap[key] = item;
    }

    // Mutators
    // Erase everything
    void clear();

    // Erase one value
    void erase(const string& name);

    // Combine the name/value pairs from two datas
    void merge(const STable& table);

    // Combine two SData into one
    void merge(const SData& rhs);

    // Accessors
    // Returns whether or not this data is empty
    bool empty() const;

    // Returns whether or not a particular value has been set
    bool isSet(const string& name) const;

    // Return as an int value.
    int calc(const string& name) const;

    // Return as a 64-bit value
    int64_t calc64(const string& name) const;

    // Return as an unsigned 64-bit value
    uint64_t calcU64(const string& name) const;

    // Returns if the value evaluates to true
    bool test(const string& name) const;

    // Gets the verb (eg "GET") from the method line
    string getVerb() const;

    // Serialization
    // Serializes this to an ostringstream
    void serialize(ostringstream& out) const;

    // Serializes this to a string
    string serialize() const;

    // Deserializes from a string
    int deserialize(const string& rhs);

    // Deserializes from a buffer
    int deserialize(const char* buffer, size_t length);

    // Deserializes from an SFastBuffer.
    int deserialize(const SFastBuffer& buf);

    // Initializes a new SData from a string. If there is no content provided,
    // then use whatever data remains in the string as the content
    // **DEPRECATED** Use the constructor that handles this instead.
    static SData create(const string& rhs);
    static const string placeholder;
};

// Support output stream operations.
inline ostream& operator<<(ostream& output, const SData& val)
{
    output << val.serialize();
    return output;
}
