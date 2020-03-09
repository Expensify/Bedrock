#include "libstuff.h"

const string SData::placeholder;

SData::SData() {
    // Nothing to do here
}

// Initializes a new SData from a string.  If the string provided is not
// an entire HTTPs like message, the string is used as the methodLine.
SData::SData(const string& rhs) {
    if(!SParseHTTP(rhs, methodLine, nameValueMap, content)){
        methodLine = rhs;
    }
}

// This version creates an entry, if necessary, and returns a reference
string& SData::operator[](const string& name) {
    return nameValueMap[name];
}

// This version takes care not to create an entry if none is present
const string& SData::operator[](const string& name) const {
    STable::const_iterator it = nameValueMap.find(name);
    if (it == nameValueMap.end()) {
        return placeholder;
    } else {
        return it->second;
    }
}

// Erase everything
void SData::clear() {
    methodLine.clear();
    nameValueMap.clear();
    content.clear();
}

// Erase one value
void SData::erase(const string& name) {
    nameValueMap.erase(name);
}

// Combine the name/value pairs from two datas
void SData::merge(const STable& rhs) {
    nameValueMap.insert(rhs.begin(), rhs.end());
}

// Combine two SData into one
void SData::merge(const SData& rhs) {
    // **FIXME: What do we do with the content?  Where do we use this?
    merge(rhs.nameValueMap);
}

// Returns whether or not this data is empty
bool SData::empty() const {
    return (methodLine.empty() && nameValueMap.empty() && content.empty());
}

// Returns whether or not a particular value has been set
bool SData::isSet(const string& name) const {
    return SContains(nameValueMap, name);
}

// Return as a 32-bit value.
int SData::calc(const string& name) const {
    return min((long)calc64(name), (long)0x7fffffffL);
}

// Return as a 64-bit value
int64_t SData::calc64(const string& name) const {
    STable::const_iterator it = nameValueMap.find(name);
    if (it == nameValueMap.end()) {
        return 0;
    } else {
        return strtoll(it->second.c_str(), 0, 10);
    }
}

// Return as an unsigned 64-bit value
uint64_t SData::calcU64(const string& name) const {
    STable::const_iterator it = nameValueMap.find(name);
    if (it == nameValueMap.end()) {
        return 0;
    } else {
        return strtoull(it->second.c_str(), 0, 10);
    }
}

// Returns if the value evaluates to true
bool SData::test(const string& name) const {
    const string& value = (*this)[name];
    return (SIEquals(value, "true") || calc(name) != 0);
}

// Gets the verb (eg "GET") from the method line
string SData::getVerb() const {
    return methodLine.substr(0, methodLine.find(" "));
}

// Serializes this to an ostringstream
void SData::serialize(ostringstream& out) const {
    out << SComposeHTTP(methodLine, nameValueMap, content);
}

// Serializes this to a string
string SData::serialize() const {
    // Serializes this to a string
    return SComposeHTTP(methodLine, nameValueMap, content);
}

// Deserializes from a string
int SData::deserialize(const string& rhs) {
    return (SParseHTTP(rhs, methodLine, nameValueMap, content));
}

// Deserializes from a buffer
int SData::deserialize(const char* buffer, int length) {
    return (SParseHTTP(buffer, length, methodLine, nameValueMap, content));
}

// Initializes a new SData from a string.  If there is no content provided,
// then use whatever data remains in the string as the content
// **DEPRECATED** Use the constructor that handles this instead.
SData SData::create(const string& rhs) {
    SData data;
    int header = data.deserialize(rhs);
    if (header && data.content.empty()) {
        data.content = rhs.substr(header);
    }
    return data;
}
