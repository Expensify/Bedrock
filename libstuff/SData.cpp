#include "libstuff.h"

const string SData::placeholder;
// --------------------------------------------------------------------------
SData::SData() {
    // Nothing to do here
}

// --------------------------------------------------------------------------
SData::SData(const string& method) {
    // Initialize the method
    methodLine = method;
}

// --------------------------------------------------------------------------
string& SData::operator[](const string& name) {
    // This version creates an entry, if necessary, and returns a reference
    return nameValueMap[name];
}

// --------------------------------------------------------------------------
const string& SData::operator[](const string& name) const {
    // This version takes care not to create an entry if none is present
    STable::const_iterator it = nameValueMap.find(name);
    if (it == nameValueMap.end()) {
        return placeholder;
    } else {
        return it->second;
    }
}

// --------------------------------------------------------------------------
void SData::clear() {
    // Erase everything
    methodLine.clear();
    nameValueMap.clear();
    content.clear();
}

// --------------------------------------------------------------------------
void SData::erase(const string& name) {
    // Erase one value
    nameValueMap.erase(name);
}

// --------------------------------------------------------------------------
void SData::merge(const STable& rhs) {
    // Combine the name/value pairs from two datas
    nameValueMap.insert(rhs.begin(), rhs.end());
}

// --------------------------------------------------------------------------
void SData::merge(const SData& rhs) {
    // Combine two SData into one
    // **FIXME: What do we do with the content?  Where do we use this?
    merge(rhs.nameValueMap);
}

// --------------------------------------------------------------------------
bool SData::empty() const {
    // Returns whether or not this data is empty
    return (methodLine.empty() && nameValueMap.empty() && content.empty());
}

// --------------------------------------------------------------------------
bool SData::isSet(const string& name) const {
    // Returns whether or not a particular value has been set
    return SContains(nameValueMap, name);
}

// --------------------------------------------------------------------------
int SData::calc(const string& name) const {
    // Forcing 32 bitness here.
    return min((long)calc64(name), (long)0x7fffffffL);
}

// --------------------------------------------------------------------------
int64_t SData::calc64(const string& name) const {
    // Return as a 64-bit value
    STable::const_iterator it = nameValueMap.find(name);
    if (it == nameValueMap.end()) {
        return 0;
    } else {
        return strtoll(it->second.c_str(), 0, 10);
    }
}

// --------------------------------------------------------------------------
uint64_t SData::calcU64(const string& name) const {
    // Return as an unsigned 64-bit value
    STable::const_iterator it = nameValueMap.find(name);
    if (it == nameValueMap.end()) {
        return 0;
    } else {
        return strtoull(it->second.c_str(), 0, 10);
    }
}

// --------------------------------------------------------------------------
bool SData::test(const string& name) const {
    // Returns if the value evaluates to true
    const string& value = (*this)[name];
    return (SIEquals(value, "true") || calc(name) != 0);
}

// --------------------------------------------------------------------------
string SData::getVerb() const {
    // Gets the verb (eg "GET") from the method line
    return methodLine.substr(0, methodLine.find(" "));
}

// --------------------------------------------------------------------------
void SData::serialize(ostringstream& out) const {
    // Serializes this to an ostringstream
    out << SComposeHTTP(methodLine, nameValueMap, content);
}

// --------------------------------------------------------------------------
string SData::serialize() const {
    // Serializes this to a string
    return SComposeHTTP(methodLine, nameValueMap, content);
}

// --------------------------------------------------------------------------
int SData::deserialize(const string& rhs) {
    // Deserializes from a string
    return (SParseHTTP(rhs, methodLine, nameValueMap, content));
}

// --------------------------------------------------------------------------
int SData::deserialize(const char* buffer, int length) {
    // Deserializes from a buffer
    return (SParseHTTP(buffer, length, methodLine, nameValueMap, content));
}

// --------------------------------------------------------------------------
SData SData::create(const string& rhs) {
    // Initializes a new SData from a string.  If there is no content provided,
    // then use whatever data remains in the string as the content
    SData data;
    int header = data.deserialize(rhs);
    if (header && data.content.empty()) {
        data.content = rhs.substr(header);
    }
    return data;
}
