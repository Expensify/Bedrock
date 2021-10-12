#include "SData.h"

#include <libstuff/SFastBuffer.h>

const string SData::placeholder;

SData::SData() {
    // Nothing to do here
}

SData::SData(const STable& from) : nameValueMap(from), movedFrom(false)
{
}

SData::~SData() {
    if (movedFrom) {
        SINFO("Destructor for moved-from SData " << this);
    }
}

SData::SData(SData&& from) : 
    methodLine(move(from.methodLine)),
    nameValueMap(move(from.nameValueMap)),
    content(move(from.content)),
    movedFrom(false)
{
    from.movedFrom = true;
    from.nameValueMap.clear();
    SINFO("SData move constructor on " << (&from));
}

SData::SData(const SData& from) :
    methodLine(from.methodLine),
    nameValueMap(from.nameValueMap),
    content(from.content),
    movedFrom(false)
{
}

SData& SData::operator=(const SData& from) {
    methodLine = from.methodLine;
    nameValueMap = from.nameValueMap;
    content = from.content;
    return *this;
}

SData::SData(const string& fromString) : movedFrom(false){
    if(!SParseHTTP(fromString, methodLine, nameValueMap, content)){
        methodLine = fromString;
    }
}

string& SData::operator[](const string& name) {
    return nameValueMap[name];
}

const string& SData::operator[](const string& name) const {
    STable::const_iterator it = nameValueMap.find(name);
    if (it == nameValueMap.end()) {
        return placeholder;
    } else {
        return it->second;
    }
}

void SData::clear() {
    methodLine.clear();
    nameValueMap.clear();
    content.clear();
}

void SData::erase(const string& name) {
    nameValueMap.erase(name);
}

void SData::merge(const STable& table) {
    nameValueMap.insert(table.begin(), table.end());
}

void SData::merge(const SData& rhs) {
    // **FIXME: What do we do with the content?  Where do we use this?
    merge(rhs.nameValueMap);
}

bool SData::empty() const {
    return (methodLine.empty() && nameValueMap.empty() && content.empty());
}

bool SData::isSet(const string& name) const {
    return SContains(nameValueMap, name);
}

int SData::calc(const string& name) const {
    return min((long)calc64(name), (long)0x7fffffffL);
}

int64_t SData::calc64(const string& name) const {
    STable::const_iterator it = nameValueMap.find(name);
    if (it == nameValueMap.end()) {
        return 0;
    } else {
        return strtoll(it->second.c_str(), 0, 10);
    }
}

uint64_t SData::calcU64(const string& name) const {
    STable::const_iterator it = nameValueMap.find(name);
    if (it == nameValueMap.end()) {
        return 0;
    } else {
        return strtoull(it->second.c_str(), 0, 10);
    }
}

bool SData::test(const string& name) const {
    const string& value = (*this)[name];
    return (SIEquals(value, "true") || calc(name) != 0);
}

string SData::getVerb() const {
    return methodLine.substr(0, methodLine.find(" "));
}

void SData::serialize(ostringstream& out) const {
    out << SComposeHTTP(methodLine, nameValueMap, content);
}

string SData::serialize() const {
    return SComposeHTTP(methodLine, nameValueMap, content);
}

int SData::deserialize(const string& fromString) {
    return (SParseHTTP(fromString, methodLine, nameValueMap, content));
}

int SData::deserialize(const char* buffer, size_t length) {
    return (SParseHTTP(buffer, length, methodLine, nameValueMap, content));
}

SData SData::create(const string& fromString) {
    SData data;
    int header = data.deserialize(fromString);
    if (header && data.content.empty()) {
        data.content = fromString.substr(header);
    }
    return data;
}

int SData::deserialize(const SFastBuffer& buf) {
    return deserialize(buf.c_str(), buf.size());
}
