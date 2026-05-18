#include "SQliteParameter.h"

#include "libstuff.h"

#include <cstdio>
#include <cstdlib>

string SQliteParameter::serialize() const
{
    switch (type) {
    case Type::Null:
        return "N";
    case Type::Int64:
        return "I" + to_string(intValue);
    case Type::Double: {
        char buf[32];
        snprintf(buf, sizeof(buf), "%.17g", doubleValue);
        return string("D") + buf;
    }
    case Type::Text:
        return "T" + SEncodeBase64(stringValue);
    case Type::Blob:
        return "B" + SEncodeBase64(stringValue);
    }
    return "N";
}

SQliteParameter SQliteParameter::deserialize(const string& encoded)
{
    if (encoded.empty()) {
        return null();
    }
    const string payload = encoded.substr(1);
    switch (encoded[0]) {
    case 'N':
        return null();
    case 'I':
        return i(strtoll(payload.c_str(), nullptr, 10));
    case 'D':
        return d(strtod(payload.c_str(), nullptr));
    case 'T':
        return text(SDecodeBase64(payload));
    case 'B':
        return blob(SDecodeBase64(payload));
    }
    return null();
}
