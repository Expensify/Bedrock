#include "SQliteParameter.h"

#include "libstuff.h"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <utility>

SQliteParameter SQliteParameter::null()
{
    return {};
}

SQliteParameter SQliteParameter::i(int64_t v)
{
    SQliteParameter p;
    p.type = Type::Int64;
    p.intValue = v;
    return p;
}

SQliteParameter SQliteParameter::d(double v)
{
    SQliteParameter p;
    p.type = Type::Double;
    p.doubleValue = v;
    return p;
}

SQliteParameter SQliteParameter::text(string v)
{
    SQliteParameter p;
    p.type = Type::Text;
    p.stringValue = move(v);
    return p;
}

SQliteParameter SQliteParameter::blob(string v)
{
    SQliteParameter p;
    p.type = Type::Blob;
    p.stringValue = move(v);
    return p;
}

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

string SQliteParameter::uriEncodeParamName(const string& name)
{
    string out;
    out.reserve(name.size());
    for (char c : name) {
        if (c == ':' || c == '@' || c == '$' || c == '#') {
            char buf[4];
            snprintf(buf, sizeof(buf), "#%02X", (unsigned char) c);
            out += buf;
        } else {
            out += c;
        }
    }
    return out;
}

string SQliteParameter::uriDecodeParamName(const string& encoded)
{
    string out;
    out.reserve(encoded.size());
    for (size_t i = 0; i < encoded.size(); i++) {
        if (encoded[i] == '#' && i + 2 < encoded.size()
            && isxdigit((unsigned char) encoded[i + 1]) && isxdigit((unsigned char) encoded[i + 2])) {
            char hex[3] = {encoded[i + 1], encoded[i + 2], 0};
            out += (char) strtol(hex, nullptr, 16);
            i += 2;
        } else {
            out += encoded[i];
        }
    }
    return out;
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
