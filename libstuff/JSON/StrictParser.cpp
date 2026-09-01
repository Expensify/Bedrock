#include "Parser.h"

#include "Metrics.h"

#include <chrono>
#include <string>

#include <rapidjson/document.h>

using namespace JSON;

namespace
{
Value convertValue(const rapidjson::Value& source)
{
    if (source.IsNull()) {
        return Value(NIL);
    }
    if (source.IsBool()) {
        return Value(source.GetBool());
    }
    if (source.IsInt64()) {
        return Value(source.GetInt64());
    }
    if (source.IsUint64()) {
        return Value(source.GetUint64());
    }
    if (source.IsDouble()) {
        return Value(source.GetDouble());
    }
    if (source.IsString()) {
        return Value(string(source.GetString(), source.GetStringLength()));
    }
    if (source.IsArray()) {
        Value result(ARRAY);
        result.arrayReserve(source.Size());
        for (const auto& item : source.GetArray()) {
            result.push_back(convertValue(item));
        }
        return result;
    }

    Value result(OBJECT);
    for (auto member = source.MemberBegin(); member != source.MemberEnd(); ++member) {
        string key(member->name.GetString(), member->name.GetStringLength());
        if (!result.emplace(key, convertValue(member->value)).second) {
            throw InvalidArgument("bad JSON string, duplicate object key");
        }
    }
    return result;
}
}

unique_ptr<Value> Parser::readStrict(const string& json)
{
    if (json.find('\0') != string::npos) {
        throw InvalidArgument("bad JSON string, embedded NUL byte");
    }

    const auto start = chrono::high_resolution_clock::now();
    rapidjson::Document document;
    constexpr unsigned parseFlags = rapidjson::kParseValidateEncodingFlag | rapidjson::kParseFullPrecisionFlag;
    rapidjson::ParseResult parseResult = document.Parse<parseFlags>(json.data(), json.size());
    if (parseResult.IsError()) {
        throw InvalidArgument("bad JSON string, code: " + to_string(parseResult.Code()));
    }

    auto value = make_unique<Value>(convertValue(document));
    const auto end = chrono::high_resolution_clock::now();
    reportMetrics(MetricsOperation::PARSE, chrono::duration_cast<chrono::microseconds>(end - start).count(), json.size());
    return value;
}
