#include "Parser.h"

#include "Metrics.h"
#include "SAXHandler.h"

#include <chrono>

#include <rapidjson/reader.h>

using namespace JSON;

unique_ptr<Value> Parser::read(const string& json)
{
    auto start = chrono::high_resolution_clock::now();
    SAXHandler handler;
    rapidjson::Reader reader;
    rapidjson::StringStream ss(json.c_str());
    rapidjson::ParseResult parseResult = reader.Parse(ss, handler);

    if (parseResult.IsError()) {
        throw InvalidArgument("bad JSON string, code: " + to_string(parseResult.Code()));
    }
    auto end = chrono::high_resolution_clock::now();
    reportMetrics(MetricsOperation::PARSE, chrono::duration_cast<chrono::microseconds>(end - start).count(), json.size());

    return handler.getValue();
};

unique_ptr<Value> Parser::readUnsafe(const string& json)
{
    SAXHandler handler;
    rapidjson::Reader reader;
    rapidjson::StringStream ss(json.c_str());
    reader.Parse(ss, handler);

    return handler.getValue();
}
