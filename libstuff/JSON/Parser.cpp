#include "Parser.h"

#include "Metrics.h"
#include "SAXHandler.h"

#include <chrono>

#include <rapidjson/memorystream.h>
#include <rapidjson/reader.h>

using namespace JSON;

unique_ptr<Value> Parser::read(string_view json)
{
    auto start = chrono::high_resolution_clock::now();
    SAXHandler handler;
    rapidjson::Reader reader;
    rapidjson::MemoryStream ss(json.empty() ? "" : json.data(), json.size());
    rapidjson::ParseResult parseResult = reader.Parse(ss, handler);

    if (parseResult.IsError()) {
        throw InvalidArgument("bad JSON string, code: " + to_string(parseResult.Code()));
    }
    auto end = chrono::high_resolution_clock::now();
    reportMetrics(MetricsOperation::PARSE, chrono::duration_cast<chrono::microseconds>(end - start).count(), json.size());

    return handler.getValue();
};

unique_ptr<Value> Parser::readUnsafe(string_view json)
{
    SAXHandler handler;
    rapidjson::Reader reader;
    rapidjson::MemoryStream ss(json.empty() ? "" : json.data(), json.size());
    reader.Parse(ss, handler);

    return handler.getValue();
}
