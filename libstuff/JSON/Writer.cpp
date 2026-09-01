#include "Writer.h"

#include "Metrics.h"

#include <libstuff/libstuff.h>

using namespace JSON;

Writer::Writer(const ValueType type) : rootType(type), buffer(), writer(buffer)
{
    if (rootType == OBJECT) {
        writer.StartObject();
    } else if (rootType == ARRAY) {
        writer.StartArray();
    } else {
        STHROW("500 Unsupported ValueType given to JSON::Writer");
    }
};

void Writer::writeMemberName(const string& name)
{
    writer.Key(name.c_str(), (unsigned int) name.size());
}

void Writer::writeValue(const string& value)
{
    writer.String(value.c_str(), (unsigned int) value.size());
}

void Writer::writeValue(const rapidjson::Document& value)
{
    value.Accept(writer);
}

void Writer::writeValue(const rapidjson::Value& value)
{
    value.Accept(writer);
}

void Writer::writeValue(const bool value)
{
    writer.Bool(value);
}

void Writer::writeValue(const int value)
{
    writer.Int(value);
}

void Writer::writeValue(const unsigned int value)
{
    writer.Uint(value);
}

void Writer::writeValue(const uint64_t value)
{
    writer.Uint64(value);
}

void Writer::writeValue(const int64_t value)
{
    writer.Int64(value);
}

void Writer::startArrayMember(const string& name)
{
    writeMemberName(name);
    writer.StartArray();
}

void Writer::startObjectMember(const string& name)
{
    writeMemberName(name);
    writer.StartObject();
}

void Writer::startArrayElement()
{
    writer.StartArray();
}

void Writer::startObjectElement()
{
    writer.StartObject();
}

void Writer::endArrayMember()
{
    writer.EndArray();
}

void Writer::endObjectMember()
{
    writer.EndObject();
}

void Writer::endArrayElement()
{
    writer.EndArray();
}

void Writer::endObjectElement()
{
    writer.EndObject();
}

string Writer::getString()
{
    if (rootType == OBJECT) {
        writer.EndObject();
    } else {
        writer.EndArray();
    }
    return string(buffer.GetString(), buffer.GetSize());
}

string Writer::serialize(const JSON::Value& value)
{
    auto start = chrono::high_resolution_clock::now();
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    serialize(value, writer);
    auto end = chrono::high_resolution_clock::now();
    reportMetrics(MetricsOperation::SERIALIZE, chrono::duration_cast<chrono::microseconds>(end - start).count(), buffer.GetSize());

    return string(buffer.GetString(), buffer.GetSize());
}

string Writer::serializePretty(const JSON::Value& value)
{
    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    serialize(value, writer);
    return string(buffer.GetString(), buffer.GetSize());
}

template<class JSONWriter>
void Writer::serialize(const JSON::Value& value, JSONWriter& writer)
{
    switch (value.type()) {
        case JSON::BOOL:
            writer.Bool(value.getBool());
            break;

        case JSON::INT:
            if (value.isHuge()) {
                writer.Uint64(value.getUint());
            } else {
                writer.Int64(value.getInt());
            }
            break;

        case JSON::FLOAT:
            writer.Double(value.getFloat());
            break;

        case JSON::STRING:
            writer.String(value.getString().c_str(), (rapidjson::SizeType) value.getString().length());
            break;

        case JSON::ARRAY:
            writer.StartArray();
            for (vector<Value>::const_iterator valueIt = value.arrayBegin(); valueIt != value.arrayEnd(); valueIt++) {
                serialize(*valueIt, writer);
            }
            writer.EndArray();
            break;

        case JSON::OBJECT:
            writer.StartObject();
            for (auto valueIt = value.objectBegin(); valueIt != value.objectEnd(); valueIt++) {
                writer.Key(valueIt->first.c_str(), (rapidjson::SizeType) valueIt->first.length());
                serialize(valueIt->second, writer);
            }
            writer.EndObject();
            break;

        default:
        case JSON::NIL:
            writer.Null();
            break;
    }
}
