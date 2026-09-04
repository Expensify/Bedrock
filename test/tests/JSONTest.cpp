#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>

#include <libstuff/JSON/Metrics.h>
#include <libstuff/JSON/Parser.h>
#include <libstuff/JSON/Value.h>
#include <test/lib/tpunit++.hpp>

using namespace std;

namespace
{
struct MetricsRecord
{
    size_t parseCalls = 0;
    size_t serializeCalls = 0;
    int64_t parseDurationUS = -1;
    int64_t serializeDurationUS = -1;
    size_t parsedSize = 0;
    size_t serializedSize = 0;
};

thread_local MetricsRecord metricsRecord;

void resetMetricsRecord()
{
    metricsRecord = {};
}

void recordMetrics(JSON::MetricsOperation operation, int64_t durationUS, size_t documentSize)
{
    switch (operation) {
        case JSON::MetricsOperation::PARSE:
            ++metricsRecord.parseCalls;
            metricsRecord.parseDurationUS = durationUS;
            metricsRecord.parsedSize = documentSize;
            break;

        case JSON::MetricsOperation::SERIALIZE:
            ++metricsRecord.serializeCalls;
            metricsRecord.serializeDurationUS = durationUS;
            metricsRecord.serializedSize = documentSize;
            break;
    }
}

class ScopedMetricsObserver
{
public:
    explicit ScopedMetricsObserver(JSON::MetricsObserver observer)
    {
        JSON::setMetricsObserver(observer);
    }

    ~ScopedMetricsObserver()
    {
        JSON::setMetricsObserver(nullptr);
    }

    ScopedMetricsObserver(const ScopedMetricsObserver&) = delete;
    ScopedMetricsObserver& operator=(const ScopedMetricsObserver&) = delete;
};
}

struct JSONTest : tpunit::TestFixture
{
    JSONTest()
        : tpunit::TestFixture("JSON",
                              TEST(JSONTest::testParseAndSerializeNestedValue),
                              TEST(JSONTest::testExactUint64),
                              TEST(JSONTest::testEqualitySemantics),
                              TEST(JSONTest::testMetricsObserver),
                              TEST(JSONTest::testDisabledMetricsObserver))
    {
    }

    void testParseAndSerializeNestedValue()
    {
        const string source = "{\"z\":[1,{\"name\":\"cafe\",\"active\":true},null],\"a\":{\"ratio\":1.5}}";
        const JSON::Value value = JSON::Value::parse(source);
        const string serialized = value.serialize();

        ASSERT_EQUAL("{\"a\":{\"ratio\":1.5},\"z\":[1,{\"active\":true,\"name\":\"cafe\"},null]}", serialized);
        ASSERT_EQUAL(value, JSON::Value::parse(serialized));
    }

    void testExactUint64()
    {
        const string source = "18446744073709551615";
        const JSON::Value value = JSON::Value::parse(source);

        ASSERT_TRUE(value.isHuge());
        ASSERT_EQUAL(numeric_limits<uint64_t>::max(), value.getUint());
        ASSERT_EQUAL(source, value.serialize());
    }

    void testEqualitySemantics()
    {
        ASSERT_EQUAL(JSON::Value::parse("{\"a\":1,\"b\":[true,null]}"),
                     JSON::Value::parse("{\"b\":[true,null],\"a\":1}"));
        ASSERT_NOT_EQUAL(JSON::Value::parse("[1,2]"), JSON::Value::parse("[2,1]"));
        ASSERT_NOT_EQUAL(JSON::Value::parse("1"), JSON::Value::parse("1.0"));
    }

    void testMetricsObserver()
    {
        const string source = "{\"items\":[1,2,3]}";
        string serialized;
        resetMetricsRecord();
        {
            ScopedMetricsObserver observer(recordMetrics);
            const JSON::Value value = JSON::Value::parse(source);
            serialized = value.serialize();
        }

        ASSERT_EQUAL(1, metricsRecord.parseCalls);
        ASSERT_EQUAL(1, metricsRecord.serializeCalls);
        ASSERT_GREATER_THAN_EQUAL(metricsRecord.parseDurationUS, 0);
        ASSERT_GREATER_THAN_EQUAL(metricsRecord.serializeDurationUS, 0);
        ASSERT_EQUAL(source.size(), metricsRecord.parsedSize);
        ASSERT_EQUAL(serialized.size(), metricsRecord.serializedSize);

        resetMetricsRecord();
        {
            ScopedMetricsObserver observer(recordMetrics);
            ASSERT_TRUE(SJSONEquals("{\"value\":1.0}", "{\"value\":1e0}"));
        }
        ASSERT_EQUAL(2, metricsRecord.parseCalls);
        ASSERT_EQUAL(0, metricsRecord.serializeCalls);

        resetMetricsRecord();
        {
            ScopedMetricsObserver observer(recordMetrics);
            ASSERT_FALSE(SJSONEquals("{", "not-json"));
        }
        ASSERT_EQUAL(2, metricsRecord.parseCalls);
        ASSERT_EQUAL(0, metricsRecord.serializeCalls);
    }

    void testDisabledMetricsObserver()
    {
        JSON::setMetricsObserver(recordMetrics);
        resetMetricsRecord();
        {
            ScopedMetricsObserver observer(nullptr);
            const JSON::Value value = JSON::Value::parse("{\"enabled\":false}");
            value.serialize();
        }

        ASSERT_EQUAL(0, metricsRecord.parseCalls);
        ASSERT_EQUAL(0, metricsRecord.serializeCalls);
    }
} __JSONTest;
