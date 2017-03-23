#include <libstuff/libstuff.h>
#include <test/lib/BedrockTester.h>
#include <libstuff/SJSON.h>

struct JSONPerformanceTest : tpunit::TestFixture
{
    std::string sitxPolicy;
    std::string thoughtworksPolicy;
    std::string personalPolicy;

    bool loaded;

    JSONPerformanceTest() : tpunit::TestFixture(
        TEST(JSONPerformanceTest::successfulLoad),
        TEST(JSONPerformanceTest::parseSerializeThoughtworks),
        TEST(JSONPerformanceTest::rapidParseSerializeThoughtworks),
        TEST(JSONPerformanceTest::parseSerializeSitx),
        TEST(JSONPerformanceTest::rapidParseSerializeSitx),
        TEST(JSONPerformanceTest::parseSerializePersonal),
        TEST(JSONPerformanceTest::rapidParseSerializePersonal)
    ), loaded(true)
    {
        NAME(JSONPerformance);
    }

    void successfulLoad()
    {
        loaded &= SFileLoad("data/si-tx-operations.json", sitxPolicy);
        loaded &= SFileLoad("data/thoughtworks-us.json", thoughtworksPolicy);
        loaded &= SFileLoad("data/personal.json", personalPolicy);

        ASSERT_TRUE(loaded);
        ASSERT_TRUE(sitxPolicy.length() > 1000000);
        ASSERT_TRUE(thoughtworksPolicy.length() > 1000000);
    }

    static void parseSerializePolicy(const std::string& policy, const size_t iterations = 1)
    {
        const std::string& hash          = SHashSHA1(policy);
        bool               passing       = true;
        uint64_t           parseTime     = 0;
        uint64_t           serializeTime = 0;
        uint64_t           hashTime      = 0;
        uint64_t           start;

        for (size_t i = 0; i < iterations; i++) {
            start = STimeNow();
            const SJSONValue value = SJSONValue::deserialize(policy);
            parseTime += STimeNow() - start;

            start = STimeNow();
            const string json = value.serialize();
            serializeTime += STimeNow() - start;

            start = STimeNow();
            const std::string& newHash = SHashSHA1(json);
            hashTime += STimeNow() - start;

            passing &= (newHash == hash);
        }
        parseTime     /= 1000;
        serializeTime /= 1000;
        hashTime      /= 1000;
    }

    static void rapidParseSerializePolicy(const std::string& policy, const size_t iterations = 1)
    {
        const std::string& hash          = SHashSHA1(policy);
        bool               passing       = true;
        uint64_t           parseTime     = 0;
        uint64_t           serializeTime = 0;
        uint64_t           hashTime      = 0;
        uint64_t           start;

        for (size_t i = 0; i < iterations; i++) {
            start = STimeNow();
            const SJSONValue value = SJSONValue::deserialize(policy);
            parseTime += STimeNow() - start;

            start = STimeNow();
            const string json = value.serialize();
            serializeTime += STimeNow() - start;

            start = STimeNow();
            const std::string& newHash = SHashSHA1(json);
            hashTime += STimeNow() - start;

            passing &= (newHash == hash);
        }
        parseTime     /= 1000;
        serializeTime /= 1000;
        hashTime      /= 1000;
    }

    void parseSerializeThoughtworks()
    {
        parseSerializePolicy(thoughtworksPolicy);
    }

    void rapidParseSerializeThoughtworks()
    {
        rapidParseSerializePolicy(thoughtworksPolicy);
    }

    void parseSerializeSitx()
    {
        parseSerializePolicy(sitxPolicy);
    }

    void rapidParseSerializeSitx()
    {
        rapidParseSerializePolicy(sitxPolicy);
    }

    void parseSerializePersonal()
    {
        parseSerializePolicy(personalPolicy, 1000);
    }

    void rapidParseSerializePersonal()
    {
        rapidParseSerializePolicy(personalPolicy, 1000);
    }

} __JSONPerformanceTest;
