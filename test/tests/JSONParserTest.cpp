#include <libstuff/JSON/Value.h>
#include <test/lib/tpunit++.hpp>

struct JSONParserTest : tpunit::TestFixture
{
    JSONParserTest() : tpunit::TestFixture("JSONParser",
                                           TEST(JSONParserTest::unicode),
                                           TEST(JSONParserTest::simpleNull),
                                           TEST(JSONParserTest::simpleBool),
                                           TEST(JSONParserTest::simpleBool2),
                                           TEST(JSONParserTest::simpleInt),
                                           TEST(JSONParserTest::simpleInt2),
                                           TEST(JSONParserTest::simpleBigInt),
                                           TEST(JSONParserTest::simpleDouble),
                                           TEST(JSONParserTest::simpleString),
                                           TEST(JSONParserTest::simpleArray),
                                           TEST(JSONParserTest::nestedArray),
                                           TEST(JSONParserTest::simpleObject),
                                           TEST(JSONParserTest::nestedObject),
                                           TEST(JSONParserTest::badJSON)
    )
    {
    }

    void unicode()
    {
        const JSON::Value value = JSON::Value::parse("\"A VaL1d'\\u00ae\\ud83c\\udf55\\u00e9\"");
        EXPECT_EQUAL(value.getString(), "A VaL1d'\xC2\xAE\xF0\x9F\x8D\x95\xC3\xA9");
    }

    void simpleNull()
    {
        EXPECT_EQUAL(JSON::Value::parse("null").type(), JSON::NIL);
    }

    void simpleBool()
    {
        const JSON::Value value = JSON::Value::parse("true");
        EXPECT_EQUAL(value.type(), JSON::BOOL);
        EXPECT_EQUAL(value.getBool(), true);
    }

    void simpleBool2()
    {
        const JSON::Value value = JSON::Value::parse("false");
        EXPECT_EQUAL(value.type(), JSON::BOOL);
        EXPECT_EQUAL(value.getBool(), false);
    }

    void simpleInt()
    {
        const JSON::Value value = JSON::Value::parse("-15");
        EXPECT_EQUAL(value.type(), JSON::INT);
        EXPECT_FALSE(value.isHuge());
        EXPECT_EQUAL(value.getInt(), -15);
    }

    void simpleInt2()
    {
        const JSON::Value value = JSON::Value::parse("123");
        EXPECT_EQUAL(value.type(), JSON::INT);
        EXPECT_FALSE(value.isHuge());
        EXPECT_EQUAL(value.getInt(), 123);
    }

    void simpleBigInt()
    {
        // 2^63 + 1
        const JSON::Value value = JSON::Value::parse("9223372036854775808");
        EXPECT_EQUAL(value.type(), JSON::INT);
        EXPECT_TRUE(value.isHuge());
        EXPECT_EQUAL(value.getUint(), 9223372036854775808U);
    }

    void simpleDouble()
    {
        const JSON::Value value = JSON::Value::parse("1.5");
        EXPECT_EQUAL(value.type(), JSON::FLOAT);
        EXPECT_EQUAL(value.getFloat(), 1.5);
    }

    void simpleString()
    {
        const JSON::Value value = JSON::Value::parse("\"test\"");
        EXPECT_EQUAL(value.type(), JSON::STRING);
        EXPECT_EQUAL(value.getString(), "test");
    }

    void simpleArray()
    {
        const JSON::Value array = JSON::Value::parse("[true,false,null,-15,123,1.5,\"asdf\"]");

        EXPECT_EQUAL(array.size(), 7);

        EXPECT_EQUAL(array[0].type(), JSON::BOOL);
        EXPECT_EQUAL(array[0].getBool(), true);

        EXPECT_EQUAL(array[1].type(), JSON::BOOL);
        EXPECT_EQUAL(array[1].getBool(), false);

        EXPECT_EQUAL(array[2].type(), JSON::NIL);

        EXPECT_EQUAL(array[3].type(), JSON::INT);
        EXPECT_EQUAL(array[3].getInt(), -15);

        EXPECT_EQUAL(array[4].type(), JSON::INT);
        EXPECT_EQUAL(array[4].getInt(), 123);

        EXPECT_EQUAL(array[5].type(), JSON::FLOAT);
        EXPECT_EQUAL(array[5].getFloat(), 1.5);

        EXPECT_EQUAL(array[6].type(), JSON::STRING);
        EXPECT_EQUAL(array[6].getString(), "asdf");
    }

    void nestedArray()
    {
        const JSON::Value array = JSON::Value::parse("[true,[3,2,1],false]");

        EXPECT_EQUAL(array.size(), 3);

        EXPECT_EQUAL(array[0].type(), JSON::BOOL);
        EXPECT_EQUAL(array[1].type(), JSON::ARRAY);
        EXPECT_EQUAL(array[2].type(), JSON::BOOL);

        EXPECT_EQUAL(array[0].getBool(), true);
        EXPECT_EQUAL(array[2].getBool(), false);

        EXPECT_EQUAL(array[1].size(), 3);

        EXPECT_EQUAL(array[1][0].type(), JSON::INT);
        EXPECT_EQUAL(array[1][1].type(), JSON::INT);
        EXPECT_EQUAL(array[1][2].type(), JSON::INT);

        EXPECT_EQUAL(array[1][0].getInt(), 3);
        EXPECT_EQUAL(array[1][1].getInt(), 2);
        EXPECT_EQUAL(array[1][2].getInt(), 1);
    }

    void simpleObject()
    {
        const JSON::Value object = JSON::Value::parse("{\"a\":3,\"b\":null,\"c\":-3,\"d\":true,\"e\":\"test\",\"f\":-1.5}");

        EXPECT_EQUAL(object.size(), 6);

        EXPECT_EQUAL(object["a"].type(), JSON::INT);
        EXPECT_EQUAL(object["b"].type(), JSON::NIL);
        EXPECT_EQUAL(object["c"].type(), JSON::INT);
        EXPECT_EQUAL(object["d"].type(), JSON::BOOL);
        EXPECT_EQUAL(object["e"].type(), JSON::STRING);
        EXPECT_EQUAL(object["f"].type(), JSON::FLOAT);

        EXPECT_EQUAL(object["a"].getInt(), 3);
        EXPECT_EQUAL(object["c"].getInt(), -3);
        EXPECT_EQUAL(object["d"].getBool(), true);
        EXPECT_EQUAL(object["e"].getString(), "test");
        EXPECT_EQUAL(object["f"].getFloat(), -1.5);
    }

    void nestedObject()
    {
        const JSON::Value object = JSON::Value::parse("{\"a\":true,\"b\":{\"a\":3}}");

        EXPECT_EQUAL(object.size(), 2);

        EXPECT_EQUAL(object["a"].type(), JSON::BOOL);
        EXPECT_EQUAL(object["b"].type(), JSON::OBJECT);
        EXPECT_EQUAL(object["a"].getBool(), true);

        EXPECT_EQUAL(object["b"].size(), 1);
        EXPECT_EQUAL(object["b"]["a"].type(), JSON::INT);
        EXPECT_EQUAL(object["b"]["a"].getInt(), 3);
    }

    void badJSON()
    {
        EXPECT_THROW(JSON::Value::parse(""), JSON::InvalidArgument);
        EXPECT_THROW(JSON::Value::parse("{"), JSON::InvalidArgument);
        EXPECT_THROW(JSON::Value::parse("["), JSON::InvalidArgument);
        EXPECT_THROW(JSON::Value::parse("]"), JSON::InvalidArgument);
        EXPECT_THROW(JSON::Value::parse("}"), JSON::InvalidArgument);
        EXPECT_NO_THROW(JSON::Value::parse("{}"));
        EXPECT_NO_THROW(JSON::Value::parse("[]"));
        EXPECT_NO_THROW(JSON::Value::parse("null"));
        EXPECT_NO_THROW(JSON::Value::parse("true"));
        EXPECT_NO_THROW(JSON::Value::parse("1.4"));
        EXPECT_NO_THROW(JSON::Value::parse("45"));
    }
} __JSONParserTest;
