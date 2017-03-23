#include <libstuff/libstuff.h>
#include <test/lib/BedrockTester.h>
#include <libstuff/SJSON.h>

struct JSONParserTest : tpunit::TestFixture
{
    JSONParserTest() : tpunit::TestFixture(
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
        NAME(JSONParser);
    }

    void unicode()
    {
        // The reasonable way to check the output of these values is to compare against known byte sequences. Printing
        // them out works, too, on modern terminals with UTF-8 support.

        // one-byte encoding.
        SJSONValue value = SJSONValue::deserialize("\"\\u0024\"");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_STRING);
        EXPECT_EQUAL(value.getString(), "$");

        // two-byte encoding (upside down M thing).
        value = SJSONValue::deserialize("\"\\u019C\"");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_STRING);
        // cout << value.getString() << endl;

        // Three-byte encoding (infinity symbol thing).
        value = SJSONValue::deserialize("\"\\u1011\"");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_STRING);
        // cout << value.getString() << endl;

        // All of them together, padded with other text.
        value = SJSONValue::deserialize("\"begin\\u0024\\u019C\\u1011end\"");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_STRING);
        // cout << value.getString() << endl;
    }

    void simpleNull()
    {
        SJSONValue value = SJSONValue::deserialize("null");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NULL);
    }

    void simpleBool()
    {
        SJSONValue value = SJSONValue::deserialize("true");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(value.getBool(), true);
    }

    void simpleBool2()
    {
        SJSONValue value = SJSONValue::deserialize("false");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(value.getBool(), false);
    }

    void simpleInt()
    {
        SJSONValue value = SJSONValue::deserialize("-15");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_TRUE(value.getDouble() < 0);
        EXPECT_EQUAL(value.getInt(), -15);
    }

    void simpleInt2()
    {
        SJSONValue value = SJSONValue::deserialize("123");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_FALSE(value.getDouble() < 0);
        EXPECT_EQUAL(value.getInt(), 123);
    }

    void simpleBigInt()
    {
        // 2^63 + 1
        SJSONValue value = SJSONValue::deserialize("9223372036854775808");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        //EXPECT_TRUE(value.isHuge());
        EXPECT_FALSE(value.getDouble() < 0);
        //EXPECT_EQUAL(value.getUint(), 9223372036854775808U);
    }

    void simpleDouble()
    {
        SJSONValue value = SJSONValue::deserialize("1.5");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(value.getDouble(), 1.5);
    }

    void simpleString()
    {
        SJSONValue value = SJSONValue::deserialize("\"test\"");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_STRING);
        EXPECT_EQUAL(value.getString(), "test");
    }

    void simpleArray()
    {
        SJSONValue array = SJSONValue::deserialize("[true,false,null,-15,123,1.5,\"asdf\"]");

        EXPECT_EQUAL(array.size(), 7);

        EXPECT_EQUAL(array[0].type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(array[0].getBool(), true);

        EXPECT_EQUAL(array[1].type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(array[1].getBool(), false);

        EXPECT_EQUAL(array[2].type(), SJSONValue::JSON_NULL);

        EXPECT_EQUAL(array[3].type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(array[3].getInt(), -15);

        EXPECT_EQUAL(array[4].type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(array[4].getInt(), 123);

        EXPECT_EQUAL(array[5].type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(array[5].getDouble(), 1.5);

        EXPECT_EQUAL(array[6].type(), SJSONValue::JSON_STRING);
        EXPECT_EQUAL(array[6].getString(), "asdf");
    }

    void nestedArray()
    {
        SJSONValue array = SJSONValue::deserialize("[true,[3,2,1],false]");

        EXPECT_EQUAL(array.size(), 3);

        EXPECT_EQUAL(array[0].type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(array[1].type(), SJSONValue::JSON_ARRAY);
        EXPECT_EQUAL(array[2].type(), SJSONValue::JSON_BOOL);

        EXPECT_EQUAL(array[0].getBool(), true);
        EXPECT_EQUAL(array[2].getBool(), false);

        EXPECT_EQUAL(array[1].size(), 3);

        EXPECT_EQUAL(array[1][0].type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(array[1][1].type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(array[1][2].type(), SJSONValue::JSON_NUMBER);

        EXPECT_EQUAL(array[1][0].getInt(), 3);
        EXPECT_EQUAL(array[1][1].getInt(), 2);
        EXPECT_EQUAL(array[1][2].getInt(), 1);
    }


    void simpleObject()
    {
        SJSONValue object = SJSONValue::deserialize("{\"a\":3,\"b\":null,\"c\":-3,\"d\":true,\"e\":\"test\",\"f\":-1.5}");

        EXPECT_EQUAL(object.size(), 6);

        EXPECT_EQUAL(object["a"].type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(object["b"].type(), SJSONValue::JSON_NULL);
        EXPECT_EQUAL(object["c"].type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(object["d"].type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(object["e"].type(), SJSONValue::JSON_STRING);
        EXPECT_EQUAL(object["f"].type(), SJSONValue::JSON_NUMBER);

        EXPECT_EQUAL(object["a"].getInt(), 3);
        EXPECT_EQUAL(object["c"].getInt(), -3);
        EXPECT_EQUAL(object["d"].getBool(), true);
        EXPECT_EQUAL(object["e"].getString(), "test");
        EXPECT_EQUAL(object["f"].getDouble(), -1.5);
    }

    void nestedObject()
    {
        SJSONValue object = SJSONValue::deserialize("{\"a\":true,\"b\":{\"a\":3}}");

        EXPECT_EQUAL(object.size(), 2);

        EXPECT_EQUAL(object["a"].type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(object["b"].type(), SJSONValue::JSON_OBJECT);
        EXPECT_EQUAL(object["a"].getBool(), true);

        EXPECT_EQUAL(object["b"].size(), 1);
        EXPECT_EQUAL(object["b"]["a"].type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(object["b"]["a"].getInt(), 3);
    }

    void badJSON()
    {
        EXPECT_THROW(SJSONValue::deserialize(""),     out_of_range);
        EXPECT_THROW(SJSONValue::deserialize("{"),    out_of_range);
        EXPECT_THROW(SJSONValue::deserialize("["),    out_of_range);
        EXPECT_THROW(SJSONValue::deserialize("]"),    out_of_range);
        EXPECT_THROW(SJSONValue::deserialize("}"),    out_of_range);
        EXPECT_NO_THROW(SJSONValue::deserialize("{}"));
        EXPECT_NO_THROW(SJSONValue::deserialize("[]"));
        EXPECT_NO_THROW(SJSONValue::deserialize("null"));
        EXPECT_NO_THROW(SJSONValue::deserialize("true"));
        EXPECT_NO_THROW(SJSONValue::deserialize("1.4"));
        EXPECT_NO_THROW(SJSONValue::deserialize("45"));
    }
} __JSONParserTest;
