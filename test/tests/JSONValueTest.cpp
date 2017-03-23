#include <libstuff/libstuff.h>
#include <test/lib/BedrockTester.h>
#include <libstuff/SJSON.h>

struct JSONValueTest : tpunit::TestFixture
{
    JSONValueTest() : tpunit::TestFixture(
        TEST(JSONValueTest::setBool),
        TEST(JSONValueTest::setBool2),
        TEST(JSONValueTest::setInt),
        TEST(JSONValueTest::setInt2),
        //TEST(JSONValueTest::setBigInt),
        TEST(JSONValueTest::setDouble),
        TEST(JSONValueTest::setString),
        TEST(JSONValueTest::ctorNull),
        TEST(JSONValueTest::ctorBool),
        TEST(JSONValueTest::ctorBool2),
        TEST(JSONValueTest::ctorInt),
        TEST(JSONValueTest::ctorInt2),
        //TEST(JSONValueTest::ctorBigInt),
        TEST(JSONValueTest::ctorDouble),
        TEST(JSONValueTest::ctorString),
        TEST(JSONValueTest::simpleArray),
        TEST(JSONValueTest::nestedArray),
        TEST(JSONValueTest::simpleObject),
        TEST(JSONValueTest::nestedObject),
        TEST(JSONValueTest::invalidIndex),
        TEST(JSONValueTest::invalidKey)
    )
    {
        NAME(JSONValue);
    }

    void setBool()
    {
        SJSONValue value;
        value = true;
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(value.serialize(), "true");
    }

    void setBool2()
    {
        SJSONValue value;
        value = false;
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(value.serialize(), "false");
    }

    void setInt()
    {
        SJSONValue value;
        value = -15;
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_TRUE(value.getDouble() < 0);
        EXPECT_EQUAL(value.serialize(), "-15");
    }

    void setInt2()
    {
        SJSONValue value;
        value = 123;
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_FALSE(value.getDouble() < 0);
        EXPECT_EQUAL(value.serialize(), "123");
        EXPECT_EQUAL(value.getInt(), 123);
    }

    /*
    void setBigInt()
    {
        SJSONValue value;
        value = 9223372036854775808ULL;
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_FALSE(value.getDouble() < 0);
        EXPECT_EQUAL(value.serialize(), "9223372036854775808");
        EXPECT_THROW(value.getInt(), invalid_argument);
    }
    */

    void setDouble()
    {
        SJSONValue value;
        value = 1.5;
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(value.serialize(), "1.5");
    }

    void setString()
    {
        SJSONValue value;
        value = "test";
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_STRING);
        EXPECT_EQUAL(value.serialize(), "\"test\"");
    }

    void ctorNull()
    {
        const SJSONValue value;
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NULL);
        EXPECT_EQUAL(value.serialize(), "null");
    }

    void ctorBool()
    {
        const SJSONValue value(true);
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(value.serialize(), "true");
    }

    void ctorBool2()
    {
        const SJSONValue value(false);
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_BOOL);
        EXPECT_EQUAL(value.serialize(), "false");
    }

    void ctorInt()
    {
        const SJSONValue value(-15);
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(value.serialize(), "-15");
    }

    void ctorInt2()
    {
        const SJSONValue value(123);
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(value.serialize(), "123");
    }

    /*
    void ctorBigInt()
    {
        const SJSONValue value(9223372036854775808ULL);
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_FALSE(value.getDouble() < 0);
        EXPECT_EQUAL(value.serialize(), "9223372036854775808");
        EXPECT_THROW(value.getInt(), invalid_argument);
    }
    */

    void ctorDouble()
    {
        const SJSONValue value(1.5);
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_NUMBER);
        EXPECT_EQUAL(value.serialize(), "1.5");
    }

    void ctorString()
    {
        const SJSONValue value("test");
        EXPECT_EQUAL(value.type(), SJSONValue::JSON_STRING);
        EXPECT_EQUAL(value.serialize(), "\"test\"");
    }

    void simpleArray()
    {
        SJSONValue array(SJSONValue::JSON_ARRAY);

        EXPECT_EQUAL(array.serialize(), "[]");

        array.push_back(SJSONValue(true));
        EXPECT_EQUAL(array.serialize(), "[true]");

        array.push_back(SJSONValue(false));
        EXPECT_EQUAL(array.serialize(), "[true,false]");

        array.push_back(SJSONValue());
        EXPECT_EQUAL(array.serialize(), "[true,false,null]");

        array.push_back(SJSONValue(-15));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15]");

        array.push_back(SJSONValue(123));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15,123]");

        array.push_back(SJSONValue(1.5));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15,123,1.5]");

        array.push_back(SJSONValue("asdf"));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15,123,1.5,\"asdf\"]");
    }

    void nestedArray()
    {
        SJSONValue outer(SJSONValue::JSON_ARRAY);
        SJSONValue inner(SJSONValue::JSON_ARRAY);

        outer.push_back(SJSONValue(true));
        inner.push_back(SJSONValue(3));
        inner.push_back(2);
        inner.push_back(SJSONValue(1));
        outer.push_back(inner);
        outer.push_back(false);

        EXPECT_EQUAL(outer.serialize(), "[true,[3,2,1],false]");
    }


    void simpleObject()
    {
        SJSONValue doc(SJSONValue::JSON_OBJECT);

        EXPECT_EQUAL(doc.serialize(), "{}");
        doc["a"] = SJSONValue(3);
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3}");
        doc["b"] = SJSONValue();
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null}");
        doc["c"] = -3;
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3}");
        doc["d"] = SJSONValue(false);
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3,\"d\":false}");
        doc["d"] = true;
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3,\"d\":true}");
        doc["e"] = SJSONValue("test");
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3,\"d\":true,\"e\":\"test\"}");
    }

    void nestedObject()
    {
        SJSONValue outer(SJSONValue::JSON_OBJECT);
        SJSONValue inner(SJSONValue::JSON_OBJECT);

        outer["a"] = SJSONValue(true);
        inner["a"] = 3;
        outer["b"] = inner; // inner is passed to outer by copy
        inner["b"] = "This should not be in outer";

        EXPECT_EQUAL(inner.serialize(), "{\"a\":3,\"b\":\"This should not be in outer\"}");
        EXPECT_EQUAL(outer.serialize(), "{\"a\":true,\"b\":{\"a\":3}}");
    }

    void invalidIndex()
    {
        SJSONValue array(SJSONValue::JSON_ARRAY);

        EXPECT_EQUAL(array.size(), 0);
        EXPECT_THROW(array[0], std::out_of_range);
    }

    void invalidKey()
    {
        const SJSONValue immutableObject(SJSONValue::JSON_OBJECT);
        EXPECT_EQUAL(immutableObject.size(), 0);
        EXPECT_THROW(immutableObject["asdf"], std::out_of_range);

        SJSONValue mutableObject(SJSONValue::JSON_OBJECT);
        EXPECT_EQUAL(mutableObject.size(), 0);
        EXPECT_NO_THROW(mutableObject["asdf"] = 1);
        EXPECT_EQUAL(mutableObject.size(), 1);

    }

} __JSONValueTest;
