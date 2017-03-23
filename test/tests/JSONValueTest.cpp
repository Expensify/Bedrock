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
        SJSON value;
        value = true;
        EXPECT_EQUAL(value.type(), SJSON::JSON_BOOL);
        EXPECT_EQUAL(value.serialize(), "true");
    }

    void setBool2()
    {
        SJSON value;
        value = false;
        EXPECT_EQUAL(value.type(), SJSON::JSON_BOOL);
        EXPECT_EQUAL(value.serialize(), "false");
    }

    void setInt()
    {
        SJSON value;
        value = -15;
        EXPECT_EQUAL(value.type(), SJSON::JSON_NUMBER);
        EXPECT_TRUE(value.getDouble() < 0);
        EXPECT_EQUAL(value.serialize(), "-15");
    }

    void setInt2()
    {
        SJSON value;
        value = 123;
        EXPECT_EQUAL(value.type(), SJSON::JSON_NUMBER);
        EXPECT_FALSE(value.getDouble() < 0);
        EXPECT_EQUAL(value.serialize(), "123");
        EXPECT_EQUAL(value.getInt(), 123);
    }

    /*
    void setBigInt()
    {
        SJSON value;
        value = 9223372036854775808ULL;
        EXPECT_EQUAL(value.type(), SJSON::JSON_NUMBER);
        EXPECT_FALSE(value.getDouble() < 0);
        EXPECT_EQUAL(value.serialize(), "9223372036854775808");
        EXPECT_THROW(value.getInt(), invalid_argument);
    }
    */

    void setDouble()
    {
        SJSON value;
        value = 1.5;
        EXPECT_EQUAL(value.type(), SJSON::JSON_NUMBER);
        EXPECT_EQUAL(value.serialize(), "1.5");
    }

    void setString()
    {
        SJSON value;
        value = "test";
        EXPECT_EQUAL(value.type(), SJSON::JSON_STRING);
        EXPECT_EQUAL(value.serialize(), "\"test\"");
    }

    void ctorNull()
    {
        const SJSON value;
        EXPECT_EQUAL(value.type(), SJSON::JSON_NULL);
        EXPECT_EQUAL(value.serialize(), "null");
    }

    void ctorBool()
    {
        const SJSON value(true);
        EXPECT_EQUAL(value.type(), SJSON::JSON_BOOL);
        EXPECT_EQUAL(value.serialize(), "true");
    }

    void ctorBool2()
    {
        const SJSON value(false);
        EXPECT_EQUAL(value.type(), SJSON::JSON_BOOL);
        EXPECT_EQUAL(value.serialize(), "false");
    }

    void ctorInt()
    {
        const SJSON value(-15);
        EXPECT_EQUAL(value.type(), SJSON::JSON_NUMBER);
        EXPECT_EQUAL(value.serialize(), "-15");
    }

    void ctorInt2()
    {
        const SJSON value(123);
        EXPECT_EQUAL(value.type(), SJSON::JSON_NUMBER);
        EXPECT_EQUAL(value.serialize(), "123");
    }

    /*
    void ctorBigInt()
    {
        const SJSON value(9223372036854775808ULL);
        EXPECT_EQUAL(value.type(), SJSON::JSON_NUMBER);
        EXPECT_FALSE(value.getDouble() < 0);
        EXPECT_EQUAL(value.serialize(), "9223372036854775808");
        EXPECT_THROW(value.getInt(), invalid_argument);
    }
    */

    void ctorDouble()
    {
        const SJSON value(1.5);
        EXPECT_EQUAL(value.type(), SJSON::JSON_NUMBER);
        EXPECT_EQUAL(value.serialize(), "1.5");
    }

    void ctorString()
    {
        const SJSON value("test");
        EXPECT_EQUAL(value.type(), SJSON::JSON_STRING);
        EXPECT_EQUAL(value.serialize(), "\"test\"");
    }

    void simpleArray()
    {
        SJSON array(SJSON::JSON_ARRAY);

        EXPECT_EQUAL(array.serialize(), "[]");

        array.push_back(SJSON(true));
        EXPECT_EQUAL(array.serialize(), "[true]");

        array.push_back(SJSON(false));
        EXPECT_EQUAL(array.serialize(), "[true,false]");

        array.push_back(SJSON());
        EXPECT_EQUAL(array.serialize(), "[true,false,null]");

        array.push_back(SJSON(-15));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15]");

        array.push_back(SJSON(123));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15,123]");

        array.push_back(SJSON(1.5));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15,123,1.5]");

        array.push_back(SJSON("asdf"));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15,123,1.5,\"asdf\"]");
    }

    void nestedArray()
    {
        SJSON outer(SJSON::JSON_ARRAY);
        SJSON inner(SJSON::JSON_ARRAY);

        outer.push_back(SJSON(true));
        inner.push_back(SJSON(3));
        inner.push_back(2);
        inner.push_back(SJSON(1));
        outer.push_back(inner);
        outer.push_back(false);

        EXPECT_EQUAL(outer.serialize(), "[true,[3,2,1],false]");
    }


    void simpleObject()
    {
        SJSON doc(SJSON::JSON_OBJECT);

        EXPECT_EQUAL(doc.serialize(), "{}");
        doc["a"] = SJSON(3);
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3}");
        doc["b"] = SJSON();
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null}");
        doc["c"] = -3;
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3}");
        doc["d"] = SJSON(false);
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3,\"d\":false}");
        doc["d"] = true;
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3,\"d\":true}");
        doc["e"] = SJSON("test");
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3,\"d\":true,\"e\":\"test\"}");
    }

    void nestedObject()
    {
        SJSON outer(SJSON::JSON_OBJECT);
        SJSON inner(SJSON::JSON_OBJECT);

        outer["a"] = SJSON(true);
        inner["a"] = 3;
        outer["b"] = inner; // inner is passed to outer by copy
        inner["b"] = "This should not be in outer";

        EXPECT_EQUAL(inner.serialize(), "{\"a\":3,\"b\":\"This should not be in outer\"}");
        EXPECT_EQUAL(outer.serialize(), "{\"a\":true,\"b\":{\"a\":3}}");
    }

    void invalidIndex()
    {
        SJSON array(SJSON::JSON_ARRAY);

        EXPECT_EQUAL(array.size(), 0);
        EXPECT_THROW(array[0], std::out_of_range);
    }

    void invalidKey()
    {
        const SJSON immutableObject(SJSON::JSON_OBJECT);
        EXPECT_EQUAL(immutableObject.size(), 0);
        EXPECT_THROW(immutableObject["asdf"], std::out_of_range);

        SJSON mutableObject(SJSON::JSON_OBJECT);
        EXPECT_EQUAL(mutableObject.size(), 0);
        EXPECT_NO_THROW(mutableObject["asdf"] = 1);
        EXPECT_EQUAL(mutableObject.size(), 1);

    }

} __JSONValueTest;
