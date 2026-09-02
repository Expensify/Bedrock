#include <test/tests/JSONUtilsTest.h>

#include <libstuff/JSON/Utils.h>
#include <libstuff/libstuff.h>

namespace
{
const set<string> SENSITIVE_KEYS = {"ssn"};
}

JSONUtilsTest::JSONUtilsTest() : tpunit::TestFixture("JSONUtils",
                                                     TEST(JSONUtilsTest::mergeObjectsOverwritesRightOrder),
                                                     TEST(JSONUtilsTest::mergeObjectsHandlesLeftNonObject),
                                                     TEST(JSONUtilsTest::mergeObjectsHandlesRightNonObject),
                                                     TEST(JSONUtilsTest::mergeObjectsBothNonObjectsToEmptyObject),
                                                     TEST(JSONUtilsTest::mergeDeepWithSQLiteBehavior),
                                                     TEST(JSONUtilsTest::applyJSONMergePatchNonObjectPatchReplaces),
                                                     TEST(JSONUtilsTest::applyJSONMergePatchNonObjectExistingTreatedAsEmptyObject),
                                                     TEST(JSONUtilsTest::mergeObjectJSON),
                                                     TEST(JSONUtilsTest::stripOutFields),
                                                     TEST(JSONUtilsTest::removeObjectKeysWithNullValues),
                                                     TEST(JSONUtilsTest::containAnyKeys),
                                                     TEST(JSONUtilsTest::getFirstString),
                                                     TEST(JSONUtilsTest::parseOrDefault),
                                                     TEST(JSONUtilsTest::sanitizeJSONStringForTransportStripsControlBytes),
                                                     TEST(JSONUtilsTest::sanitizeJSONStringForTransportPreservesValidUTF8),
                                                     TEST(JSONUtilsTest::sanitizeJSONStringForTransportRejectsInvalidMultiByte),
                                                     TEST(JSONUtilsTest::parseJSONPath)
)
{
}

void JSONUtilsTest::mergeObjectsOverwritesRightOrder()
{
    JSON::Value left = JSON::Value::parse(R"({"a":1,"b":1})");
    JSON::Value right = JSON::Value::parse(R"({"b":2,"c":3})");
    JSON::Value merged = left;
    merged.mergeDeep(right, true);
    ASSERT_TRUE(merged.isObject());
    ASSERT_TRUE(merged.hasMember("a"));
    ASSERT_TRUE(merged.hasMember("b"));
    ASSERT_TRUE(merged.hasMember("c"));
    ASSERT_EQUAL(merged["a"].getInt(), 1);
    ASSERT_EQUAL(merged["b"].getInt(), 2);
    ASSERT_EQUAL(merged["c"].getInt(), 3);
}

void JSONUtilsTest::mergeObjectsHandlesLeftNonObject()
{
    JSON::Value left = JSON::Value("not-an-object");
    JSON::Value right = JSON::Value::parse(R"({"x":10})");
    JSON::Value merged = left.isObject() ? left : JSON::Utils::EMPTY_OBJECT;
    merged.mergeDeep(right.isObject() ? right : JSON::Utils::EMPTY_OBJECT, true);
    ASSERT_TRUE(merged.isObject());
    ASSERT_TRUE(merged.hasMember("x"));
    ASSERT_EQUAL(merged["x"].getInt(), 10);
}

void JSONUtilsTest::mergeObjectsHandlesRightNonObject()
{
    JSON::Value left = JSON::Value::parse(R"({"x":10})");
    JSON::Value right = JSON::Value(123);
    JSON::Value merged = left.isObject() ? left : JSON::Utils::EMPTY_OBJECT;
    merged.mergeDeep(right.isObject() ? right : JSON::Utils::EMPTY_OBJECT, true);
    ASSERT_TRUE(merged.isObject());
    ASSERT_TRUE(merged.hasMember("x"));
    ASSERT_EQUAL(merged["x"].getInt(), 10);
}

void JSONUtilsTest::mergeObjectsBothNonObjectsToEmptyObject()
{
    JSON::Value left = JSON::Value(123);
    JSON::Value right = JSON::Value("abc");
    JSON::Value merged = left.isObject() ? left : JSON::Utils::EMPTY_OBJECT;
    merged.mergeDeep(right.isObject() ? right : JSON::Utils::EMPTY_OBJECT, true);
    ASSERT_TRUE(merged.isObject());
    ASSERT_EQUAL(merged.size(), 0u);
}

string JSONUtilsTest::getOldObject()
{
    return "{"
           "  \"cookie\":\"COOKIE1\","
           "  \"aNullObject\":null,"
           "  \"anEmptyObject\":{},"
           "  \"aNonEmptyObject\":{\"a\":\"b\"},"
           "  \"aNonEmptyArray\":[\"a\",\"b\"],"
           "  \"aNonEmptyString\":\"hola\","
           "  \"questions\":{"
           "    \"QUESTION1\":\"ANSWER1\","
           "    \"QUESTION2\":\"ANSWER2\","
           "    \"QUESTION4\":\"ANSWER4\","
           "    \"QUESTION5\":null"
           "  },"
           "  \"nested1\":{"
           "    \"nested2\":{"
           "      \"nested3a\":{"
           "        \"anArray\":[],"
           "        \"aString\":\"string1\""
           "      }"
           "    }"
           "  }"
           "}";
}

string JSONUtilsTest::getNewObject()
{
    return "{"
           "  \"cookie\":\"COOKIE2\","
           "  \"aNullObject\":{},"
           "  \"anEmptyObject\":null,"
           "  \"aNonEmptyObject\":{},"
           "  \"aNonEmptyArray\":[],"
           "  \"aNonEmptyString\":\"\","
           "  \"questions\":{"
           "    \"QUESTION1\":\"NEWANSWER\","
           "    \"QUESTION3\":\"ANSWER3\","
           "    \"QUESTION4\":null,"
           "    \"QUESTION5\":\"ANSWER5\""
           "  },"
           "  \"nested1\":{"
           "    \"nested2\":{"
           "      \"nested3b\":{"
           "        \"anArray\":[],"
           "        \"aString\":\"string2\""
           "      }"
           "    }"
           "  }"
           "}";
}

void JSONUtilsTest::assertObjectMerged(const JSON::Value& object)
{
    const JSON::Value questionsObject = object["questions"];
    ASSERT_EQUAL(object["cookie"].getString(), "COOKIE2");
    ASSERT_TRUE(object["questions"].isObject());
    ASSERT_EQUAL(questionsObject["QUESTION1"].getString(), "NEWANSWER");
    ASSERT_EQUAL(questionsObject["QUESTION2"].getString(), "ANSWER2");
    ASSERT_EQUAL(questionsObject["QUESTION3"].getString(), "ANSWER3");
    ASSERT_EQUAL(questionsObject["QUESTION5"].getString(), "ANSWER5");
    ASSERT_TRUE(object["nested1"]["nested2"]["nested3a"]["anArray"].isArray());
    ASSERT_EQUAL(object["nested1"]["nested2"]["nested3a"]["aString"].getString(), "string1");
    ASSERT_TRUE(object["nested1"]["nested2"]["nested3b"]["anArray"].isArray());
    ASSERT_EQUAL(object["nested1"]["nested2"]["nested3b"]["aString"].getString(), "string2");
    ASSERT_TRUE(object["aNullObject"].isObject() && !object["aNullObject"].size());
    ASSERT_TRUE(object["aNonEmptyObject"].isObject() && object["aNonEmptyObject"].size() == 1);
}

void JSONUtilsTest::mergeDeepWithSQLiteBehavior()
{
    // Test 1: Null values should DELETE keys when useSQLiteMergeBehavior=true
    JSON::Value obj1 = JSON::Value::parse(R"({"a":1,"b":2,"c":3})");
    obj1.mergeDeep(JSON::Value::parse(R"({"b":null})"), true);
    ASSERT_TRUE(obj1.hasMember("a"));
    ASSERT_FALSE(obj1.hasMember("b"));
    ASSERT_TRUE(obj1.hasMember("c"));

    // Test 2: Null values should be PRESERVED when useSQLiteMergeBehavior=false
    JSON::Value obj2 = JSON::Value::parse(R"({"a":1,"b":2,"c":3})");
    obj2.mergeDeep(JSON::Value::parse(R"({"b":null})"), false);
    ASSERT_TRUE(obj2.hasMember("a"));
    ASSERT_TRUE(obj2.hasMember("b"));
    ASSERT_TRUE(obj2["b"].isNull());
    ASSERT_TRUE(obj2.hasMember("c"));

    // Test 3: Arrays should be REPLACED when useSQLiteMergeBehavior=true
    JSON::Value obj3 = JSON::Value::parse(R"({"arr":[1,2,3]})");
    obj3.mergeDeep(JSON::Value::parse(R"({"arr":[4,5]})"), true);
    ASSERT_EQUAL(obj3["arr"].size(), 2u);
    ASSERT_EQUAL(obj3["arr"][0].getInt(), 4);
    ASSERT_EQUAL(obj3["arr"][1].getInt(), 5);

    // Test 4: Arrays should be CONCATENATED when useSQLiteMergeBehavior=false
    JSON::Value obj4 = JSON::Value::parse(R"({"arr":[1,2,3]})");
    obj4.mergeDeep(JSON::Value::parse(R"({"arr":[4,5]})"), false);
    ASSERT_EQUAL(obj4["arr"].size(), 5u);
    ASSERT_EQUAL(obj4["arr"][0].getInt(), 1);
    ASSERT_EQUAL(obj4["arr"][1].getInt(), 2);
    ASSERT_EQUAL(obj4["arr"][2].getInt(), 3);
    ASSERT_EQUAL(obj4["arr"][3].getInt(), 4);
    ASSERT_EQUAL(obj4["arr"][4].getInt(), 5);

    // Test 5: Nested null deletion with useSQLiteMergeBehavior=true
    JSON::Value obj5 = JSON::Value::parse(R"({"nested":{"a":1,"b":2}})");
    obj5.mergeDeep(JSON::Value::parse(R"({"nested":{"b":null,"c":3}})"), true);
    ASSERT_TRUE(obj5["nested"].hasMember("a"));
    ASSERT_FALSE(obj5["nested"].hasMember("b"));
    ASSERT_TRUE(obj5["nested"].hasMember("c"));
}

void JSONUtilsTest::parseOrDefault()
{
    JSON::Value fallback(JSON::OBJECT);
    fallback["x"] = 1;

    ASSERT_TRUE(JSON::Utils::parseOrDefault("", fallback).isObject());
    ASSERT_TRUE(JSON::Utils::parseOrDefault("", fallback).hasMember("x"));
    ASSERT_EQUAL(JSON::Utils::parseOrDefault("", fallback)["x"].getInt(), 1);

    ASSERT_TRUE(JSON::Utils::parseOrDefault("{not json", fallback).isObject());
    ASSERT_TRUE(JSON::Utils::parseOrDefault("{not json", fallback).hasMember("x"));

    const JSON::Value parsed = JSON::Utils::parseOrDefault(R"({"y":2})", fallback);
    ASSERT_TRUE(parsed.isObject());
    ASSERT_TRUE(parsed.hasMember("y"));
    ASSERT_FALSE(parsed.hasMember("x"));
}

void JSONUtilsTest::applyJSONMergePatchNonObjectPatchReplaces()
{
    // RFC 7396: a non-object patch replaces the document (SQLite JSON_PATCH matches this).
    JSON::Value arrayPatch(JSON::ARRAY);
    arrayPatch.push_back(JSON::Value(1));
    JSON::Value merged = JSON::Utils::applyJSONMergePatch(R"({"a":1})", arrayPatch);
    ASSERT_TRUE(merged.isArray());
    ASSERT_EQUAL(merged.size(), 1u);
    ASSERT_EQUAL(merged[0].getInt(), 1);

    JSON::Value scalarPatch(42);
    merged = JSON::Utils::applyJSONMergePatch(R"({"a":1})", scalarPatch);
    ASSERT_TRUE(merged.isInt());
    ASSERT_EQUAL(merged.getInt(), 42);
}

void JSONUtilsTest::applyJSONMergePatchNonObjectExistingTreatedAsEmptyObject()
{
    // SQLite JSON_PATCH: object patch against a non-object document applies as if the document were {}.
    JSON::Value patch = JSON::Value::parse(R"({"k":"v"})");

    JSON::Value merged = JSON::Utils::applyJSONMergePatch("[]", patch);
    ASSERT_TRUE(merged.isObject());
    ASSERT_EQUAL(merged["k"].getString(), "v");

    merged = JSON::Utils::applyJSONMergePatch("1", patch);
    ASSERT_TRUE(merged.isObject());
    ASSERT_EQUAL(merged["k"].getString(), "v");

    merged = JSON::Utils::applyJSONMergePatch(R"("s")", patch);
    ASSERT_TRUE(merged.isObject());
    ASSERT_EQUAL(merged["k"].getString(), "v");
}

void JSONUtilsTest::mergeObjectJSON()
{
    JSON::Value object = JSON::Value::parse(getOldObject());
    object.mergeDeep(JSON::Value::parse(getNewObject()), true);
    assertObjectMerged(object);

    // Make sure we can erase or empty existing values
    ASSERT_FALSE(object["questions"].hasMember("QUESTION4"));
    ASSERT_FALSE(object.hasMember("anEmptyObject"));
    ASSERT_TRUE(object["aNonEmptyArray"].isArray() && !object["aNonEmptyArray"].size());
    ASSERT_TRUE(object["aNonEmptyString"].isString() && object["aNonEmptyString"].getString().empty());
}

void JSONUtilsTest::removeObjectKeysWithNullValues()
{
    JSON::Value flat = JSON::Value::parse(R"({"a":1,"b":null,"c":"x"})");
    JSON::Utils::removeObjectKeysWithNullValues(flat);
    ASSERT_TRUE(flat.hasMember("a"));
    ASSERT_FALSE(flat.hasMember("b"));
    ASSERT_TRUE(flat.hasMember("c"));

    JSON::Value nested = JSON::Value::parse(R"({"outer":{"removeMe":null,"keep":2},"top":null})");
    JSON::Utils::removeObjectKeysWithNullValues(nested);
    ASSERT_FALSE(nested.hasMember("top"));
    ASSERT_TRUE(nested["outer"].isObject());
    ASSERT_TRUE(nested["outer"].hasMember("keep"));
    ASSERT_FALSE(nested["outer"].hasMember("removeMe"));

    JSON::Value withArray = JSON::Value::parse(R"({"items":[{"x":null,"y":1},null]})");
    JSON::Utils::removeObjectKeysWithNullValues(withArray);
    ASSERT_TRUE(withArray["items"].isArray());
    ASSERT_EQUAL(withArray["items"].size(), 2u);
    ASSERT_TRUE(withArray["items"][0].isObject());
    ASSERT_FALSE(withArray["items"][0].hasMember("x"));
    ASSERT_TRUE(withArray["items"][0].hasMember("y"));
    ASSERT_TRUE(withArray["items"][1].isNull());

    JSON::Value inPlace(JSON::OBJECT);
    inPlace["nilField"] = JSON::Value(JSON::NIL);
    inPlace["n"] = 3;
    JSON::Utils::removeObjectKeysWithNullValues(inPlace);
    ASSERT_FALSE(inPlace.hasMember("nilField"));
    ASSERT_EQUAL(inPlace["n"].getInt(), 3);

    JSON::Value scalar(42);
    JSON::Utils::removeObjectKeysWithNullValues(scalar);
    ASSERT_TRUE(scalar.isInt());
    ASSERT_EQUAL(scalar.getInt(), 42);
}

void JSONUtilsTest::stripOutFields()
{
    JSON::Value parentObject = JSON::Value::parse("{\"ssn\":\"1234\",\"layer1\":{\"ssn\":\"1234\",\"layer2\":{\"ssn\":\"1234\",\"layer3\":{\"ssn\":\"1234\",\"transactions\":[]},\"arrayField\":[{\"ssn\":\"1234\",\"test\":1}]}}}");
    JSON::Utils::stripOutFields(parentObject, SENSITIVE_KEYS);
    ASSERT_FALSE(parentObject["layer1"]["layer2"]["layer3"].hasMember("ssn"));
    ASSERT_FALSE(parentObject["layer1"]["layer2"].hasMember("ssn"));
    ASSERT_FALSE(parentObject["layer1"].hasMember("ssn"));
    ASSERT_FALSE(parentObject.hasMember("ssn"));
    ASSERT_TRUE(parentObject["layer1"]["layer2"].hasMember("arrayField"));
    ASSERT_TRUE(parentObject["layer1"]["layer2"]["arrayField"].isArray());
    ASSERT_EQUAL(parentObject["layer1"]["layer2"]["arrayField"].size(), 1);
    ASSERT_FALSE(parentObject["layer1"]["layer2"]["arrayField"][0].hasMember("ssn"));
    ASSERT_TRUE(parentObject["layer1"]["layer2"]["arrayField"][0].hasMember("test"));
    ASSERT_TRUE(parentObject["layer1"]["layer2"]["layer3"].hasMember("transactions"));
    JSON::Utils::stripOutFields(parentObject, {"transactions"});
    ASSERT_FALSE(parentObject["layer1"]["layer2"]["layer3"].hasMember("transactions"));
}

void JSONUtilsTest::containAnyKeys()
{
    // Test that the function returns true with sensitive keys at different levels
    JSON::Value parentObject = JSON::Value::parse("{\"ssn\":\"1234\",\"layer1\":{\"ssn\":\"1234\",\"layer2\":{\"ssn\":\"1234\",\"layer3\":{\"ssn\":\"1234\"},\"arrayField\":[{\"ssn\":\"1234\",\"test\":1}]}}}");
    ASSERT_TRUE(JSON::Utils::containAnyKeys(parentObject, SENSITIVE_KEYS));

    parentObject = JSON::Value::parse("{\"field\":\"1234\",\"layer1\":{\"ssn\":\"1234\",\"layer2\":{\"ssn\":\"1234\",\"layer3\":{\"ssn\":\"1234\"},\"arrayField\":[{\"ssn\":\"1234\",\"test\":1}]}}}");
    ASSERT_TRUE(JSON::Utils::containAnyKeys(parentObject, SENSITIVE_KEYS));

    parentObject = JSON::Value::parse("{\"field\":\"1234\",\"layer1\":{\"field\":\"1234\",\"layer2\":{\"ssn\":\"1234\",\"layer3\":{\"ssn\":\"1234\"},\"arrayField\":[{\"ssn\":\"1234\",\"test\":1}]}}}");
    ASSERT_TRUE(JSON::Utils::containAnyKeys(parentObject, SENSITIVE_KEYS));

    parentObject = JSON::Value::parse("{\"field\":\"1234\",\"layer1\":{\"field\":\"1234\",\"layer2\":{\"field\":\"1234\",\"layer3\":{\"ssn\":\"1234\"},\"arrayField\":[{\"ssn\":\"1234\",\"test\":1}]}}}");
    ASSERT_TRUE(JSON::Utils::containAnyKeys(parentObject, SENSITIVE_KEYS));

    parentObject = JSON::Value::parse("{\"field\":\"1234\",\"layer1\":{\"field\":\"1234\",\"layer2\":{\"field\":\"1234\",\"layer3\":{\"field\":\"1234\"},\"arrayField\":[{\"ssn\":\"1234\",\"test\":1}]}}}");
    ASSERT_TRUE(JSON::Utils::containAnyKeys(parentObject, SENSITIVE_KEYS));

    // No more sensitive keys so it should return false
    parentObject = JSON::Value::parse("{\"field\":\"1234\",\"layer1\":{\"field\":\"1234\",\"layer2\":{\"field\":\"1234\",\"layer3\":{\"field\":\"1234\"},\"arrayField\":[{\"field\":\"1234\",\"test\":1}]}}}");
    ASSERT_FALSE(JSON::Utils::containAnyKeys(parentObject, SENSITIVE_KEYS));
}

void JSONUtilsTest::getFirstString()
{
    JSON::Value obj = JSON::Value::parse("{\"aKey\":\"aString\"}");
    ASSERT_EQUAL(JSON::Utils::getFirstString(obj, "aKey"), "aString");
    obj = JSON::Value::parse("{\"aKey\":[\"aStringInArray\"]}");
    ASSERT_EQUAL(JSON::Utils::getFirstString(obj, "aKey"), "aStringInArray");
    ASSERT_EQUAL(JSON::Utils::getFirstString(obj, "anotherKey"), "");
}

void JSONUtilsTest::sanitizeJSONStringForTransportStripsControlBytes()
{
    // ASCII control bytes (except \t \n \r) should be stripped
    string input = "hello\x01world\x7F";
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport(input), "helloworld");

    // Whitespace preserved
    string ws = "hello\tworld\nfoo\rbar";
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport(ws), ws);

    // Null byte stripped
    string nullInput = string("ab\0cd", 5);
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport(nullInput), "abcd");
}

void JSONUtilsTest::sanitizeJSONStringForTransportPreservesValidUTF8()
{
    // 2-byte: copyright sign (C2 A9)
    string twoBytes = "\xC2\xA9";
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport(twoBytes), twoBytes);

    // 3-byte: check mark (E2 9C 93)
    string threeBytes = "\xE2\x9C\x93";
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport(threeBytes), threeBytes);

    // 4-byte: grinning face emoji (F0 9F 98 80)
    string fourBytes = "\xF0\x9F\x98\x80";
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport(fourBytes), fourBytes);

    // Mixed valid content
    string mixed = "price: \xC2\xA3" "100 \xE2\x9C\x93 done";
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport(mixed), mixed);
}

void JSONUtilsTest::sanitizeJSONStringForTransportRejectsInvalidMultiByte()
{
    // Overlong 2-byte for ASCII (C0 AF)
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport("\xC0\xAF"), "");

    // Surrogate (ED A0 80 = U+D800)
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport("\xED\xA0\x80"), "");

    // Above U+10FFFF (F4 90 80 80)
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport("\xF4\x90\x80\x80"), "");

    // Truncated sequence at end of string
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport("abc\xC2"), "abc");

    // Invalid continuation byte
    ASSERT_EQUAL(JSON::Utils::sanitizeJSONStringForTransport("\xC2\x00"), "");
}

void JSONUtilsTest::parseJSONPath()
{
    // Segments are joined with '|' (a character that never appears in these paths) so the exact
    // split can be asserted with a single string comparison.

    // A plain dotted path splits on every dot
    ASSERT_EQUAL(SComposeList(JSON::Utils::parseJSONPath("$.a.b.c"), "|"), "$|a|b|c");

    // A single key with no dots is returned as-is
    ASSERT_EQUAL(SComposeList(JSON::Utils::parseJSONPath("foo"), "|"), "foo");

    // A double-quoted segment is a single key: dots inside it are preserved and the quotes stripped
    ASSERT_EQUAL(SComposeList(JSON::Utils::parseJSONPath("$.batata.\"email.com\""), "|"), "$|batata|email.com");

    // The Uber employee path shape: the whole email (dots and '@' included) is one key
    ASSERT_EQUAL(SComposeList(JSON::Utils::parseJSONPath("receiptPartners.uber.employees.\"john.doe@uber.com\""), "|"),
                 "receiptPartners|uber|employees|john.doe@uber.com");

    // Multiple quoted segments and a trailing unquoted key are each their own part
    ASSERT_EQUAL(SComposeList(JSON::Utils::parseJSONPath("$.\"a.b\".\"c.d\".status"), "|"), "$|a.b|c.d|status");

    // An escaped quote inside a quoted segment is kept literally
    ASSERT_EQUAL(SComposeList(JSON::Utils::parseJSONPath("a.\"b\\\"c\""), "|"), "a|b\"c");
}

JSONUtilsTest __JSONUtilsTest;
