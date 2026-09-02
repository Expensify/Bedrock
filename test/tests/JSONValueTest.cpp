#include <libstuff/JSON/Value.h>
#include <libstuff/libstuff.h>
#include <test/lib/tpunit++.hpp>

struct JSONValueTest : tpunit::TestFixture
{
    JSONValueTest() : tpunit::TestFixture("JSONValue",
                                          TEST(JSONValueTest::setBool),
                                          TEST(JSONValueTest::setBool2),
                                          TEST(JSONValueTest::setInt),
                                          TEST(JSONValueTest::setInt2),
                                          TEST(JSONValueTest::setBigInt),
                                          TEST(JSONValueTest::setDouble),
                                          TEST(JSONValueTest::setString),
                                          TEST(JSONValueTest::ctorNull),
                                          TEST(JSONValueTest::ctorBool),
                                          TEST(JSONValueTest::ctorBool2),
                                          TEST(JSONValueTest::ctorInt),
                                          TEST(JSONValueTest::ctorInt2),
                                          TEST(JSONValueTest::ctorBigInt),
                                          TEST(JSONValueTest::ctorDouble),
                                          TEST(JSONValueTest::ctorString),
                                          TEST(JSONValueTest::simpleArray),
                                          TEST(JSONValueTest::emptyArrayAccess),
                                          TEST(JSONValueTest::nestedArray),
                                          TEST(JSONValueTest::simpleObject),
                                          TEST(JSONValueTest::nestedObject),
                                          TEST(JSONValueTest::invalidIndex),
                                          TEST(JSONValueTest::invalidKey),
                                          TEST(JSONValueTest::concatenateArrays),
                                          TEST(JSONValueTest::arbitraryDS),
                                          TEST(JSONValueTest::extractString),
                                          TEST(JSONValueTest::merge),
                                          TEST(JSONValueTest::getAtPath),
                                          TEST(JSONValueTest::serializeEmptyString),
                                          TEST(JSONValueTest::castOperators),
                                          TEST(JSONValueTest::operatorThrows),
                                          TEST(JSONValueTest::testSelfAssignment),
                                          TEST(JSONValueTest::hasIndex),
                                          TEST(JSONValueTest::convenienceCasts),
                                          TEST(JSONValueTest::operators),
                                          TEST(JSONValueTest::shallowCopyValue),
                                          TEST(JSONValueTest::shallowCopyToKey),
                                          TEST(JSONValueTest::preventTypeChangeInShallowCopy),
                                          TEST(JSONValueTest::getBoolFromBinaryIntOrBool))
    {
    }

    void setBool()
    {
        JSON::Value value;
        value = true;
        EXPECT_EQUAL(value.type(), JSON::BOOL);
        EXPECT_EQUAL(value.serialize(), "true");
    }

    void setBool2()
    {
        JSON::Value value;
        value = false;
        EXPECT_EQUAL(value.type(), JSON::BOOL);
        EXPECT_EQUAL(value.serialize(), "false");
    }

    void setInt()
    {
        JSON::Value value;
        value = -15;
        EXPECT_EQUAL(value.type(), JSON::INT);
        EXPECT_FALSE(value.isHuge());
        EXPECT_EQUAL(value.serialize(), "-15");
        EXPECT_THROW(value.getUint(), JSON::TypeError);
    }

    void setInt2()
    {
        JSON::Value value;
        value = 123;
        EXPECT_EQUAL(value.type(), JSON::INT);
        EXPECT_FALSE(value.isHuge());
        EXPECT_EQUAL(value.serialize(), "123");
        EXPECT_EQUAL(value.getInt(), 123);
        EXPECT_EQUAL(value.getUint(), 123);
    }

    void setBigInt()
    {
        JSON::Value value;
        value = 9223372036854775808ULL;
        EXPECT_EQUAL(value.type(), JSON::INT);
        EXPECT_TRUE(value.isHuge());
        EXPECT_EQUAL(value.serialize(), "9223372036854775808");
        EXPECT_THROW(value.getInt(), JSON::TypeError);
    }

    void setDouble()
    {
        JSON::Value value;
        value = 1.5;
        EXPECT_EQUAL(value.type(), JSON::FLOAT);
        EXPECT_EQUAL(value.serialize(), "1.5");
    }

    void setString()
    {
        JSON::Value value;
        value = "test";
        EXPECT_EQUAL(value.type(), JSON::STRING);
        EXPECT_EQUAL(value.serialize(), "\"test\"");
    }

    void ctorNull()
    {
        const JSON::Value value;
        EXPECT_EQUAL(value.type(), JSON::NIL);
        EXPECT_EQUAL(value.serialize(), "null");
    }

    void ctorBool()
    {
        const JSON::Value value(true);
        EXPECT_EQUAL(value.type(), JSON::BOOL);
        EXPECT_EQUAL(value.serialize(), "true");
    }

    void ctorBool2()
    {
        const JSON::Value value(false);
        EXPECT_EQUAL(value.type(), JSON::BOOL);
        EXPECT_EQUAL(value.serialize(), "false");
    }

    void ctorInt()
    {
        const JSON::Value value(-15);
        EXPECT_EQUAL(value.type(), JSON::INT);
        EXPECT_EQUAL(value.serialize(), "-15");
    }

    void ctorInt2()
    {
        const JSON::Value value(123);
        EXPECT_EQUAL(value.type(), JSON::INT);
        EXPECT_EQUAL(value.serialize(), "123");
    }

    void ctorBigInt()
    {
        const JSON::Value value(9223372036854775808ULL);
        EXPECT_EQUAL(value.type(), JSON::INT);
        EXPECT_TRUE(value.isHuge());
        EXPECT_EQUAL(value.serialize(), "9223372036854775808");
        EXPECT_THROW(value.getInt(), JSON::TypeError);
    }

    void ctorDouble()
    {
        const JSON::Value value(1.5);
        EXPECT_EQUAL(value.type(), JSON::FLOAT);
        EXPECT_EQUAL(value.serialize(), "1.5");
    }

    void ctorString()
    {
        const JSON::Value value("test");
        EXPECT_EQUAL(value.type(), JSON::STRING);
        EXPECT_EQUAL(value.serialize(), "\"test\"");
    }

    void simpleArray()
    {
        JSON::Value array(JSON::ARRAY);

        EXPECT_EQUAL(array.serialize(), "[]");

        array.push_back(JSON::Value(true));
        EXPECT_EQUAL(array.serialize(), "[true]");

        array.push_back(JSON::Value(false));
        EXPECT_EQUAL(array.serialize(), "[true,false]");

        array.push_back(JSON::Value());
        EXPECT_EQUAL(array.serialize(), "[true,false,null]");

        array.push_back(JSON::Value(-15));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15]");

        array.push_back(JSON::Value(123));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15,123]");

        array.push_back(JSON::Value(1.5));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15,123,1.5]");

        array.push_back(JSON::Value("asdf"));
        EXPECT_EQUAL(array.serialize(), "[true,false,null,-15,123,1.5,\"asdf\"]");
    }

    void emptyArrayAccess()
    {
        // Given an empty array value
        JSON::Value array(JSON::ARRAY);

        // When back is called
        // Then a JSON::NotFound is thrown instead of causing a crash
        ASSERT_THROW(array.back(), JSON::NotFound);

        // Same for indexing
        ASSERT_THROW(array[0], JSON::NotFound);

        // Given a non-empty array
        array = JSON::Value(list<string>{"1", "2", "3", "4", "5"});

        // When we access items then we get the expected results
        ASSERT_EQUAL("5", array.back().getString());
        ASSERT_EQUAL("5", array[4].getString());

        // When an item is accessed by an out of range index
        // Then a JSON::NotFound is thrown
        ASSERT_THROW(array[9], JSON::NotFound);
    }

    void nestedArray()
    {
        JSON::Value outer(JSON::ARRAY);
        JSON::Value inner(JSON::ARRAY);

        outer.push_back(JSON::Value(true));
        inner.push_back(JSON::Value(3));
        inner.push_back(2);
        inner.push_back(JSON::Value(1));
        outer.push_back(inner);
        outer.push_back(false);

        EXPECT_EQUAL(outer.serialize(), "[true,[3,2,1],false]");
    }

    void simpleObject()
    {
        JSON::Value doc(JSON::OBJECT);

        EXPECT_EQUAL(doc.serialize(), "{}");
        doc["a"] = JSON::Value(3);
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3}");
        doc["b"] = JSON::Value();
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null}");
        doc["c"] = -3;
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3}");
        doc["d"] = JSON::Value(false);
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3,\"d\":false}");
        doc["d"] = true;
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3,\"d\":true}");
        doc["e"] = JSON::Value("test");
        EXPECT_EQUAL(doc.serialize(), "{\"a\":3,\"b\":null,\"c\":-3,\"d\":true,\"e\":\"test\"}");
    }

    void nestedObject()
    {
        JSON::Value outer(JSON::OBJECT);
        JSON::Value inner(JSON::OBJECT);

        outer["a"] = JSON::Value(true);
        inner["a"] = 3;
        outer["b"] = inner; // inner is passed to outer by copy
        inner["b"] = "This should not be in outer";

        EXPECT_EQUAL(inner.serialize(), "{\"a\":3,\"b\":\"This should not be in outer\"}");
        EXPECT_EQUAL(outer.serialize(), "{\"a\":true,\"b\":{\"a\":3}}");
    }

    void invalidIndex()
    {
        JSON::Value array(JSON::ARRAY);

        EXPECT_EQUAL(array.size(), 0);
        EXPECT_THROW(array[0], JSON::NotFound);
    }

    void invalidKey()
    {
        const JSON::Value immutableObject(JSON::OBJECT);
        EXPECT_EQUAL(immutableObject.size(), 0);
        EXPECT_THROW(immutableObject["asdf"], JSON::NotFound);

        JSON::Value mutableObject(JSON::OBJECT);
        EXPECT_EQUAL(mutableObject.size(), 0);
        EXPECT_NO_THROW(mutableObject["asdf"] = 1);
        EXPECT_EQUAL(mutableObject.size(), 1);
    }

    void extractString()
    {
        JSON::Value object(JSON::OBJECT);
        EXPECT_EQUAL(object.size(), 0);
        object["index"] = "1234";
        EXPECT_EQUAL(object.size(), 1);
        EXPECT_EQUAL(object.extractStringWithDefault("index"), "1234");
        EXPECT_EQUAL(object.size(), 0);

        // Should not throw if key does not exist
        EXPECT_NO_THROW(object.extractStringWithDefault("index"));

        // Should return the defaultValue parameter
        EXPECT_EQUAL(object.extractStringWithDefault("index", "default-value"), "default-value");

        // Should not throw if using function with default even if it's an array object
        JSON::Value arrayObj(JSON::ARRAY);
        EXPECT_NO_THROW(arrayObj.extractStringWithDefault("index"));
    }

    void concatenateArrays()
    {
        JSON::Value a1(vector<JSON::Value>{"A", "B", "C"});
        JSON::Value a2(vector<JSON::Value>{1, 2, 3});
        JSON::Value concatenated(vector<JSON::Value>{"A", "B", "C", 1, 2, 3});

        a1.arrayInsert(a1.arrayEnd(), a2.arrayBegin(), a2.arrayEnd());
        ASSERT_EQUAL(a1, concatenated);
    }

    void arbitraryDS()
    {
        map<string, map<string, set<int64_t>>> flosMagicDataStructure = {
            {"1234", {
                 // Duplicate 3s on purpose, they should get de-duped by the `set` constructor.
                 {"sharedReportIDs", {1, 2, 3, 3, 3}},
                 {"unsharedReportIDs", {4, 5, 6}},
             }},
            {"5678", {
                 {"sharedReportIDs", {42}},
                 {"unsharedReportIDs", {}},
             }}
        };

        map<int64_t, map<string, set<int64_t>>> flosMagickerDataStructure = {
            {1234, {
                 // Duplicate 3s on purpose, they should get de-duped by the `set` constructor.
                 {"sharedReportIDs", {1, 2, 3, 3, 3}},
                 {"unsharedReportIDs", {4, 5, 6}},
             }},
            {5678, {
                 {"sharedReportIDs", {42}},
                 {"unsharedReportIDs", {}},
             }}
        };

        JSON::Value reference = JSON::Value::parse("{"
            "\"1234\": {"
            "    \"sharedReportIDs\": [1, 2, 3],"
            "    \"unsharedReportIDs\": [4, 5, 6]"
            "},"
            "\"5678\": {"
            "    \"sharedReportIDs\": [42],"
            "    \"unsharedReportIDs\": []"
            "}"
        "}");

        // Build our JSON from our data structure above.
        JSON::Value json1 = JSON::Value::fromDataStructure(flosMagicDataStructure);
        JSON::Value json2 = JSON::Value::fromDataStructure(flosMagickerDataStructure);

        // Compare to our reference value.
        ASSERT_EQUAL(json1, reference);
        ASSERT_EQUAL(json2, reference);
    }

    void merge()
    {
        JSON::Value target({
            {"1234", 1},
            {"5678", 2}
        });
        JSON::Value source({
            {"1234", 111},
            {"4321", 3}
        });

        target.merge(source);

        JSON::Value reference({
            {"1234", 111},
            {"5678", 2},
            {"4321", 3},
        });

        ASSERT_EQUAL(target.serialize(), reference.serialize());

        // Verify existing keys are overriden
        target.merge(JSON::Value({
            {"1234", "123-replaced"},
            {"1235", "new-key"},
        }));

        reference = JSON::Value({
            {"1234", "123-replaced"},
            {"1235", "new-key"},
            {"5678", 2},
            {"4321", 3},
        });
        ASSERT_EQUAL(target.serialize(), reference.serialize());
    }

    void getAtPath()
    {
        JSON::Value obj = JSON::Value({
            {"settings", JSON::Value({
                    {"expensifyCard", JSON::Value({
                            {"program", "US"},
                            {"shouldBill", true}
                })},
                    {"randomKey", 10}
            })},
            {"name", "cardSettings"},
        });

        // Test getting nested string
        JSON::Value program = obj.getValueAtPath({"settings", "expensifyCard", "program"});
        ASSERT_FALSE(program.isNull());
        ASSERT_TRUE(program.isString());
        ASSERT_EQUAL("US", program.getString());

        // Test path doesn't exist
        JSON::Value doesntExist = obj.getValueAtPath({"path", "not", "exist"});
        ASSERT_TRUE(doesntExist.isNull());

        // Test getting nested int
        JSON::Value randomKey = obj.getValueAtPath({"settings", "randomKey"});
        ASSERT_FALSE(randomKey.isNull());
        ASSERT_TRUE(randomKey.isInt());
        ASSERT_EQUAL(10, randomKey.getInt());

        // Test top level
        JSON::Value name = obj.getValueAtPath({"name"});
        ASSERT_FALSE(name.isNull());
        ASSERT_TRUE(name.isString());
        ASSERT_EQUAL("cardSettings", name.getString());

        // Test nested bool
        JSON::Value boolVal = obj.getValueAtPath({"settings", "expensifyCard", "shouldBill"});
        ASSERT_FALSE(boolVal.isNull());
        ASSERT_TRUE(boolVal.isBool());
        ASSERT_EQUAL(true, boolVal.getBool());

        // Test getting value with repeated key.
        JSON::Value test1(JSON::Value::parse("{\"a\":{\"a\":1}}"));
        JSON::Value shouldBe1 = test1.getValueAtPath({"a", "a"});
        ASSERT_EQUAL(shouldBe1.getInt(), 1);
    }

    void serializeEmptyString()
    {
        // Serializing an empty string results in a string which is the empty string: e.g. """", not "" as you might expect
        // This is because it always returns a string which is valid JSON; it can't be empty
        ASSERT_EQUAL(JSON::Value("").serialize(), "\"\"");

        // Instead, you can get the string value directly
        ASSERT_EQUAL(JSON::Value("").getString(), "");
    }

    void castOperators()
    {
        JSON::Value target = JSON::Value::parse("{"
            "\"key1\": 1, "
            "\"key2\": true,"
            "\"key3\": \"string\","
            "\"key4\": 2.65,"
            "\"key5\": [1,2,3]"
        "}");

        uint64_t a = (decltype(a)) target["key1"];
        int64_t b = (decltype(b)) target["key1"];
        bool c = (decltype(c)) target["key2"];
        string d = (decltype(d)) target["key3"];
        double e = (decltype(e)) target["key4"];
        list<int64_t> f = (decltype(f)) target["key5"];

        ASSERT_EQUAL(a, 1);
        ASSERT_EQUAL(b, 1ull);
        ASSERT_EQUAL(c, true);
        ASSERT_EQUAL(d, "string");
        ASSERT_FLOAT_EQUAL(e, 2.65);
        ASSERT_THROW(d = (decltype(d)) target["key1"], JSON::TypeError);

        list<int64_t> compare{1, 2, 3};
        ASSERT_EQUAL(f, compare);

        // Repeat, but with the fill method
        target = JSON::Value::parse("{"
            "\"key1\": 2, "
            "\"key2\": false,"
            "\"key3\": \"word\","
            "\"key4\": 6.52,"
            "\"key5\": [3,2,1]"
        "}");
        target["key1"].fill(a);
        target["key1"].fill(b);
        target["key2"].fill(c);
        target["key3"].fill(d);
        target["key4"].fill(e);
        target["key5"].fill(f);

        ASSERT_EQUAL(a, 2);
        ASSERT_EQUAL(b, 2ull);
        ASSERT_EQUAL(c, false);
        ASSERT_EQUAL(d, "word");
        ASSERT_FLOAT_EQUAL(e, 6.52);
        ASSERT_THROW(target["key1"].fill(d), JSON::TypeError);

        list<int64_t> compare2{3, 2, 1};
        ASSERT_EQUAL(f, compare2);
    }

    void operatorThrows()
    {
        const JSON::Value constTestObject({{"goodValue", JSON::Value("test")}});
        try {
            constTestObject["badValue"].getString();
            FAIL();
        } catch (const JSON::NotFound& e) {
            ASSERT_EQUAL("JSON Error, key not found - 'badValue' method: 'operator[] const&'", string(e.what()));
            PASS();
        }

        const JSON::Value constNotAnObject(JSON::ARRAY);
        try {
            constNotAnObject["badValue"].getString();
            FAIL();
        } catch (const JSON::TypeError& e) {
            ASSERT_EQUAL("JSON Type Error, expected: 'object' actual: 'array' key: 'badValue' method: 'operator[] const&'", string(e.what()));
            PASS();
        }

        JSON::Value stringNotObject("test");
        try {
            stringNotObject["badValue"].getString();
            FAIL();
        } catch (const JSON::TypeError& e) {
            ASSERT_EQUAL("JSON Type Error, expected: 'object' actual: 'string' key: 'badValue' method: 'operator[] &'", string(e.what()));
            PASS();
        }

        const JSON::Value intNotObject(0);
        try {
            intNotObject.hasMember("badValue");
            FAIL();
        } catch (const JSON::TypeError& e) {
            ASSERT_EQUAL("JSON Type Error, expected: 'object' actual: 'int' key: 'badValue' method: 'hasMember'", string(e.what()));
            PASS();
        }

        try {
            intNotObject.size();
            FAIL();
        } catch (const JSON::TypeError& e) {
            ASSERT_EQUAL("JSON Type Error, expected: 'object' or 'array' actual: 'int' method: 'size'", string(e.what()));
            PASS();
        }
    }

    void testSelfAssignment()
    {
        JSON::Value objTest = JSON::Value::parse("{\"stripeResponse\":{\"allowed_source_types\":[\"card\"],\"amount\":49000,\"amount_capturable\":0,\"amount_details\":{\"tip\":{}},\"amou"
        "nt_received\":0,\"application\":null,\"application_fee_amount\":null,\"automatic_payment_methods\":null,\"canceled_at\":null,\"cancellation_reason\":null,\"capture_method\":\"au"
        "tomatic\",\"charges\":{\"data\":[],\"has_more\":false,\"object\":\"list\",\"total_count\":0,\"url\":\"/v1/charges?payment_intent=pi_XXXXXXXXXXXXXXXXXXXXXXXX\"},\"client_secret\""
        ":\"pi_XXXXXXXXXXXXXXXXXXXXXXXX_secret_XXXXXXXXXXXXXXXXXXXXXXXXX\",\"confirmation_method\":\"automatic\",\"created\":1715169719,\"currency\":\"gbp\",\"customer\":\"cus_PUPEfNMJmw"
        "yYPT\",\"description\":\"|true|2024-04|clear\",\"id\":\"pi_XXXXXXXXXXXXXXXXXXXXXXXX\",\"invoice\":null,\"last_payment_error\":null,\"latest_charge\":null,\"livemode\":true,\"met"
        "adata\":{},\"next_action\":{\"redirect_to_url\":{\"return_url\":\"https://secure.expensify.com/partners/stripe/callback.php\",\"url\":\"https://hooks.stripe.com/3d_secure_2/host"
        "ed?merchant=XXXXXXXXXXXXXXXXXXXXX&payment_intent=pi_XXXXXXXXXXXXXXXXXXXXXXXX&payment_intent_client_secret=pi_XXXXXXXXXXXXXXXXXXXXXXXX_secret_XXXXXXXXXXXXXXXXXXXXXXXXX&publishabl"
        "e_key=pk_live_XXXXXXXXXXXXXXXXXXXXXXXX&source=payatt_XXXXXXXXXXXXXXXXXXXXXXXX\"},\"type\":\"redirect_to_url\"},\"next_source_action\":{\"authorize_with_url\":{\"return_url\":\"h"
        "ttps://secure.expensify.com/partners/stripe/callback.php\",\"url\":\"https://hooks.stripe.com/3d_secure_2/hosted?merchant=XXXXXXXXXXXXXXXXXXXXX&payment_intent=pi_XXXXXXXXXXXXX"
        "XXXXXXXXXXX&payment_intent_client_secret=pi_XXXXXXXXXXXXXXXXXXXXXXXX_secret_XXXXXXXXXXXXXXXXXXXXXXXXX&publishable_key=pk_live_XXXXXXXXXXXXXXXXXXXXXXXX&source=payatt_XXXXXXXXXXXX"
        "XXXXXXXXXXXX\"},\"type\":\"authorize_with_url\"},\"object\":\"payment_intent\",\"on_behalf_of\":null,\"payment_method\":\"pm_XXXXXXXXXXXXXXXXXXXXXXXX\",\"payment_method_configur"
        "ation_details\":null,\"payment_method_options\":{\"card\":{\"installments\":null,\"mandate_options\":null,\"network\":null,\"request_three_d_secure\":\"automatic\"}},\"payment_m"
        "ethod_types\":[\"card\"],\"processing\":null,\"receipt_email\":null,\"review\":null,\"setup_future_usage\":\"off_session\",\"shipping\":null,\"source\":null,\"statement_descript"
        "or\":null,\"statement_descriptor_suffix\":null,\"status\":\"requires_source_action\",\"transfer_data\":null,\"transfer_group\":null}}");

        // This copies the stripeResponse value, which is what we expect the whole value to end up as.
        JSON::Value expected = objTest["stripeResponse"];

        // Now we do the self-assignment. In previous (buggy) versions of this, we would get either a segfault, some sort of infinite memory allocation, or the wrong value here.
        objTest = objTest["stripeResponse"];

        // So, if we don't crash and the value is the expected value, we'll say we passed this test.
        ASSERT_EQUAL(expected, objTest);
    }

    void hasIndex()
    {
        JSON::Value testObject(JSON::OBJECT);
        JSON::Value testArray(JSON::ARRAY);

        try {
            testObject.hasIndex(1);
        } catch (const JSON::TypeError& e) {
            ASSERT_EQUAL("JSON Type Error, expected: 'array' actual: 'object' index: '1' method: 'hasIndex'", string(e.what()));
        }

        ASSERT_FALSE(testArray.hasIndex(0));

        testArray.push_back(JSON::Value("pepperoni"));
        ASSERT_TRUE(testArray.hasIndex(0));
        ASSERT_FALSE(testArray.hasIndex(1));

        testArray.push_back(JSON::Value("capicola"));
        ASSERT_TRUE(testArray.hasIndex(0));
        ASSERT_TRUE(testArray.hasIndex(1));
    }

    void convenienceCasts()
    {
        // The following is a single-item object, but doesn't work, the call is ambiguous.
        // auto v = JSON::Value({{"key", "value"}});

        // This would work, but is ugly and annoying.
        // auto v = JSON::Value(map<string, JSON::Value>{{"key", "value"}});

        // This is a prettier, cleaner way to do this.
        auto obj = JSON::Value::object({{"key", "value"}});

        ASSERT_TRUE(obj.isObject());
        ASSERT_EQUAL(obj["key"], "value");
        ASSERT_EQUAL(obj.size(), 1);

        // Similarly, what if we want an array with one item? This thinks you've got unneccesary braces.
        // auto array = JSON::Value({5});

        // You could do, this, but that's ugly too:
        // auto array = JSON::Value(list<JSON::Value>{5});

        // So we allow the following:
        auto array = JSON::Value::singleItemArray(5);

        ASSERT_TRUE(array.isArray());
        ASSERT_EQUAL(array.size(), 1);
        ASSERT_EQUAL(array[0].getInt(), 5);
    }

    void operators()
    {
        JSON::Value object({
            {"key1", 1},
            {"key2", JSON::Value({
                    {"key3", 3},
                    {"key4", 4},
                    {"arrayKey", JSON::Value(list<JSON::Value>{5, 6, JSON::Value(list<JSON::Value>{7, 8})})},
            })},
        });
        JSON::Value objectCopy(object);
        ASSERT_EQUAL(JSON::Value(1), object["key1"]);
        ASSERT_EQUAL(JSON::Value({{"key3", 3}, {"key4", 4}, {"arrayKey", JSON::Value(list<JSON::Value>{5, 6, JSON::Value(list<JSON::Value>{7, 8})})}}), object["key2"]);
        ASSERT_EQUAL(JSON::Value(3), object["key2"]["key3"]);
        ASSERT_EQUAL(JSON::Value(3), object["key2"]["key3"]);
        ASSERT_EQUAL(JSON::Value(4), object["key2"]["key4"]);
        ASSERT_EQUAL(JSON::Value(5), object["key2"]["arrayKey"][0]);
        ASSERT_EQUAL(JSON::Value(6), object["key2"]["arrayKey"][1]);
        ASSERT_EQUAL(JSON::Value(7), object["key2"]["arrayKey"][2][0]);
        ASSERT_EQUAL(JSON::Value(8), object["key2"]["arrayKey"][2][1]);

        // A key that doesn't exist should return NULL
        ASSERT_EQUAL(JSON::Value(JSON::NIL), object["doesntexist"]);
        ASSERT_TRUE(object.hasMember("doesntexist"));

        // Verify r-value operator[] works
        ASSERT_EQUAL(JSON::Value({{"key3", 3}, {"key4", 4}, {"arrayKey", JSON::Value(list<JSON::Value>{5, 6, JSON::Value(list<JSON::Value>{7, 8})})}}), JSON::Value(object)["key2"]);
        ASSERT_EQUAL(JSON::Value(3), JSON::Value(object)["key2"]["key3"]);
        ASSERT_EQUAL(JSON::Value(3), JSON::Value(object)["key2"]["key3"]);
        ASSERT_EQUAL(JSON::Value(4), JSON::Value(object)["key2"]["key4"]);
        ASSERT_EQUAL(JSON::Value(5), JSON::Value(object)["key2"]["arrayKey"][0]);
        ASSERT_EQUAL(JSON::Value(6), JSON::Value(object)["key2"]["arrayKey"][1]);
        ASSERT_EQUAL(JSON::Value(7), JSON::Value(object)["key2"]["arrayKey"][2][0]);
        ASSERT_EQUAL(JSON::Value(8), JSON::Value(object)["key2"]["arrayKey"][2][1]);

        // A key that doesn't exist should return NULL
        ASSERT_EQUAL(JSON::Value(JSON::NIL), JSON::Value(object)["doesntexist2"]);

        // Verify that the object didn't change except for adding "doesntexist" = null
        objectCopy["doesntexist"] = JSON::Value(JSON::NIL);
        ASSERT_EQUAL(objectCopy, object);

        // Lets verify that the operator[string] && doesn't copy
        vector<JSON::Value>::iterator iterator1 = object["key2"]["arrayKey"].arrayBegin();
        JSON::Value arrayKey = move(object)["key2"]["arrayKey"];
        vector<JSON::Value>::iterator iterator2 = arrayKey.arrayBegin();
        ASSERT_TRUE(iterator1 == iterator2);

        // Restore object
        object = objectCopy;

        // Lets verify that the operator[size_t] && doesn't copy
        iterator1 = object["key2"]["arrayKey"][2].arrayBegin();
        JSON::Value arrayKey2 = move(object)["key2"]["arrayKey"][2];
        iterator2 = arrayKey2.arrayBegin();
        ASSERT_TRUE(iterator1 == iterator2);
    }

    void shallowCopyValue()
    {
        JSON::Value objectSource({
            {"name", "source"},
            {"count", 1},
        });
        JSON::Value objectAlias;
        objectAlias.shallowCopy(objectSource);

        objectSource["count"] = 2;
        ASSERT_TRUE(objectAlias.isObject());
        ASSERT_EQUAL(objectAlias["count"], 2);

        JSON::Value arraySource(JSON::ARRAY);
        arraySource.push_back("first");
        JSON::Value arrayAlias;
        arrayAlias.shallowCopy(arraySource);

        arraySource[0] = "updated";
        ASSERT_TRUE(arrayAlias.isArray());
        ASSERT_EQUAL(arrayAlias[0], "updated");

        JSON::Value nullSource(JSON::NIL);
        JSON::Value nullAlias;
        nullAlias.shallowCopy(nullSource);

        ASSERT_TRUE(nullAlias.isNull());
    }

    void shallowCopyToKey()
    {
        JSON::Value objectSource({
            {"name", "source"},
            {"count", 1},
        });
        JSON::Value arraySource(JSON::ARRAY);
        arraySource.push_back("first");
        JSON::Value nullSource(JSON::NIL);

        JSON::Value container(JSON::OBJECT);
        container.shallowCopy("object", objectSource);
        container.shallowCopy("array", arraySource);
        container.shallowCopy("nil", nullSource);

        objectSource["count"] = 2;
        arraySource[0] = "updated";

        ASSERT_TRUE(container["object"].isObject());
        ASSERT_EQUAL(container["object"]["count"], 2);
        ASSERT_TRUE(container["array"].isArray());
        ASSERT_EQUAL(container["array"][0], "updated");
        ASSERT_TRUE(container["nil"].isNull());
    }

    void preventTypeChangeInShallowCopy()
    {
        string expectedError = "500 Change the type of a wrapper in shallow copy is not allowed";
        auto assertTypeChangeRejected = [&](auto&& fn) {
            try {
                fn();
                FAIL();
            } catch (const SException& e) {
                ASSERT_EQUAL(expectedError, string(e.what()));
            }
        };

        JSON::Value nested(JSON::OBJECT);
        nested["child"] = JSON::Value(JSON::OBJECT);
        nested["child"]["leaf"] = 1;
        assertTypeChangeRejected([&] {
            nested["child"]["leaf"].shallowCopy(nested);
        });

        assertTypeChangeRejected([&] {
            nested["child"].shallowCopy("leaf", nested);
        });

        JSON::Value nilValue;
        JSON::Value objectValue(JSON::OBJECT);
        nilValue.shallowCopy(objectValue);
        ASSERT_TRUE(nilValue.isObject());
    }

    void getBoolFromBinaryIntOrBool()
    {
        // Test with boolean true
        JSON::Value boolTrue(true);
        ASSERT_EQUAL(true, boolTrue.getBoolFromBinaryIntOrBool());

        // Test with boolean false
        JSON::Value boolFalse(false);
        ASSERT_EQUAL(false, boolFalse.getBoolFromBinaryIntOrBool());

        // Test with integer 1 (should return true)
        JSON::Value intOne(1);
        ASSERT_EQUAL(true, intOne.getBoolFromBinaryIntOrBool());

        // Test with integer 0 (should return false)
        JSON::Value intZero(0);
        ASSERT_EQUAL(false, intZero.getBoolFromBinaryIntOrBool());

        // Test with negative integer -1 (should throw TypeError)
        JSON::Value intNegOne(-1);
        EXPECT_THROW(intNegOne.getBoolFromBinaryIntOrBool(), JSON::TypeError);

        // Test with integer 2 (should throw TypeError - not binary)
        JSON::Value intTwo(2);
        EXPECT_THROW(intTwo.getBoolFromBinaryIntOrBool(), JSON::TypeError);

        // Test with large integer (should throw TypeError)
        JSON::Value intLarge(42);
        EXPECT_THROW(intLarge.getBoolFromBinaryIntOrBool(), JSON::TypeError);

        // Test with unsigned integer 1 (should return true)
        JSON::Value uintOne(1ULL);
        ASSERT_EQUAL(true, uintOne.getBoolFromBinaryIntOrBool());

        // Test with unsigned integer 0 (should return false)
        JSON::Value uintZero(0ULL);
        ASSERT_EQUAL(false, uintZero.getBoolFromBinaryIntOrBool());

        // Test with large unsigned integer (should throw TypeError)
        JSON::Value uintLarge(9223372036854775808ULL);
        EXPECT_THROW(uintLarge.getBoolFromBinaryIntOrBool(), JSON::TypeError);

        // Test with string (should throw TypeError)
        JSON::Value stringVal("true");
        EXPECT_THROW(stringVal.getBoolFromBinaryIntOrBool(), JSON::TypeError);

        // Test with null (should throw TypeError)
        JSON::Value nullVal;
        EXPECT_THROW(nullVal.getBoolFromBinaryIntOrBool(), JSON::TypeError);

        // Test with array (should throw TypeError)
        JSON::Value arrayVal(JSON::ARRAY);
        EXPECT_THROW(arrayVal.getBoolFromBinaryIntOrBool(), JSON::TypeError);

        // Test with object (should throw TypeError)
        JSON::Value objectVal(JSON::OBJECT);
        EXPECT_THROW(objectVal.getBoolFromBinaryIntOrBool(), JSON::TypeError);

        // Test with float (should throw TypeError)
        JSON::Value floatVal(1.0);
        EXPECT_THROW(floatVal.getBoolFromBinaryIntOrBool(), JSON::TypeError);

        // Test values parsed from JSON strings
        JSON::Value parsedIntOne = JSON::Value::parse("1");
        ASSERT_EQUAL(true, parsedIntOne.getBoolFromBinaryIntOrBool());

        JSON::Value parsedIntZero = JSON::Value::parse("0");
        ASSERT_EQUAL(false, parsedIntZero.getBoolFromBinaryIntOrBool());

        JSON::Value parsedBoolTrue = JSON::Value::parse("true");
        ASSERT_EQUAL(true, parsedBoolTrue.getBoolFromBinaryIntOrBool());

        JSON::Value parsedBoolFalse = JSON::Value::parse("false");
        ASSERT_EQUAL(false, parsedBoolFalse.getBoolFromBinaryIntOrBool());

        // Test from nested object
        JSON::Value policy = JSON::Value::parse("{\"areTagsEnabled\": 1, \"areWorkflowsEnabled\": true}");
        ASSERT_EQUAL(true, policy["areTagsEnabled"].getBoolFromBinaryIntOrBool());
        ASSERT_EQUAL(true, policy["areWorkflowsEnabled"].getBoolFromBinaryIntOrBool());

        JSON::Value policy2 = JSON::Value::parse("{\"areTagsEnabled\": 0, \"areWorkflowsEnabled\": false}");
        ASSERT_EQUAL(false, policy2["areTagsEnabled"].getBoolFromBinaryIntOrBool());
        ASSERT_EQUAL(false, policy2["areWorkflowsEnabled"].getBoolFromBinaryIntOrBool());
    }
} __JSONValueTest;
