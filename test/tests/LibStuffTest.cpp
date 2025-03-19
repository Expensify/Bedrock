#include <cstring>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <libstuff/SRandom.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/BedrockTester.h>

struct LibStuff : tpunit::TestFixture {
    LibStuff() : tpunit::TestFixture(true, "LibStuff",
                                    TEST(LibStuff::testEncryptDecrpyt),
                                    TEST(LibStuff::testSHMACSHA1),
                                    TEST(LibStuff::testSHMACSHA256),
                                    TEST(LibStuff::testJSONDecode),
                                    TEST(LibStuff::testJSON),
                                    TEST(LibStuff::testEscapeUnescape),
                                    TEST(LibStuff::testTrim),
                                    TEST(LibStuff::testCollapse),
                                    TEST(LibStuff::testStrip),
                                    TEST(LibStuff::testChunkedEncoding),
                                    TEST(LibStuff::testDaysInMonth),
                                    TEST(LibStuff::testGZip),
                                    TEST(LibStuff::testConstantTimeEquals),
                                    TEST(LibStuff::testParseIntegerList),
                                    TEST(LibStuff::testSData),
                                    TEST(LibStuff::testSTable),
                                    TEST(LibStuff::testFileIO),
                                    TEST(LibStuff::testSQList),
                                    TEST(LibStuff::testRandom),
                                    TEST(LibStuff::testHexConversion),
                                    TEST(LibStuff::testBase32Conversion),
                                    TEST(LibStuff::testContains),
                                    TEST(LibStuff::testFirstOfMonth),
                                    TEST(LibStuff::SREMatchTest),
                                    TEST(LibStuff::SREReplaceTest),
                                    TEST(LibStuff::SQResultTest),
                                    TEST(LibStuff::testReturningClause),
                                    TEST(LibStuff::SRedactSensitiveValuesTest)
                                    )
    { }

    void testEncryptDecrpyt() {
        string iv = "58fae8d18b6fe8ed";
        const string key = "44e8ff3f0e0e5323e953ac91685a62e0";
        string clearText = "Encrypt this message.";
        const string encrypted = SAESEncrypt(clearText, iv, key);
        const string decrypted = SAESDecrypt(encrypted, iv, key);
        ASSERT_TRUE(clearText != encrypted);
        ASSERT_EQUAL(clearText, decrypted);
    }

    void testSHMACSHA1() {
        ASSERT_EQUAL(SToHex(SHMACSHA1("", "")), "FBDB1D1B18AA6C08324B7D64B71FB76370690E1D");
        ASSERT_EQUAL(SToHex(SHMACSHA1("key", "The quick brown fox jumps over the lazy dog")),
                     "DE7C9B85B8B78AA6BC8A7A36F70A90701C9DB4D9");
    }

    void testSHMACSHA256() {
        ASSERT_EQUAL(SToHex(SHMACSHA256("", "")), "B613679A0814D9EC772F95D778C35FC5FF1697C493715653C6C712144292C5AD");
        ASSERT_EQUAL(SToHex(SHMACSHA256("key", "Only a Sith deals in absolutes")), "524C9B1C0B6E9F47F10041A429FCB2C880129F940DC9E41F31267E0909D46845");
    }

    void testJSONDecode() {
        const string& sampleJson = SFileLoad("sample_data/lottoNumbers.json");
        ASSERT_FALSE(sampleJson.empty());

        STable obj = SParseJSONObject(sampleJson);
        STable metaData = SParseJSONObject(obj["meta"]);
        STable view = SParseJSONObject(metaData["view"]);
        ASSERT_EQUAL(obj["top"], "top level attribute");
        ASSERT_EQUAL(metaData["null_attribute"], "null");
        ASSERT_EQUAL(view["name"], "Lottery Mega Millions Winning Numbers: Beginning 2002");
    }

    void testJSON() {
        // Floating point value tests
        ASSERT_EQUAL(SToJSON("{\"imAFloat\":0.0000}"), "{\"imAFloat\":0.0000}");
        ASSERT_EQUAL(SToJSON("{\"imAFloat\":-0.23456789}"), "{\"imAFloat\":-0.23456789}");
        ASSERT_EQUAL(SToJSON("{\"imAFloat\":-123456789.23456789}"), "{\"imAFloat\":-123456789.23456789}");
        ASSERT_EQUAL(SToJSON("{\"object\":{\"imAFloat\":1.00}}"), "{\"object\":{\"imAFloat\":1.00}}");

        STable testFloats;
        testFloats["imAFloat"] = (double)0.000;
        string returnVal = SComposeJSONObject(testFloats);
        ASSERT_EQUAL(returnVal, "{\"imAFloat\":0.000000}");

        // Scientific notation tests
        ASSERT_EQUAL(SToJSON("{\"science\":1.5e-8}"), "{\"science\":1.5e-8}");
        ASSERT_EQUAL(SToJSON("{\"science\":-1.5e-30}"), "{\"science\":-1.5e-30}");
        ASSERT_EQUAL(SToJSON("{\"science\":1e58}"), "{\"science\":1e58}");
        ASSERT_EQUAL(SToJSON("{\"science\":9e+61}"), "{\"science\":9e+61}");
        ASSERT_EQUAL(SToJSON("{\"science\":1E+99}"), "{\"science\":1E+99}");

        STable innerObject0, innerObject1, innerObject0Verify, innerObject1Verify;
        innerObject0["utf8"] = "{\"foo\":\"\\u00b7\"}";
        innerObject0["singleQuoteTest"] = "These are 'single quotes'.";
        innerObject0["doubleQuoteTest"] = "These are \"double quotes\".";
        innerObject0["wackyTest"] = "`1234567890-=~!@#$%^&*()_+[]\\{}|.;':\",./<>\\n?";
        innerObject0["badJSON"] = "{\"name\":\"bb\",\"type\":\"text\"}],\"approverTable\":false,\"ccList\":[],"
                                  "\"approver\":\"blah@company.com\"}";
        innerObject0["ofxTest"] = "{\"ofx\":\"<OFX>\\n<SIGNONMSGSRSV1>\\n<SONRS>\\n<STATUS><CODE>15501\\n<SEVERITY>"
                                  "ERROR\\n<MESSAGE>The profile currently in "
                                  "use.\\n<\\/"
                                  "STATUS>\\n<DTSERVER>20110304033419.851[0:GMT]\\n<LANGUAGE>ENG\\n<FI>\\n<ORG>BB&amp;"
                                  "T\\n<FID>BB&amp;T\\n<\\/FI>\\n<\\/SONRS>\\n<\\/"
                                  "SIGNONMSGSRSV1>\\n<SIGNUPMSGSRSV1>\\n<ACCTINFOTRNRS>\\n<TRNUID>315B1FB9-913C-991F-"
                                  "1BD7-6537CAA54C26\\n<STATUS>\\n<CODE>15500\\n<SEVERITY>ERROR\\n<\\/"
                                  "STATUS>\\n<CLTCOOKIE>4\\n<\\/ACCTINFOTRNRS>\\n<\\/SIGNUPMSGSRSV1>\\n<\\/OFX>\"}";

        // Verify we can undo our own encoding (Objects)
        innerObject0Verify = SParseJSONObject(SComposeJSONObject(innerObject0));
        for (auto& item : innerObject0) {
            ASSERT_EQUAL(innerObject0[item.first], innerObject0Verify[item.first]);
        }

        // Verify we can undo our own encoding (Arrays)
        innerObject1 = innerObject0;
        list<string> innerObjectList;
        innerObjectList.push_back(SComposeJSONObject(innerObject0));
        innerObjectList.push_back(SComposeJSONObject(innerObject1));
        list<string> innerObjectListVerify = SParseJSONArray(SComposeJSONArray(innerObjectList));
        ASSERT_TRUE(innerObjectListVerify.size() == 2);

        // Verify we can undo our own encoding through 2 levels (Object)
        innerObject0Verify = SParseJSONObject(innerObjectListVerify.front());
        innerObject1Verify = SParseJSONObject(innerObjectListVerify.back());
        for (auto& item : innerObject0) {
            ASSERT_EQUAL(innerObject0[item.first], innerObject0Verify[item.first]);
        }
        for (auto& item : innerObject1) {
            ASSERT_EQUAL(innerObject1[item.first], innerObject1Verify[item.first]);
        }

        // Verify we can parse/encode PHP objects
        ASSERT_EQUAL(innerObject0["ofxTest"], SComposeJSONObject(SParseJSONObject(innerObject0["ofxTest"])));

        // Test parsing a crazy thing
        STable ignore = SParseJSONObject(
            SStrFromHex("7D6628F7AE67FBACE9DAF79312C48BA0B41AADD5BA1704E929B96B6F87708C0898868D55C0AAAE117CF20F1317D151"
                        "348706C9EDFE8A0CDD13BFB476367DEA2761A102B26443C7D3A464DB49A37F1F816B8BEC4C55DBD9DAF0B70652D32A"
                        "CBD224F9487E25398E740E99B24089A6343B6FD6C1BC6A89AF90F3DC69016A42066AAF430B1B584D236B8AD285828D"
                        "59BB8375E2E955E246390DE9AA69D05DEF1FBC25318C9CCFE90159EC7EAA71637C07BD"));
    }

    void testEscapeUnescape() {
        ASSERT_EQUAL(SEscape("a,b,c", ","), "a\\,b\\,c");
        ASSERT_EQUAL(SUnescape(SEscape("\r\n\\\"\\zqy", "q")), "\r\n\\\"\\zqy");
        ASSERT_EQUAL(SEscape("\x1a", "\x1a"), "\\u001a");
        ASSERT_EQUAL(SUnescape("\\u0041"), "A");            // 1 Byte
        ASSERT_EQUAL(SUnescape("\\u00b7"), "\xc2\xb7");     // 2 Byte
        ASSERT_EQUAL(SUnescape("\\uc2b7"), "\xec\x8a\xb7"); // 3 Byte
        ASSERT_EQUAL(SUnescape("\\u05c0"), "\xd7\x80");     // 2 Byte, bottom 0
    }

    void testTrim() {
        ASSERT_EQUAL("", STrim(""));
        ASSERT_EQUAL("", STrim(" \t\n\r"));
        ASSERT_EQUAL("FooBar", STrim("FooBar"));
        ASSERT_EQUAL("FooBar", STrim(" FooBar"));
        ASSERT_EQUAL("FooBar", STrim("FooBar "));
        ASSERT_EQUAL("FooBar", STrim(" FooBar "));
    }

    void testCollapse() {
        ASSERT_EQUAL("", SCollapse(""));
        ASSERT_EQUAL(" ", SCollapse("   "));
        ASSERT_EQUAL(" ", SCollapse("\t  "));
        ASSERT_EQUAL("Lorem ipsum", SCollapse("Lorem ipsum"));
        ASSERT_EQUAL("Lorem ipsum", SCollapse("Lorem \r\t\nipsum"));
        ASSERT_EQUAL(" Lorem ipsum ", SCollapse("  Lorem \r\t\nipsum \r\n"));
    }

    void testStrip() {
        // simple SStrip
        ASSERT_EQUAL("", SStrip(""));
        ASSERT_EQUAL("   ", SStrip("   "));
        ASSERT_EQUAL("", SStrip("\t\n\r\x0b\x1f"));

        // SStrip(lhr, chars, charsAreSafe)
        ASSERT_EQUAL("FooBr", SStrip("Foo Bar", " a", false));
        ASSERT_EQUAL(" a", SStrip("Foo Bar", " a", true));
    }

    void testChunkedEncoding() {
        string methodLine, content, recvBuffer;
        STable headers;
        size_t processed;

        recvBuffer = "some method line\r\n"
                     "header1: value1\r"
                     "header2: value2\n"
                     "header3: value3\r\n"
                     "\r\n"
                     "this is the body";
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL((int64_t)processed, (int)recvBuffer.size());
        ASSERT_EQUAL(methodLine, "some method line");
        ASSERT_EQUAL(headers["header1"], "value1");
        ASSERT_EQUAL(headers["header2"], "value2");
        ASSERT_EQUAL(headers["header3"], "value3");
        ASSERT_EQUAL(content, "this is the body");

        recvBuffer = "some method line\r\n"
                     "Content-Length: 100\r"
                     "\r\n"
                     "too short";
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL(processed, 0);
        ASSERT_EQUAL(methodLine, "");
        ASSERT_EQUAL(content, "");

        recvBuffer = "some method line\r\n"
                     "Content-Length: 5\r"
                     "\r\n"
                     "too short"; // only 'too s' read.
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL(methodLine, "some method line");
        ASSERT_EQUAL(content, "too s");

        recvBuffer = "some method line\r\n"
                     "Transfer-Encoding: chunked\r"
                     "\r\n"
                     "5\r\n"
                     "abcde\r\n"
                     "a; ignored chunk header crap\r\n"
                     "0123456789\r\n"
                     "0\r\n"
                     "\r\n";
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL(methodLine, "some method line");
        ASSERT_EQUAL(content, "abcde0123456789");

        recvBuffer = "some method line\r\n"
                     "Transfer-Encoding: chunked\r"
                     "\r\n"
                     "6\r\n" // one too long.
                     "abcde";
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL(methodLine, "");
        ASSERT_EQUAL(content, "");

        recvBuffer = "some method line\r\n"
                     "Transfer-Encoding: chunked\r"
                     "\r\n"
                     "5\r\n" // exact without end.
                     "abcde";
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL(methodLine, "");
        ASSERT_EQUAL(content, "");

        recvBuffer = "some method line\r\n"
                     "header1: value1\r"
                     "header2: value2\n"
                     "Transfer-Encoding: chunked\r"
                     "\r\n"
                     "5\r\n"
                     "abcde\r\n"
                     "a; ignored chunk header crap\r\n"
                     "0123456789\r\n"
                     "0\r\n"
                     "header2: value2a\n"
                     "header3: value3\n"
                     "\r\n";
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL(methodLine, "some method line");
        ASSERT_EQUAL(headers["header1"], "value1");
        ASSERT_EQUAL(headers["header2"], "value2a");
        ASSERT_EQUAL(headers["header3"], "value3");
        ASSERT_EQUAL(content, "abcde0123456789");

        recvBuffer = "some method line x\r\n"
                     "Transfer-Encoding: chunked\r"
                     "\r\n"
                     "5\r\n"
                     "abcde\r\n"
                     "aa; ignored chunk header crap\r\n" // too long
                     "0123456789\r\n"
                     "0\r\n"
                     "\r\n";
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL(methodLine, "");
        ASSERT_EQUAL(content, "");

        recvBuffer = "some method line y\r\n"
                     "Transfer-Encoding: chunked\r"
                     "\r\n"
                     "5\r\n"
                     "abcde\r\n"
                     "az; ignored chunk header crap\r\n" // invalid hex
                     "0123456789\r\n"
                     "0\r\n"
                     "\r\n";
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL(methodLine, "some method line y");
        ASSERT_EQUAL(content, "abcde"); // partial fail.

        recvBuffer = "some method line z\r\n"
                     "Transfer-Encoding: chunked\r"
                     "\r\n"
                     "5\r\n"
                     "abcde\r\n"
                     "a; ignored chunk header crap\r\n"
                     "0123456789\r\n"
                     "0\r\n";
        // "\r\n"; // missing last new line.
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL(methodLine, "");
        ASSERT_EQUAL(content, "");
    }

    void testDaysInMonth() {
        ASSERT_EQUAL(SDaysInMonth(2012, 2), 29);
        ASSERT_EQUAL(SDaysInMonth(2013, 2), 28);
        ASSERT_EQUAL(SDaysInMonth(2013, 4), 30);
        ASSERT_EQUAL(SDaysInMonth(2014, 1), 31);
        ASSERT_EQUAL(SDaysInMonth(2014, 7), 31);
    }

    void testGZip() {

        // All these really test is that we won't segfault or anything.
        string data = "";
        ASSERT_TRUE(SGZip(data).length() > 1);

        data += (char)(SRandom::rand64() % 256);
        ASSERT_TRUE(SGZip(data).length() > 1);

        data = "";
        for (int i = 0; i < 10000000; i++) {
            data += (char)(SRandom::rand64() % 256);
        }
        ASSERT_TRUE(SGZip(data).length() > 1);

        // Ok, this actually tests for correctness.
        data = "this is a test";
        ASSERT_EQUAL(SToHex(SGZip(data)), "1F8B08000000000002032BC9C82C5600A2448592D4E21200EAE71E0D0E000000");

        // Test end to end.
        ASSERT_EQUAL(SGUnzip(SGZip(data)), data);
    }

    void testConstantTimeEquals() {
        // Tests equality but not timing, which is really the important part of this function.
        ASSERT_TRUE(SConstantTimeEquals("", ""));
        ASSERT_TRUE(SConstantTimeEquals("a", "a"));
        ASSERT_TRUE(!SConstantTimeEquals("A", "a"));
        ASSERT_TRUE(SConstantTimeIEquals("A", "a"));
        ASSERT_TRUE(SConstantTimeEquals("1F8B08000000000002032BC9C82C5600A2448592D4E21200EAE71E0D0E000000",
                                        "1F8B08000000000002032BC9C82C5600A2448592D4E21200EAE71E0D0E000000"));
        ASSERT_TRUE(!SConstantTimeEquals("", "secret"));
        ASSERT_TRUE(!SConstantTimeEquals("secret", ""));
        ASSERT_TRUE(!SConstantTimeEquals("secre", "secret"));
        ASSERT_TRUE(!SConstantTimeEquals("secret", "secre"));
        ASSERT_TRUE(!SConstantTimeEquals("1F8B08000000000002032BC9C82C5600A2448592D4E21200EAE71E0D0E000000", "a"));
    }

    void testParseIntegerList() {
        list<int64_t> before;
        for (int i = -10; i < 300; i++) {
            before.push_back(i);
        }
        list<int64_t> after = SParseIntegerList(SComposeList(before));
        ASSERT_TRUE(before == after);
    }

    void testSData() {
        SData a("this is a methodline");
        SData b("methodline");
        ASSERT_EQUAL(a.methodLine, "this is a methodline");
        ASSERT_EQUAL(b.methodLine, "methodline");
        ASSERT_EQUAL(a.getVerb(), "this");
        ASSERT_EQUAL(b.getVerb(), "methodline");

        SData c("Test");
        c.set("a", 1);
        c.set("b", 2.5);
        c.set("c", 3ll);
        c.set("d", 4ull);
        c.set("e", "char*");
        c.set("f", string("string"));
        c.set("g", 'a'); // char, get's converted to number, not string!

        ASSERT_EQUAL(c["a"], "1");
        ASSERT_EQUAL(SToFloat(c["b"]), 2.5);
        ASSERT_EQUAL(c["c"], "3");
        ASSERT_EQUAL(c["d"], "4");
        ASSERT_EQUAL(c["e"], "char*");
        ASSERT_EQUAL(c["f"], "string");
        ASSERT_EQUAL(SToInt(c["g"]), 97);
    }

    void testSTable() {
        // Verify that auto-stringification works.
        STable test;
        test["a"] = 1;
        test["b"] = -1;
        test["c"] = 0;
        test["d"] = 5000000001;
        test["e"] = -5000000001;
        test["f"] = 1.2;
        test["g"] = (unsigned char)'a';
        test["h"] = 'b';
        test["i"] = "string";
        test["j"] = true;
        test["k"] = false;
        ASSERT_EQUAL(test["a"], "1");
        ASSERT_EQUAL(test["b"], "-1");
        ASSERT_EQUAL(test["c"], "0");
        ASSERT_EQUAL(test["d"], "5000000001");
        ASSERT_EQUAL(test["e"], "-5000000001");
        ASSERT_EQUAL(test["f"], "1.200000"); // default precision.
        ASSERT_EQUAL(test["g"], "a");
        ASSERT_EQUAL(test["h"], "b");
        ASSERT_EQUAL(test["i"], "string");
        ASSERT_EQUAL(test["j"], "true");
        ASSERT_EQUAL(test["k"], "false");
    }

    void testFileIO() {
        const string path = "./fileio.test";
        const string contents = "test";
        string readBuffer;

        // File doesn't exist yet
        ASSERT_TRUE(!SFileExists(path));

        // We can create a file
        ASSERT_TRUE(SFileSave(path, contents));

        // The file exists
        ASSERT_TRUE(SFileExists(path));

        // We can read its contents
        ASSERT_TRUE(SFileLoad(path, readBuffer));

        // The contents did not change
        ASSERT_EQUAL(readBuffer, contents);

        // The file length is correct
        ASSERT_EQUAL(SFileSize(path), contents.length());

        // We can delete the file
        ASSERT_TRUE(SFileDelete(path));

        // The file no longer exists
        ASSERT_TRUE(!SFileExists(path));

        // The non-existent file's size is reported as zero
        ASSERT_EQUAL(SFileSize(path), 0);
    }

    void testSQList() {
        list<int> intList;
        list<unsigned> uintList;
        list<int64_t> int64List;
        list<uint64_t> uint64List;
        list<string> stringList;

        vector<int> intVector;
        vector<unsigned> uintVector;
        vector<int64_t> int64Vector;
        vector<uint64_t> uint64Vector;
        vector<string> stringVector;

        set<int> intSet;
        set<unsigned> uintSet;
        set<int64_t> int64Set;
        set<uint64_t> uint64Set;
        set<string> stringSet;

        ASSERT_EQUAL(SQList(intList), "");
        ASSERT_EQUAL(SQList(uintList), "");
        ASSERT_EQUAL(SQList(int64List), "");
        ASSERT_EQUAL(SQList(uint64List), "");
        ASSERT_EQUAL(SQList(stringList), "");

        ASSERT_EQUAL(SQList(intVector), "");
        ASSERT_EQUAL(SQList(uintVector), "");
        ASSERT_EQUAL(SQList(int64Vector), "");
        ASSERT_EQUAL(SQList(uint64Vector), "");
        ASSERT_EQUAL(SQList(stringVector), "");

        ASSERT_EQUAL(SQList(intSet), "");
        ASSERT_EQUAL(SQList(uintSet), "");
        ASSERT_EQUAL(SQList(int64Set), "");
        ASSERT_EQUAL(SQList(uint64Set), "");
        ASSERT_EQUAL(SQList(stringSet), "");

        intList.push_back(1);
        uintList.push_back(1);
        int64List.push_back(-10000000000);
        uint64List.push_back(10000000000);
        stringList.push_back("1");

        intVector.push_back(1);
        uintVector.push_back(1);
        int64Vector.push_back(-10000000000);
        uint64Vector.push_back(10000000000);
        stringVector.push_back("1");

        intSet.insert(1);
        uintSet.insert(1);
        int64Set.insert(-10000000000);
        uint64Set.insert(10000000000);
        stringSet.insert("1");

        ASSERT_EQUAL(SQList(intList), "1");
        ASSERT_EQUAL(SQList(uintList), "1");
        ASSERT_EQUAL(SQList(int64List), "-10000000000");
        ASSERT_EQUAL(SQList(uint64List), "10000000000");
        ASSERT_EQUAL(SQList(stringList), "'1'");

        ASSERT_EQUAL(SQList(intVector), "1");
        ASSERT_EQUAL(SQList(uintVector), "1");
        ASSERT_EQUAL(SQList(int64Vector), "-10000000000");
        ASSERT_EQUAL(SQList(uint64Vector), "10000000000");
        ASSERT_EQUAL(SQList(stringVector), "'1'");

        ASSERT_EQUAL(SQList(intSet), "1");
        ASSERT_EQUAL(SQList(uintSet), "1");
        ASSERT_EQUAL(SQList(int64Set), "-10000000000");
        ASSERT_EQUAL(SQList(uint64Set), "10000000000");
        ASSERT_EQUAL(SQList(stringSet), "'1'");

        intList.push_back(-1);
        uintList.push_back(2);
        int64List.push_back(10000000000);
        uint64List.push_back(2);
        stringList.push_back("potato");

        intVector.push_back(-1);
        uintVector.push_back(2);
        int64Vector.push_back(10000000000);
        uint64Vector.push_back(2);
        stringVector.push_back("potato");

        intSet.insert(-1);
        uintSet.insert(2);
        int64Set.insert(10000000000);
        uint64Set.insert(2);
        stringSet.insert("potato");

        ASSERT_EQUAL(SQList(intList), "1, -1");
        ASSERT_EQUAL(SQList(uintList), "1, 2");
        ASSERT_EQUAL(SQList(int64List), "-10000000000, 10000000000");
        ASSERT_EQUAL(SQList(uint64List), "10000000000, 2");
        ASSERT_EQUAL(SQList(stringList), "'1', 'potato'");

        ASSERT_EQUAL(SQList(intSet), "-1, 1");
        ASSERT_EQUAL(SQList(uintSet), "1, 2");
        ASSERT_EQUAL(SQList(int64Set), "-10000000000, 10000000000");
        ASSERT_EQUAL(SQList(uint64Set), "2, 10000000000");
        ASSERT_EQUAL(SQList(stringSet), "'1', 'potato'");

        ASSERT_EQUAL(SQList(intVector), "1, -1");
        ASSERT_EQUAL(SQList(uintVector), "1, 2");
        ASSERT_EQUAL(SQList(int64Vector), "-10000000000, 10000000000");
        ASSERT_EQUAL(SQList(uint64Vector), "10000000000, 2");
        ASSERT_EQUAL(SQList(stringVector), "'1', 'potato'");

        ASSERT_EQUAL(SQList(intVector), SQList(SComposeList(intList)));
        ASSERT_EQUAL(SQList(stringList), SQList(SComposeList(stringList), false));
    }

    void testUpperLower() {
        ASSERT_EQUAL(SToUpper("asdf"), "ASDF");
        ASSERT_EQUAL(SToUpper("as-as"), "AS-AS");
        ASSERT_EQUAL(SToLower("ASDF"), "asdf");
        ASSERT_EQUAL(SToLower("AS_DF"), "as_df");
    }

    void testRandom() {
        // There's not really a good way to test a random number generator. This test is here in case someone wants to
        // uncomment the `cout` line and verify that the numbers don't all come out the same, or sequential, but an
        // automated test for this is impractical. This might be useful if you change the random number code and want
        // to make sure it's not fundamentally broken.
        for (int i = 0; i < 50; i++) {
            uint64_t randomNumber = SRandom::rand64();
            ASSERT_TRUE(randomNumber | 1); // Shuts up the "unused variable" warning.
            // cout << "Randomly generated uint64_t: " << randomNumber << endl;
        }
    }

    void testHexConversion() {
        ASSERT_EQUAL(SStrFromHex("48656c6c6f"), "Hello");
        ASSERT_EQUAL(SStrFromHex("48656C6C6f"), "Hello");

        string start = "I wish I was an Oscar Meyer Weiner";
        ASSERT_EQUAL(SStrFromHex(SToHex(start)), start);

    }

    void testBase32Conversion() {
        ASSERT_EQUAL(SBase32HexStringFromBase32("ABCDEF"), "012345");
        ASSERT_EQUAL(SBase32HexStringFromBase32("LMNOPQRST"), "BCDEFGHIJ");

        ASSERT_EQUAL(SHexStringFromBase32("RQMRTRON"), "DEADBEEF17")
    }

    void testContains() {
        list<string> stringList = list<string>();
        stringList.push_back("asdf");
        ASSERT_TRUE(SContains(stringList, "asdf"));
        ASSERT_FALSE(SContains(stringList, "fdsa"));

        ASSERT_TRUE(SContains(string("asdf"), "a"));
        ASSERT_TRUE(SContains(string("asdf"), string("asd")));
    }

    void testFirstOfMonth() {
        string timeStamp = "2020-03-01";
        string timeStamp2 = "2020-12-01";
        string timeStamp3 = "2020-06-17";
        string timeStamp4 = "2020-07-07";
        string timeStamp5 = "2020-11-11";
        string timeStamp6 = "2020-01-01";
        string octalTimestamp = "2019-09-03";
        string notATimeStamp = "this is not a timestamp";

        ASSERT_EQUAL(SFirstOfMonth(timeStamp, 1), "2020-04-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp2, 1), "2021-01-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp3, 1), "2020-07-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4), "2020-07-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, 12), "2021-07-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, 25), "2022-08-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, -1), "2020-06-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, -13), "2019-06-01")
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, -25), "2018-06-01")
        ASSERT_EQUAL(SFirstOfMonth(timeStamp5, 3), "2021-02-01");
        ASSERT_EQUAL(SFirstOfMonth(octalTimestamp, 1), "2019-10-01");
        ASSERT_THROW(SFirstOfMonth(notATimeStamp), SException);

        ASSERT_EQUAL(SFirstOfMonth(timeStamp4), "2020-07-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, 1), "2020-08-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, 6), "2021-01-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, 13), "2021-08-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, 25), "2022-08-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, -1), "2020-06-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, -7), "2019-12-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, -13), "2019-06-01");
        ASSERT_EQUAL(SFirstOfMonth(timeStamp4, -25), "2018-06-01");
    }

    void SREMatchTest() {
        // Basic case.
        ASSERT_TRUE(SREMatch(".*cat.*", "this contains cat"));
        ASSERT_FALSE(SREMatch(".*cat.*", "this does not"));

        // Case sensitive but case doesn't match.
        ASSERT_FALSE(SREMatch(".*CAT.*", "this contains cat"));

        // Case-insensitive.
        ASSERT_TRUE(SREMatch(".*CAT.*", "this contains cat", false));
        ASSERT_FALSE(SREMatch(".*CAT.*", "this does not", false));

        // Capture groups don't break internal code.
        ASSERT_TRUE(SREMatch(".*cat.*", "(this) (contains) (cat)"));
        ASSERT_FALSE(SREMatch(".*cat.*", "(this) (does) (not)"));

        // Partial matches aren't counted.
        ASSERT_FALSE(SREMatch("cat", "this contains cat"));

        // Now try with partial specified, should work.
        ASSERT_TRUE(SREMatch("cat", "this contains cat", true, true));

        // Test returning matches.
        vector<string> matches;
        SREMatch(R"((\w+) (\w+) (\w+))", "this contains cat", false, false, &matches);

        // The whole string, and the three groups.
        ASSERT_EQUAL(matches.size(), 4);
        ASSERT_EQUAL(matches[0], "this contains cat");
        ASSERT_EQUAL(matches[1], "this");
        ASSERT_EQUAL(matches[2], "contains");
        ASSERT_EQUAL(matches[3], "cat");

        // Let's do multiple matches.
        string catNames = "kitty Whiskers, kitty Mittens, kitty Snowball";
        auto matchList = SREMatchAll(R"(kitty\s+(\w+))", catNames);
        ASSERT_EQUAL(matchList.size(), 3);
        ASSERT_EQUAL(matchList[0][1], "Whiskers");
        ASSERT_EQUAL(matchList[1][1], "Mittens");
        ASSERT_EQUAL(matchList[2][1], "Snowball");
    }

    void SREReplaceTest() {
        // This specifically tests multiple replacements and that the final string is longer than the starting string.
        string from = "a cat is not a dog it is a cat";
        string expected = "a dinosaur is not a dog it is a dinosaur";
        string result = SREReplace("cat", from, "dinosaur");
        ASSERT_EQUAL(result, expected);

        // And test case sensitivity (disabled)
        string result2 = SREReplace("CAT", from, "dinosaur");
        ASSERT_EQUAL(result2, from);

        // And test case sensitivity (enabled)
        string result3 = SREReplace("CAT", from, "dinosaur", false);
        ASSERT_EQUAL(result3, expected);

        // Test match groups.
        string from2 = "a cat did something to a dog";
        string result4 = SREReplace("cat(.*)dog", from2, "chicken$1horse");
        ASSERT_EQUAL(result4, "a chicken did something to a horse");
    }

    void SQResultTest() {
        SQLite db(":memory:", 1000, 1000, 1);
        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("CREATE TABLE testTable(id INTEGER PRIMARY KEY, name STRING, value STRING, created DATE);");
        db.write ("INSERT INTO testTable VALUES(1, 'name1', 'value1', '2023-12-20');");
        db.write ("INSERT INTO testTable VALUES(2, 'name2', 'value2', '2023-12-21');");
        db.write ("INSERT INTO testTable VALUES(3, 'name3', 'value3', '2023-12-22');");
        db.prepare();
        db.commit();

        db.beginTransaction(SQLite::TRANSACTION_TYPE::SHARED);
        SQResult result;
        db.read("SELECT name, value FROM testTable ORDER BY id;", result);
        db.rollback();

        // All our names make sense?
        ASSERT_EQUAL(result[0]["name"], "name1");
        ASSERT_EQUAL(result[1]["name"], "name2");
        ASSERT_EQUAL(result[2]["name"], "name3");

        // All our values make sense?
        ASSERT_EQUAL(result[0]["value"], "value1");
        ASSERT_EQUAL(result[1]["value"], "value2");
        ASSERT_EQUAL(result[2]["value"], "value3");

        // Validate our exception handling.
        bool threw = false;
        try {
            string s = result[0]["notacolumn"];
        } catch (const out_of_range& e) {
            threw = true;
        }
        ASSERT_TRUE(threw);

        // Test aliased names.
        db.beginTransaction(SQLite::TRANSACTION_TYPE::SHARED);
        result.clear();
        db.read("SELECT name as coco, value FROM testTable ORDER BY id;", result);
        db.rollback();
        ASSERT_EQUAL(result[0]["coco"], "name1");
    }

    void testReturningClause() {
        // Given a sqlite DB with a table and pre inserted values
        SQLite db(":memory:", 1000, 1000, 1);
        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        db.write("CREATE TABLE testReturning(id INTEGER PRIMARY KEY, name STRING, value STRING, created DATE);");
        db.write("INSERT INTO testReturning VALUES(11, 'name1', 'value1', '2024-12-02');");
        db.write("INSERT INTO testReturning VALUES(21, 'name2', 'value2', '2024-12-03');");
        db.prepare();
        db.commit();

        // When trying to delete a row by returning the deleted items
        db.beginTransaction(SQLite::TRANSACTION_TYPE::SHARED);
        SQResult result;
        db.writeIdempotent("DELETE FROM testReturning WHERE id = 21 AND name = 'name2' RETURNING id, name;", result);
        db.prepare();
        db.commit();

        // Verify that deleted items were returned as expected
        ASSERT_EQUAL("21", result[0][0]);
        ASSERT_EQUAL("name2", result[0][1]);

        // Verify that the row was successfully deleted and now the table has only one row
        db.beginTransaction(SQLite::TRANSACTION_TYPE::SHARED);
        db.read("SELECT name, value FROM testReturning ORDER BY id;", result);
        db.rollback();

        ASSERT_EQUAL(1, result.size());
        ASSERT_EQUAL(result[0]["name"], "name1");
        ASSERT_EQUAL(result[0]["value"], "value1");
    }

    void SRedactSensitiveValuesTest() {
        string logValue = R"({"edits":["test1", "test2", "test3"]})";
        SRedactSensitiveValues(logValue);
        ASSERT_EQUAL(R"({"edits":["REDACTED"]})", logValue);

        logValue = R"({"authToken":"123IMANAUTHTOKEN321"})";
        SRedactSensitiveValues(logValue);
        ASSERT_EQUAL(R"({"authToken":<REDACTED>)", logValue);

        logValue = R"({"html":"private conversation happens here"})";
        SRedactSensitiveValues(logValue);
        ASSERT_EQUAL(R"({"html":"<REDACTED>"})", logValue);
    }
} __LibStuff;
