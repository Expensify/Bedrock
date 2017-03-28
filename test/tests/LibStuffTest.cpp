#include <libstuff/libstuff.h>
#include <test/lib/BedrockTester.h>

struct LibStuff : tpunit::TestFixture {
    LibStuff() : tpunit::TestFixture("LibStuff",
                                    TEST(LibStuff::testMaskPAN),
                                    TEST(LibStuff::testEncryptDecrpyt),
                                    TEST(LibStuff::testSHMACSHA1),
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
                                    TEST(LibStuff::testFileIO),
                                    TEST(LibStuff::testSTimeNow),
                                    TEST(LibStuff::testCurrentTimestamp),
                                    TEST(LibStuff::testSQList),
                                    TEST(LibStuff::testRandom),
                                    TEST(LibStuff::testHexConversion))
    { }

    void testMaskPAN() {
        ASSERT_EQUAL(SMaskPAN(""), "");
        ASSERT_EQUAL(SMaskPAN("123"), "XXX");
        ASSERT_EQUAL(SMaskPAN("1234"), "1234");
        ASSERT_EQUAL(SMaskPAN("12345"), "X2345");
        ASSERT_EQUAL(SMaskPAN("1234567"), "XXX4567");
        ASSERT_EQUAL(SMaskPAN("12345678"), "XXXX5678");
        ASSERT_EQUAL(SMaskPAN("12345678a"), "XXXXX678X");
        ASSERT_EQUAL(SMaskPAN("1234567890123"), "XXXXXXXXX0123");
        ASSERT_EQUAL(SMaskPAN("12345678901234"), "123456XXXX1234");
        ASSERT_EQUAL(SMaskPAN("12345678901234567"), "123456XXXXXXX4567");
        ASSERT_EQUAL(SMaskPAN("123456789012345678901"), "123456XXXXXXXXXXX8901");
    }

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

    void testJSONDecode() {
        const string& samplePolicy = SFileLoad("sample_data/samplePolicy.json");
        ASSERT_FALSE(samplePolicy.empty());

        STable obj = SParseJSONObject(samplePolicy);
        STable units = SParseJSONObject(obj["units"]);
        STable distance = SParseJSONObject(units["distance"]);
        ASSERT_EQUAL(obj["name"], "Name Test");
        ASSERT_EQUAL(distance["km"], "null");
        ASSERT_EQUAL(distance["defaultUnit"], "mi");
    }

    void testJSON() {
        // Floating point value tests
        ASSERT_EQUAL(SToJSON("{\"imAFloat\":1.2}"), "{\"imAFloat\":1.2}");
        ASSERT_EQUAL(SToJSON("{\"imAFloat\":-0.23456789}"), "{\"imAFloat\":-0.23456789}");
        ASSERT_EQUAL(SToJSON("{\"imAFloat\":-123456789.23456789}"), "{\"imAFloat\":-123456789.23456789}");

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
                     "ignored body";
        processed = SParseHTTP(recvBuffer.c_str(), recvBuffer.length(), methodLine, headers, content);
        ASSERT_EQUAL((int64_t)processed, (int)recvBuffer.size() - (int)strlen("ignored body"));
        ASSERT_EQUAL(methodLine, "some method line");
        ASSERT_EQUAL(headers["header1"], "value1");
        ASSERT_EQUAL(headers["header2"], "value2");
        ASSERT_EQUAL(headers["header3"], "value3");
        ASSERT_EQUAL(content, "");

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

    void testSTimeNow() {
        // This is a super-awful test.
        uint64_t prev = STimeNow();
        uint64_t next;
        int failures = 0;
        for (int i = 0; i < 10000; i++) {
            next = STimeNow();
            if (next < prev) {
                failures++;
            }
            prev = next;
        }
        ASSERT_EQUAL(failures, 0);
    }

    void testCurrentTimestamp() {
        // This is also a super-awful test.
        string prev = SCURRENT_TIMESTAMP();
        string next;
        int failures = 0;
        for (int i = 0; i < 10000; i++) {
            next = SCURRENT_TIMESTAMP();
            if (next < prev) {
                failures++;
            }
            prev = next;
        }
        ASSERT_EQUAL(failures, 0);
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
} __LibStuff;
