#include <libstuff/SQResult.h>
#include <libstuff/SThread.h>
#include <libstuff/sqlite3.h>
#include <test/lib/tpunit++.hpp>

#include <latch>

struct SQResultTest : tpunit::TestFixture
{
    SQResultTest()
        : tpunit::TestFixture("SQResult",
                              TEST(SQResultTest::testLiteralAndStringKeys),
                              TEST(SQResultTest::testNumericIndexes),
                              TEST(SQResultTest::testMissingAndShortRows),
                              TEST(SQResultTest::testHeaderReplacement),
                              TEST(SQResultTest::testCopyAndRowOwnership),
                              TEST(SQResultTest::testDeserialization),
                              TEST(SQResultTest::testSQueryReuse),
                              TEST(SQResultTest::testConcurrentReaders))
    {
    }

    void testLiteralAndStringKeys()
    {
        SQResult result;
        result.setHeaders({"name", "value"});
        SQResultRow first(result);
        first.push_back("first");
        first.push_back("one");
        SQResultRow second(result);
        second.push_back("second");
        second.push_back("two");
        result.emplace_back(move(first));
        result.emplace_back(move(second));

        static constexpr char nameKey[] = "name";
        static constexpr char anotherNameKey[] = "name";
        ASSERT_TRUE(static_cast<const void*>(nameKey) != static_cast<const void*>(anotherNameKey));
        for (int repetition = 0; repetition < 3; ++repetition) {
            ASSERT_EQUAL(result[0][nameKey], "first");
            ASSERT_EQUAL(result[1][nameKey], "second");
            ASSERT_EQUAL(result[0][anotherNameKey], "first");
            ASSERT_EQUAL(result[1]["value"], "two");
        }

        SQResultRow mutableRow = result[0];
        ASSERT_EQUAL(mutableRow[nameKey], "first");
        ASSERT_EQUAL(mutableRow[anotherNameKey], "first");
        string dynamicKey = "name";
        ASSERT_EQUAL(mutableRow[dynamicKey], "first");
        dynamicKey = "value";
        ASSERT_EQUAL(mutableRow[dynamicKey], "one");
        const auto& constantRow = result[0];
        ASSERT_EQUAL(constantRow[dynamicKey], "one");

        SQResult differentLayout;
        differentLayout.setHeaders({"value", "name"});
        SQResultRow differentRow(differentLayout);
        differentRow.push_back("other value");
        differentRow.push_back("other name");
        ASSERT_EQUAL(differentRow[nameKey], "other name");
    }

    void testNumericIndexes()
    {
        SQResult result;
        result.setHeaders({"first", "second"});
        SQResultRow row(result);
        row.push_back("zero");
        row.push_back("one");
        const SQResultRow& constantRow = row;

        ASSERT_EQUAL(row[0], "zero");
        ASSERT_EQUAL(row[0U], "zero");
        ASSERT_EQUAL(row[0L], "zero");
        ASSERT_EQUAL(row[0UL], "zero");
        ASSERT_EQUAL(row[0LL], "zero");
        ASSERT_EQUAL(row[0ULL], "zero");
        ASSERT_EQUAL(constantRow[0], "zero");
        ASSERT_EQUAL(constantRow[0U], "zero");
        ASSERT_EQUAL(constantRow[0L], "zero");
        ASSERT_EQUAL(constantRow[0UL], "zero");
        ASSERT_EQUAL(constantRow[0LL], "zero");
        ASSERT_EQUAL(constantRow[0ULL], "zero");
        ASSERT_EQUAL(row[1], "one");
        ASSERT_EQUAL(constantRow[size_t{1}], "one");
        ASSERT_THROW(row[-1], SException);
        ASSERT_THROW(constantRow[2U], SException);
    }

    void testMissingAndShortRows()
    {
        SQResult result;
        result.setHeaders({"first", "last"});
        SQResultRow fullRow(result);
        fullRow.push_back("a");
        fullRow.push_back("b");
        ASSERT_EQUAL(fullRow["last"], "b");

        SQResultRow shortRow(result);
        shortRow.push_back("a");
        const SQResultRow& constantShortRow = shortRow;
        ASSERT_THROW(shortRow["last"], SException);
        ASSERT_THROW(constantShortRow["last"], SException);
        ASSERT_THROW(fullRow["missing"], SException);
        ASSERT_THROW(fullRow[nullptr], SException);
        ASSERT_THROW(constantShortRow[nullptr], SException);
        SQResultRow orphan;
        ASSERT_THROW(orphan["first"], SException);

        result.setHeaders({"same", "same"});
        ASSERT_EQUAL(fullRow["same"], "a");
        result.setHeaders({"only"});
        ASSERT_THROW(fullRow["missing"], SException);
        ASSERT_EQUAL(fullRow["only"], "a");
    }

    void testHeaderReplacement()
    {
        SQResult result;

        result.setHeaders({"name", "value"});
        SQResultRow row(result);
        row.push_back("first");
        row.push_back("second");
        result.emplace_back(move(row));
        ASSERT_EQUAL(result[0]["name"], "first");
        result.setHeaders({"value", "name"});
        ASSERT_EQUAL(result[0]["name"], "second");
        ASSERT_EQUAL(result.getHeaders()[0], "value");
        result.clear();
        result.setHeaders({"name"});
        SQResultRow replacement(result);
        replacement.push_back("replacement");
        result.emplace_back(move(replacement));
        ASSERT_EQUAL(result[0]["name"], "replacement");
    }

    void testCopyAndRowOwnership()
    {
        SQResult source;
        source.setHeaders({"name", "value"});
        SQResultRow row(source);
        row.push_back("first");
        row.push_back("second");
        source.emplace_back(move(row));
        ASSERT_EQUAL(source[0]["name"], "first");

        SQResult copied(source);
        copied.setHeaders({"value", "name"});
        ASSERT_EQUAL(copied[0]["name"], "second");
        ASSERT_EQUAL(source[0]["name"], "first");
        source = copied;
        ASSERT_EQUAL(source[0]["name"], "second");
        const SQResult& sameSource = source;
        source = sameSource;
        ASSERT_EQUAL(source[0]["name"], "second");
        copied.clear();
        ASSERT_EQUAL(source[0]["name"], "second");

        vector<SQResultRow> rows{source[0]};
        SQResult constructed(move(rows), {"name", "value"});
        source.clear();
        ASSERT_EQUAL(constructed[0]["name"], "first");

        SQResult appended;
        appended.setHeaders({"value", "name"});
        SQResultRow detached = constructed[0];
        appended.emplace_back(move(detached));
        ASSERT_EQUAL(appended[0]["name"], "second");
    }

    void testDeserialization()
    {
        SQResult result;
        ASSERT_TRUE(result.deserialize(R"([{"name":"first","value":"second"}])"));
        ASSERT_EQUAL(result[0]["name"], "first");
        ASSERT_TRUE(result.deserialize(R"({"headers":["value","name"],"rows":[["third","fourth"]]})"));
        ASSERT_EQUAL(result[0]["name"], "fourth");
        // SQLite column order and repeated names survive parsing; SQL NULL remains an empty cell.
        ASSERT_TRUE(result.deserialize(R"([{"z":1,"a":null,"z":3},{"z":4,"a":5,"z":6}])"));
        ASSERT_TRUE(result.getHeaders() == vector<string>({"z", "a", "z"}));
        ASSERT_EQUAL(result[0][0], "1");
        ASSERT_EQUAL(result[0][1], "");
        ASSERT_EQUAL(result[0][2], "3");
        ASSERT_EQUAL(result[1][2], "6");

        // Legacy Bedrock results retain the literal null string.
        ASSERT_TRUE(result.deserialize(R"({"headers":["nothing"],"rows":[[null]]})"));
        ASSERT_EQUAL(result[0][0], "null");

        ASSERT_FALSE(result.deserialize("invalid"));
        ASSERT_TRUE(result.empty());
        ASSERT_TRUE(result.getHeaders().empty());
        ASSERT_TRUE(result.deserialize(R"([{"name":"last"}])"));
        ASSERT_EQUAL(result[0]["name"], "last");
    }

    void testSQueryReuse()
    {
        sqlite3* rawDatabase = nullptr;
        ASSERT_EQUAL(sqlite3_open(":memory:", &rawDatabase), SQLITE_OK);
        unique_ptr<sqlite3, decltype(& sqlite3_close)> database(rawDatabase, sqlite3_close);
        SQResult result;
        ASSERT_EQUAL(SQuery(database.get(), "SELECT 'first' AS name, 'second' AS value;", result), SQLITE_OK);
        ASSERT_EQUAL(result[0]["name"], "first");
        ASSERT_EQUAL(SQuery(database.get(), "SELECT 'third' AS value, 'fourth' AS name;", result), SQLITE_OK);
        ASSERT_EQUAL(result[0]["name"], "fourth");
        ASSERT_EQUAL(SQuery(database.get(), "SELECT 'empty' AS name WHERE 0;", result), SQLITE_OK);
        ASSERT_TRUE(result.empty());
        ASSERT_EQUAL(result.getHeaders()[0], "name");
    }

    void testConcurrentReaders()
    {
        SQResult result;
        result.setHeaders({"name", "value"});
        SQResultRow row(result);
        row.push_back("first");
        row.push_back("second");
        result.emplace_back(move(row));
        const SQResult& sharedResult = result;
        latch start(8);
        vector<pair<thread, future<bool>>> readers;
        for (int index = 0; index < 8; ++index) {
            readers.push_back(SThread([&] {
                start.arrive_and_wait();
                for (int repetition = 0; repetition < 1000; ++repetition) {
                    if (sharedResult[0]["name"] != "first" || sharedResult[0]["value"] != "second") {
                        return false;
                    }
                }
                return true;
            }));
        }
        for (auto& reader : readers) {
            reader.first.join();
        }
        for (auto& reader : readers) {
            ASSERT_TRUE(reader.second.get());
        }
    }
} __SQResultTest;
