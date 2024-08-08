#include <libstuff/SQuery.h>
#include <test/lib/BedrockTester.h>

struct SQueryTest : tpunit::TestFixture {
    SQueryTest() : tpunit::TestFixture(true, "SQuery",
        TEST(SQueryTest::testPrepare)
        )
    {}

    void testPrepare() {
        // single param
        ASSERT_EQUAL(SQuery::prepare("SELECT * FROM accounts WHERE accountID = {accountID};", {{"accountID", 42}}), "SELECT * FROM accounts WHERE accountID = 42;");

        // multiple params
        ASSERT_EQUAL(SQuery::prepare("SELECT * FROM reports WHERE accountID = {accountID} AND reportID = {reportID};", {{"accountID", 42}, {"reportID", 52}}), "SELECT * FROM reports WHERE accountID = 42 AND reportID = 52;");

        // multiple params, multiple instances
        ASSERT_EQUAL(SQuery::prepare("SELECT * FROM reportActions WHERE action == {actionCreated} OR (action != {actionCreated} AND created < {timestamp});", {{"actionCreated", "CREATED"}, {"timestamp", "2024-01-01 12:00:00"}}), "SELECT * FROM reportActions WHERE action == 'CREATED' OR (action != 'CREATED' AND created < '2024-01-01 12:00:00');");
    }
} __SQueryTest;
