#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/tpunit++.hpp>

struct SQLiteConflictParseTest : tpunit::TestFixture
{
    SQLiteConflictParseTest()
        : tpunit::TestFixture("SQLiteConflictParse",
                              TEST(SQLiteConflictParseTest::wal2PageConflict),
                              TEST(SQLiteConflictParseTest::hctreeTableConflict),
                              TEST(SQLiteConflictParseTest::hctreeIndexConflict),
                              TEST(SQLiteConflictParseTest::lockKeyScopedByLocation),
                              TEST(SQLiteConflictParseTest::unrecognized))
    {
    }

    void wal2PageConflict()
    {
        // The WAL2 backend reports the conflicting page, which is used directly as the lock key.
        auto [page, location] = SQLite::parseConflictMessage(
            "cannot commit CONCURRENT transaction - conflict at page 1854553 "
            "(read/write page; part of db table reports; content=0D00000009007100...)");
        ASSERT_EQUAL(page, 1854553);
        ASSERT_EQUAL(location, "reports");

        auto [indexPage, indexLocation] = SQLite::parseConflictMessage(
            "cannot commit CONCURRENT transaction - conflict at page 1594810 "
            "(read/write page; part of db index reportActions.reportActionsAccountIDCreatedComment; content=0A04...)");
        ASSERT_EQUAL(indexPage, 1594810);
        ASSERT_EQUAL(indexLocation, "reportActions.reportActionsAccountIDCreatedComment");
    }

    void hctreeTableConflict()
    {
        // The HC-Tree backend reports a table+rowid, which we turn into a non-zero row-level lock key.
        auto [key, location] = SQLite::parseConflictMessage(
            "cannot commit CONCURRENT transaction - conflict in table reports - "
            "range (1,5) conflicts with write to rowid 3");
        ASSERT_NOT_EQUAL(key, 0);
        ASSERT_EQUAL(location, "reports");

        // The same table+rowid must always produce the same key (so retries serialize against each other).
        auto [key2, location2] = SQLite::parseConflictMessage(
            "cannot commit CONCURRENT transaction - conflict in table reports - "
            "range (2,9) conflicts with write to rowid 3");
        ASSERT_EQUAL(key, key2);

        // A different rowid in the same table produces a different key (so unrelated rows don't serialize).
        auto [key3, location3] = SQLite::parseConflictMessage(
            "cannot commit CONCURRENT transaction - conflict in table reports - "
            "range (1,5) conflicts with write to rowid 4");
        ASSERT_NOT_EQUAL(key, key3);
    }

    void hctreeIndexConflict()
    {
        auto [key, location] = SQLite::parseConflictMessage(
            "cannot commit CONCURRENT transaction - conflict in index reportActions.someIndex - "
            "range (a,c) conflicts with write to key X'0A045B00'");
        ASSERT_NOT_EQUAL(key, 0);
        ASSERT_EQUAL(location, "reportActions.someIndex");
    }

    void lockKeyScopedByLocation()
    {
        // The same rowid in different tables must not collide, or unrelated commands would serialize.
        auto [keyA, locationA] = SQLite::parseConflictMessage(
            "cannot commit CONCURRENT transaction - conflict in table reports - "
            "range (1,5) conflicts with write to rowid 3");
        auto [keyB, locationB] = SQLite::parseConflictMessage(
            "cannot commit CONCURRENT transaction - conflict in table policies - "
            "range (1,5) conflicts with write to rowid 3");
        ASSERT_NOT_EQUAL(keyA, keyB);
    }

    void unrecognized()
    {
        // Anything that isn't a recognized conflict message yields no lock.
        auto [page, location] = SQLite::parseConflictMessage("some unrelated sqlite log message");
        ASSERT_EQUAL(page, 0);
        ASSERT_TRUE(location.empty());

        // A "cannot commit" message in an unknown format also yields no lock rather than a bogus key.
        auto [page2, location2] = SQLite::parseConflictMessage("cannot commit something we don't recognize");
        ASSERT_EQUAL(page2, 0);
        ASSERT_TRUE(location2.empty());
    }
} __SQLiteConflictParseTest;
