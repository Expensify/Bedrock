#include <libstuff/libstuff.h>
#include <test/lib/tpunit++.hpp>

struct SIsValidSQLiteDateModifierTest : tpunit::TestFixture
{
    SIsValidSQLiteDateModifierTest()
        : tpunit::TestFixture("SIsValidSQLiteDateModifier",
                              TEST(SIsValidSQLiteDateModifierTest::test))
    {
    }

    void test()
    {
        // +1 for every timeframe
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+1 SECOND"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+1 MINUTE"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+1 HOUR"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+1 DAY"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+1 MONTH"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+1 YEAR"));

        // -1 for every timeframe
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-1 SECOND"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-1 MINUTE"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-1 HOUR"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-1 DAY"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-1 MONTH"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-1 YEAR"));

        // +999 for every timeframe (singular)
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 SECOND"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 MINUTE"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 HOUR"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 DAY"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 MONTH"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 YEAR"));

        // -999 for every timeframe (singular)
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 SECOND"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 MINUTE"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 HOUR"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 DAY"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 MONTH"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 YEAR"));

        // +999 for every timeframe (plural)
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 SECONDS"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 MINUTES"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 HOURS"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 DAYS"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 MONTHS"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+999 YEARS"));

        // -999 for every timeframe (plural)
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 SECONDS"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 MINUTES"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 HOURS"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 DAYS"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 MONTHS"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-999 YEARS"));

        // We can go up to 8 digits with seconds
        EXPECT_TRUE(SIsValidSQLiteDateModifier("+99999999 SECONDS"));
        EXPECT_FALSE(SIsValidSQLiteDateModifier("+999999999 SECONDS"));
        EXPECT_TRUE(SIsValidSQLiteDateModifier("-99999999 SECONDS"));
        EXPECT_FALSE(SIsValidSQLiteDateModifier("-999999999 SECONDS"));

        // We can't go above 3 digits for others
        EXPECT_FALSE(SIsValidSQLiteDateModifier("+9999 MINUTES"));
        EXPECT_FALSE(SIsValidSQLiteDateModifier("+9999 HOURS"));
        EXPECT_FALSE(SIsValidSQLiteDateModifier("+9999 DAYS"));
        EXPECT_FALSE(SIsValidSQLiteDateModifier("+9999 MONTHS"));
        EXPECT_FALSE(SIsValidSQLiteDateModifier("+9999 YEARS"));
    }
} __SIsValidSQLiteDateModifierTest;
