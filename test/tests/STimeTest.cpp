#include <libstuff/libstuff.h>
#include <test/lib/BedrockTester.h>

struct STimeTest : tpunit::TestFixture {
    STimeTest() : tpunit::TestFixture("STimeTest",
                                     TEST(STimeTest::testSTimestampToEpoch),
                                     TEST(STimeTest::testSTimestampMSToEpoch)
                                     )
    { }

    /**
     * Test STimestampToEpoch with regular format (no fractional seconds)
     */
    void testSTimestampToEpoch() {
        uint64_t result = STimestampToEpoch("%Y-%m-%d %H:%M:%S", "2025-10-13 14:30:25");
        ASSERT_EQUAL(1760365825, result);

        result = STimestampToEpoch("%Y-%m-%d %H:%M:%S", "2020-01-01 00:00:00");
        ASSERT_EQUAL(1577836800, result);

        result = STimestampToEpoch("%Y-%m-%d %H:%M:%S", "1970-01-01 00:00:00");
        ASSERT_EQUAL(0, result);
    }

    /**
     * Test STimestampMSToEpoch with fractional seconds format
     */
    void testSTimestampMSToEpoch() {
        uint64_t result = STimestampMSToEpoch("%Y-%m-%d %H:%M:%f", "2025-10-13 14:30:25.192");
        ASSERT_EQUAL(1760365825192, result);

        result= STimestampMSToEpoch("%Y-%m-%d %H:%M:%f", "2021-06-15 09:15:30.5");
        ASSERT_EQUAL(1623748530500, result);

        result = STimestampMSToEpoch("%Y-%m-%d %H:%M:%f", "2019-03-20 12:00:00.000");
        ASSERT_EQUAL(1553083200000, result);
    }

} __STimeTest;
