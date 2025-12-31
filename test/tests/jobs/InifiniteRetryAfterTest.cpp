#include <unistd.h>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>

struct InfiniteRetryAfterJobTest : tpunit::TestFixture
{
    InfiniteRetryAfterJobTest()
        : tpunit::TestFixture("InfiniteRetryAfter",
                              TEST(InfiniteRetryAfterJobTest::testInfiniteTries),
                              TEST(InfiniteRetryAfterJobTest::testRepeatJobWithThreeTries)
        )
    {
    }

    /**
     * This tests that a job with a retryAfter won't get requeued forever.
     * We want it to fail after 10 tries.
     */
    void testInfiniteTries()
    {
        BedrockTester tester = BedrockTester({{"-plugins", "Jobs,DB"}}, {});

        // Create a job
        SData createJob("CreateJob");
        createJob["name"] = "infinite-job";
        createJob["retryAfter"] = "+1 SECOND";
        const string jobID = tester.executeWaitVerifyContentTable(createJob)["jobID"];

        // Get the job 10 times in a row
        for (size_t i = 0; i <= 10; ++i) {
            SData getJobs("GetJob");
            getJobs["name"] = "infinite-job";
            STable getJobResponse = tester.executeWaitVerifyContentTable(getJobs);

            // Verify the job state:
            SQResult result;
            tester.readDB("SELECT state, JSON_EXTRACT(data, '$.retryAfterCount') FROM jobs WHERE jobID = " + SQ(jobID) + ";", result);
            ASSERT_FALSE(result.empty());
            const string state = result[0][0];
            const size_t retryAfterCount = SToInt(result[0][1]);
            if (i == 10) {
                // For the last loop, after the 10th time, it should be FAILED (and the jobs isn't returned, since it gets failed)
                EXPECT_EQUAL(state, "FAILED");
            } else {
                // For the first 10 times, it should be in the RUNQUEUED state
                EXPECT_EQUAL(retryAfterCount, i + 1);
                EXPECT_EQUAL(state, "RUNQUEUED");
                EXPECT_EQUAL(getJobResponse["jobID"], jobID);

                // Wait for the retryAfter to kick in
                sleep(2);
            }
        }
    }

    /**
     * This tests that a job with a retryAfter finished after 5 tries will have its retryAfterCount unset.
     */
    void testRepeatJobWithThreeTries()
    {
        BedrockTester tester = BedrockTester({{"-plugins", "Jobs,DB"}}, {});

        // Create a job
        SData createJob("CreateJob");
        createJob["name"] = "not-infinite-job";
        createJob["retryAfter"] = "+1 SECOND";
        createJob["repeat"] = "SCHEDULED, +1 DAY";
        const string jobID = tester.executeWaitVerifyContentTable(createJob)["jobID"];

        // Get the job 3 times in a row
        for (size_t i = 0; i <= 3; ++i) {
            SData getJobs("GetJob");
            getJobs["name"] = "not-infinite-job";
            STable getJobResponse = tester.executeWaitVerifyContentTable(getJobs);
            EXPECT_EQUAL(getJobResponse["jobID"], jobID);

            // Verify the job state:
            SQResult result;
            tester.readDB("SELECT state, JSON_EXTRACT(data, '$.retryAfterCount') FROM jobs WHERE jobID = " + SQ(jobID) + ";", result);
            ASSERT_FALSE(result.empty());
            const string state = result[0][0];
            const size_t retryAfterCount = SToInt(result[0][1]);
            EXPECT_EQUAL(retryAfterCount, i + 1);
            EXPECT_EQUAL(state, "RUNQUEUED");

            // Wait for the retryAfter to kick in
            sleep(2);
        }

        // Finish the job
        SData finishJob("FinishJob");
        finishJob["jobID"] = jobID;
        tester.executeWaitVerifyContentTable(finishJob)["jobID"];
        SQResult result;
        tester.readDB("SELECT state, JSON_EXTRACT(data, '$.retryAfterCount') FROM jobs WHERE jobID = " + SQ(jobID) + ";", result);
        ASSERT_FALSE(result.empty());

        // State is QUEUED and retryAfterCount has been removed
        const string state = result[0][0];
        const string retryAfterCount = result[0][1];
        EXPECT_TRUE(retryAfterCount.empty());
        EXPECT_EQUAL(state, "QUEUED");
    }
} __InfiniteRetryAfterJobTest;
