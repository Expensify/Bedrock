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

    void testInfiniteTries()
    {
        // Given a job that automatically retries when its worker does not finish
        BedrockTester tester = BedrockTester({{"-plugins", "Jobs,DB"}}, {});

        SData createJob("CreateJob");
        createJob["name"] = "infinite-job";
        createJob["retryAfter"] = "+1 SECOND";
        const string jobID = tester.executeWaitVerifyContentTable(createJob)["jobID"];

        for (size_t i = 0; i <= 10; ++i) {
            // When a worker dequeues the job and writes back its snapshot without finishing
            SData getJobs("GetJob");
            getJobs["name"] = "infinite-job";
            STable getJobResponse = tester.executeWaitVerifyContentTable(getJobs);
            if (i < 10) {
                SData updateJob("UpdateJob");
                updateJob["jobID"] = jobID;
                updateJob["data"] = getJobResponse["data"];
                tester.executeWaitVerifyContent(updateJob);
            }

            // Then the stored count advances despite the stale snapshot, and the job fails after ten attempts
            SQResult result;
            tester.readDB("SELECT state, JSON_EXTRACT(data, '$.retryAfterCount') FROM jobs WHERE jobID = " + SQ(jobID) + ";", result);
            ASSERT_FALSE(result.empty());
            const string state = result[0][0];
            const size_t retryAfterCount = SToInt(result[0][1]);
            if (i == 10) {
                EXPECT_EQUAL(state, "FAILED");
            } else {
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
