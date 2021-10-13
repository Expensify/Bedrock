#include <unistd.h>

#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct InfiniteRetryAfterJobTest : tpunit::TestFixture {
    InfiniteRetryAfterJobTest()
        : tpunit::TestFixture("InfiniteRetryAfter",
                              TEST(InfiniteRetryAfterJobTest::test))
    {}

    /**
     * This tests that a job with a retryAfter won't get requeued forever.
     * We want it to fail after 10 tries.
     */
    void test() {
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

            // Verify we get the job and it is in the RUNQUEUED state
            ASSERT_EQUAL(getJobResponse["jobID"], jobID);
            string state = tester.readDB("SELECT state FROM jobs WHERE jobID = " + SQ(jobID) + ";");

            // Except for the last loop: after the 10th time, it should be FAILED
            if (i == 10) {
                ASSERT_EQUAL(state, "FAILED");
            } else {
                ASSERT_EQUAL(state, "RUNQUEUED");

                // Wait for the retryAfter to kick in
                sleep(2);
            }
        }

    }
} __InfiniteRetryAfterJobTest;
