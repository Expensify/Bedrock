#include <time.h>
#include <unistd.h>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>
#include <test/tests/jobs/JobTestHelper.h>

// Get the difference in seconds between a and b
uint64_t absoluteDiff(time_t a, time_t b) {
    if (a > b) {
        return a - b;
    }
    return b - a;
}

// Retrieve job data.
SQResult getAllJobData(BedrockTester& tester) {
    SData request;
    request.methodLine = "Query";
    request["query"] = "SELECT jobID, state, lastRun, nextRun FROM jobs;";
    request["nowhere"] = "true";
    request["format"] = "json";
    auto jobJSON = tester.executeWaitMultipleData({request});
    SQResult jobData;
    jobData.deserialize(jobJSON[0].content);
    return jobData;
}

struct GetJobsTest : tpunit::TestFixture {
    GetJobsTest()
        : tpunit::TestFixture("GetJobs",
                              TEST(GetJobsTest::getJobs)) { }

    static constexpr auto jobName = "TestJobName";
    void getJobs() {
        // Create a tester.
        BedrockTester tester({{"-plugins", "Jobs,DB"}}, {});

        // Create some jobs.
        vector<string> repeatModifiers = {
            "SCHEDULED, +1 HOUR",
            "STARTED, +1 DAY",
            "FINISHED, +7 DAYS",
        };
        vector<SData> requests;
        auto scheduledTime = SUNQUOTED_CURRENT_TIMESTAMP();
        for (auto& modifier : repeatModifiers) {
            SData request("CreateJob");
            request["name"] = jobName;
            request["repeat"] = modifier;
            request["firstRun"] = scheduledTime;
            request["retryAfter"] = "+5 MINUTES";
            requests.push_back(request);
        }
        auto createResults = tester.executeWaitMultipleData(requests);

        // And save their IDs in the same order we created them.
        vector<uint64_t> jobIDs;
        for (auto& result : createResults) {
            auto jsonResponse = SParseJSONObject(result.content);
            jobIDs.push_back(stoull(jsonResponse["jobID"]));
        }

        // Now we sleep for a couple seconds to verify that "scheduled" and "started" are different times.
        sleep(5);

        // This should return three jobs.
        SData request("GetJobs");
        request["name"] = jobName;
        request["numResults"] = "5";
        auto runResult = tester.executeWaitMultipleData({request});
        auto jsonResponse = SParseJSONObject(runResult[0].content);
        auto jsonJobs = SParseJSONArray(jsonResponse["jobs"]);
        ASSERT_EQUAL(jsonJobs.size(), 3);

        // Now we should have three jobs that are "running".
        // Right now, they should all be scheduled to run again 5 minutes from when they started, because of
        // `retryAfter`. We allow this to be within 3 seconds, because it's possible that the timestamps are generated
        // in sequential seconds, and so these can end up being, for instance, 5 minutes and 1 second different.
        SQResult jobData = getAllJobData(tester);
        for (auto& row : jobData) {
            // Assert that the difference between "lastRun + 5min" and "nextRun" is less than 3 seconds.
            ASSERT_LESS_THAN(absoluteDiff(JobTestHelper::getTimestampForDateTimeString(row[2]) + 5 * 60, JobTestHelper::getTimestampForDateTimeString(row[3])), 3);
            ASSERT_EQUAL(row[1], "RUNQUEUED");
        }

        // Sleep 5 more seconds to differentiate between "started" and "finished", and then finish them all.
        sleep(5);

        // Finish them all. They should all get rescheduled.
        requests.clear();
        for (auto& jobID : jobIDs) {
            SData request("FinishJob");
            request["jobID"] = to_string(jobID);
            requests.push_back(request);
        }
        auto finishedTime = SUNQUOTED_CURRENT_TIMESTAMP();
        auto finishResults = tester.executeWaitMultipleData(requests);

        // Now see what they look like.
        jobData = getAllJobData(tester);
        for (auto& row : jobData) {
            // Should be queued again.
            ASSERT_EQUAL(row[1], "QUEUED");

            // Let's see if it's scheduled at the right time.
            if (stoull(row[0]) == jobIDs[0]) {
                // Assert that the difference between "lastRun + 1hour - 5 seconds" and "nextRun" is less than 3 seconds.
                // We remove 5 seconds here because the job is rescheduled for when it was _scheduled_, not when it started/finished
                ASSERT_LESS_THAN(absoluteDiff(JobTestHelper::getTimestampForDateTimeString(row[2]) + 1 * 60 * 60 - 5, JobTestHelper::getTimestampForDateTimeString(row[3])), 3);
            } else if (stoull(row[0]) == jobIDs[1]) {
                // Assert that the difference between "lastRun + 1day" and "nextRun" is less than 3 seconds.
                ASSERT_LESS_THAN(absoluteDiff(JobTestHelper::getTimestampForDateTimeString(row[2]) + 1 * 60 * 60 * 24, JobTestHelper::getTimestampForDateTimeString(row[3])), 3);
            } else if (stoull(row[0]) == jobIDs[2]) {
                // Assert that the difference between "finishedTime + 7days" and "nextRun" is less than 3 seconds.
                ASSERT_LESS_THAN(absoluteDiff(JobTestHelper::getTimestampForDateTimeString(finishedTime) + 7 * 60 * 60 * 24, JobTestHelper::getTimestampForDateTimeString(row[3])), 3);
            } else {
                // It should be one of the above three.
                ASSERT_TRUE(false);
            }
        }
    }
} __GetJobsTest;
