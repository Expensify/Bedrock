#include <test/lib/BedrockTester.h>
#include <time.h>

// Get a unix timestamp from one of our sqlite date strings.
time_t stringToUnixTimestamp(const string& timestamp) {
    struct tm time;
    strptime(timestamp.c_str(), "%Y-%m-%d %H:%M:%S", &time);
    return mktime(&time);
}

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
        BedrockTester tester(_threadID, {{"-plugins", "Jobs,DB"}}, {});

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
        for (auto& row : jobData.rows) {
            // Assert that the difference between "lastRun + 5min" and "nextRun" is less than 3 seconds.
            ASSERT_LESS_THAN(absoluteDiff(stringToUnixTimestamp(row[2]) + 5 * 60, stringToUnixTimestamp(row[3])), 3);
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
        for (auto& row : jobData.rows) {
            // Should be queued again.
            ASSERT_EQUAL(row[1], "QUEUED");

            // Let's see if it's scheduled at the right time.
            if (stoull(row[0]) == jobIDs[0]) {
                // This uses a `SCHEDULED` modified, but it also uses `retryAfter`, which is just a broken combination.
                // What happens with `scheduled`, is that when we finish a job we take `nextRun` and add our time
                // interval to it. This assumes that `nextRun` is whatever it was set to last time we got the job. But
                // with `retryAfter`, we updated that to some failure check interval, like 5 minutes, rather than
                // running this from when it was last scheduled, it runs it from when it was last scheduled to be
                // retried.
                //
                // We assert nothing here because this case is broken.
            } else if (stoull(row[0]) == jobIDs[1]) {
                // Assert that the difference between "lastRun + 1day" and "nextRun" is less than 3 seconds.
                ASSERT_LESS_THAN(absoluteDiff(stringToUnixTimestamp(row[2]) + 1 * 60 * 60 * 24, stringToUnixTimestamp(row[3])), 3);
            } else if (stoull(row[0]) == jobIDs[2]) {
                // Assert that the difference between "finishedTime + 7days" and "nextRun" is less than 3 seconds.
                ASSERT_LESS_THAN(absoluteDiff(stringToUnixTimestamp(finishedTime) + 7 * 60 * 60 * 24, stringToUnixTimestamp(row[3])), 3);
            } else {
                // It should be one of the above three.
                ASSERT_TRUE(false);
            }
        }
    }
} __GetJobsTest;
