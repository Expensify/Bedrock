#include <iostream>
#include <unistd.h>

#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

struct FailedJobReplyTest : tpunit::TestFixture {
    FailedJobReplyTest() : tpunit::TestFixture("FailedJobReply") {
        registerTests(BEFORE_CLASS(FailedJobReplyTest::setupClass),
                      TEST(FailedJobReplyTest::failSendingResponse),
                      AFTER(FailedJobReplyTest::tearDown),
                      AFTER_CLASS(FailedJobReplyTest::tearDownClass));
    }

    BedrockTester* tester;

    void setupClass() { tester = new BedrockTester({{"-plugins", "Jobs,DB"}}, {});}

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass() { delete tester; }

    // Cannot cancel a job with children
    void failSendingResponse() {

        // Here's a basic job.
        STable job;
        job["name"] = "willFailReply";

        // Here's our test settings. The bool is `retryAfter`.
        list<pair<string, bool>> tests = {
            {"GetJob", false},
            {"GetJob", true},
            {"GetJobs", false},
            {"GetJobs", true},
        };

        for (auto& t : tests) {
            string commandName = t.first;
            bool retryAfter = t.second;

            // Store the job IDs here.
            list<string> createdJobIds;

            // Create the appropriate jobs.
            if (commandName == "GetJob") {
                // Just one job.
                SData command("CreateJob");

                // Copy all the data from our test job.
                for (auto& nvp : job) {
                    command[nvp.first] = nvp.second;
                }

                // Set it to retry well in the future.
                if (retryAfter) {
                    command["retryAfter"] = "+1 HOUR";
                }
                STable response = tester->executeWaitVerifyContentTable(command);
                createdJobIds.push_back(response["jobID"]);
            } else {
                SData command("CreateJobs");
                list<string> jobs;
                for (int i = 0; i < 3; i++) {
                    STable j;
                    for (auto& nvp : job) {
                        j[nvp.first] = nvp.second;
                    }
                    if (retryAfter) {
                        j["retryAfter"] = "+1 HOUR";
                    }
                    jobs.push_back(SComposeJSONObject(j));
                }
                command["jobs"] = SComposeJSONArray(jobs);
                string response = tester->executeWaitVerifyContent(command);

                STable responseJSON = SParseJSONObject(response);
                list<string> ids = SParseJSONArray(responseJSON["jobIDs"]);
                for (auto& id : ids) {
                    createdJobIds.push_back(id);
                }
            }

            // Now that our jobs exist, let's try and get them, but schedule the command to run in the future (1
            // second) so that we know the response can't get delivered.
            SData command(commandName);
            if (commandName == "GetJobs") {
                command["numResults"] = to_string(createdJobIds.size());
            }
            command["name"] = job["name"];
            command["commandExecuteTime"] = to_string(STimeNow() + 1000000);
            tester->executeWaitVerifyContentTable(command, "202");

            // Wait for the command to run, where it will requeue jobs.
            sleep(2);

            // Ok the jobs should all have been gotten, and then re-scheduled by now. We should be able to try again
            // and get them all.
            command.methodLine = commandName;
            if (commandName == "GetJobs") {
                command["numResults"] = to_string(createdJobIds.size());
            }
            command["name"] = job["name"];
            string response;

            // Give it a few tries for the command to get requeued.
            int retries = 3;
            while (retries) {
                try {
                    response = tester->executeWaitVerifyContent(command);

                    // If it doesn't throw, we're done.
                    break;
                } catch (const SException& e) {
                    // We'll retry 404s a few times waiting for the command to requeue.
                    auto it = e.headers.find("originalMethod");
                    if (retries && it != e.headers.end() && it->second.substr(0,3) == "404") {
                        // Retry in a second.
                        retries--;
                        sleep(1);
                        continue;
                    }

                    // But if we're out of retries, or we failed in any other way, re-throw the exception.
                    throw;
                }
            }

            // Verify this looks correct.
            STable responseJSON = SParseJSONObject(response);
            if (commandName == "GetJob") {
                ASSERT_EQUAL(createdJobIds.size(), 1);
                ASSERT_EQUAL(createdJobIds.front(), responseJSON["jobID"]);
            } else {
                list<string> jobs = SParseJSONArray(responseJSON["jobs"]);
                ASSERT_EQUAL(jobs.size(), createdJobIds.size());
                for (auto& j : jobs) {
                    STable job = SParseJSONObject(j);
                    string jobID = job["jobID"];
                    ASSERT_TRUE(find(createdJobIds.begin(), createdJobIds.end(), jobID) != createdJobIds.end());
                }
            }

            // Delete all the jobs for the next test.
            for (auto& id : createdJobIds) {
                command.nameValueMap.clear();
                command["jobID"] = id;
                command.methodLine = "FinishJob";
                try {
                    tester->executeWaitVerifyContent(command);
                } catch (...) {
                    cout << "Failed on 'FinishJob'" << endl;
                    throw;
                }

            }
        }
    }
} __FailedJobReplyTest;
