#include <iostream>
#include <unistd.h>
#include <random>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>

struct StressTest : tpunit::TestFixture {
    StressTest()
        : tpunit::TestFixture("StressTest",
                              BEFORE_CLASS(StressTest::setupClass),
                              TEST(StressTest::policyHarvesterStressTest),
                              AFTER(StressTest::tearDown),
                              AFTER_CLASS(StressTest::tearDownClass)) { }

    BedrockTester* tester;

    // Configuration constants
    static const int NUM_POLICY_HARVESTER_JOBS = 100;
    static const int JOBS_PER_BATCH = 25;
    static const int MAX_TIMEOUT_SECONDS = 10;

    void setupClass() {
        tester = new BedrockTester({{"-plugins", "Jobs,DB"}}, {});
    }

    // Reset the jobs table
    void tearDown() {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass() { delete tester; }

    void policyHarvesterStressTest() {
        cout << "Starting PolicyHarvester stress test with " << NUM_POLICY_HARVESTER_JOBS << " jobs..." << endl;

        // Step 1: Create PolicyHarvester jobs
        cout << "Creating " << NUM_POLICY_HARVESTER_JOBS << " PolicyHarvester jobs..." << endl;
        createPolicyHarvesterJobs();

        // Step 2: Process jobs in batches using GetJobs
        cout << "Processing jobs in batches of " << JOBS_PER_BATCH << "..." << endl;
        processJobBatches();

        // Step 3: Wait for all jobs to complete
        cout << "Waiting for all jobs to complete..." << endl;
        waitForJobCompletion();

        cout << "PolicyHarvester stress test completed successfully!" << endl;
    }

private:
    void createPolicyHarvesterJobs() {
        vector<SData> requests;

        for (int i = 0; i < NUM_POLICY_HARVESTER_JOBS; i++) {
            SData request("CreateJob");
            request["name"] = "www-prod/PolicyHarvester";

            // Create job data with policyID and isWaiting flag
            STable jobData;
            jobData["policyID"] = "policy_" + to_string(i);
            jobData["isWaiting"] = "false";
            request["data"] = SComposeJSONObject(jobData);

            requests.push_back(request);
        }

        // Create all jobs in parallel
        auto results = tester->executeWaitMultipleData(requests);

        // Verify all jobs were created successfully
        ASSERT_EQUAL(results.size(), NUM_POLICY_HARVESTER_JOBS);
        for (const auto& result : results) {
            auto response = SParseJSONObject(result.content);
            ASSERT_TRUE(response.find("jobID") != response.end());
        }
    }

    void processJobBatches() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> childCountDist(1, 50);

        while (true) {
            // Get a batch of jobs
            SData getJobsRequest("GetJobs");
            getJobsRequest["name"] = "*";
            getJobsRequest["numResults"] = to_string(JOBS_PER_BATCH);

            STable response;
            try {
                response = tester->executeWaitVerifyContentTable(getJobsRequest);
            } catch (const SException& e) {
                if (string(e.what()).find("404 No job found") != string::npos) {
                    cout << "No more jobs found, processing complete." << endl;
                    break;
                }
                throw;
            }

            // Parse the jobs from the response
            list<string> jobsArray = SParseJSONArray(response["jobs"]);
            if (jobsArray.empty()) {
                cout << "No jobs in batch, processing complete." << endl;
                break;
            }

            cout << "Processing batch of " << jobsArray.size() << " jobs..." << endl;

            // Process each job in the batch
            for (const string& jobStr : jobsArray) {
                STable job = SParseJSONObject(jobStr);
                string jobName = job["name"];
                string jobID = job["jobID"];

                if (jobName.find("PolicyHarvester") != string::npos) {
                    // Handle PolicyHarvester job
                    handlePolicyHarvesterJob(job, childCountDist(gen));
                } else if (jobName.find("UserHarvester") != string::npos) {
                    // Handle UserHarvester job - simply finish it
                    handleUserHarvesterJob(jobID);
                }
            }
        }
    }

    void handlePolicyHarvesterJob(const STable& job, int childCount) {
        auto jobIDIt = job.find("jobID");
        auto jobDataIt = job.find("data");
        ASSERT_TRUE(jobIDIt != job.end() && jobDataIt != job.end());

        string jobID = jobIDIt->second;
        STable jobData = SParseJSONObject(jobDataIt->second);
        string policyID = jobData["policyID"];
        bool isWaiting = (jobData["isWaiting"] == "true");

        if (isWaiting) {
            // Job is resuming after children completed - finish it
            cout << "Resuming PolicyHarvester job " << jobID << " (policy " << policyID << ")" << endl;

            // Update job to set isWaiting = false
            STable updatedData = jobData;
            updatedData["isWaiting"] = "false";

            SData finishRequest("FinishJob");
            finishRequest["jobID"] = jobID;
            finishRequest["data"] = SComposeJSONObject(updatedData);
            tester->executeWaitVerifyContent(finishRequest);
        } else {
            // Job is starting - create child UserHarvester jobs
            cout << "Creating " << childCount << " UserHarvester children for PolicyHarvester " << jobID << " (policy " << policyID << ")" << endl;

            createUserHarvesterJobs(jobID, policyID, childCount);

            // Mark the parent as waiting and finish it (will become PAUSED)
            STable updatedData = jobData;
            updatedData["isWaiting"] = "true";

            SData finishRequest("FinishJob");
            finishRequest["jobID"] = jobID;
            finishRequest["data"] = SComposeJSONObject(updatedData);
            tester->executeWaitVerifyContent(finishRequest);
        }
    }

    void createUserHarvesterJobs(const string& parentJobID, const string& policyID, int childCount) {
        vector<SData> childRequests;

        for (int i = 0; i < childCount; i++) {
            SData request("CreateJob");
            request["name"] = "www-prod/UserHarvester?policyID=" + policyID + "&email=user" + to_string(i) + "@test.com";
            request["parentJobID"] = parentJobID;

            // Child job data
            STable childData;
            childData["policyID"] = policyID;
            childData["email"] = "user" + to_string(i) + "@test.com";
            childData["policyHarvesterID"] = parentJobID;
            request["data"] = SComposeJSONObject(childData);

            childRequests.push_back(request);
        }

        // Create all child jobs
        auto results = tester->executeWaitMultipleData(childRequests);

        // Verify all children were created
        ASSERT_EQUAL(results.size(), childCount);
    }

    void handleUserHarvesterJob(const string& jobID) {
        // Simply finish the UserHarvester job
        SData finishRequest("FinishJob");
        finishRequest["jobID"] = jobID;
        tester->executeWaitVerifyContent(finishRequest);
    }

    void waitForJobCompletion() {
        int timeoutSeconds = MAX_TIMEOUT_SECONDS;

                while (timeoutSeconds > 0) {
            // Query for remaining jobs
            SData queryRequest("Query");
            queryRequest["query"] = "SELECT jobID, name, state FROM jobs WHERE jobID > 0;";
            queryRequest["format"] = "json";

            auto queryResponses = tester->executeWaitMultipleData({queryRequest});

            // Parse the results
            SQResult jobResults;
            jobResults.deserialize(queryResponses[0].content);

            if (jobResults.empty()) {
                cout << "All jobs completed successfully!" << endl;
                return;
            }

            cout << "Still waiting for " << jobResults.size() << " jobs to complete..." << endl;

            // Sleep for 1 second and try again
            sleep(1);
            timeoutSeconds--;
        }

        // Timeout reached - report remaining jobs and fail
        cout << "TIMEOUT: Jobs still remaining after " << MAX_TIMEOUT_SECONDS << " seconds:" << endl;
        reportRemainingJobs();
        ASSERT_TRUE(false); // Fail the test
    }

        void reportRemainingJobs() {
        SData queryRequest("Query");
        queryRequest["query"] = "SELECT jobID, name, state, data FROM jobs WHERE jobID > 0 ORDER BY state, name;";
        queryRequest["format"] = "json";

        auto queryResponses = tester->executeWaitMultipleData({queryRequest});
        SQResult jobResults;
        jobResults.deserialize(queryResponses[0].content);

        cout << "Remaining jobs report:" << endl;
        cout << "=====================" << endl;

        map<string, int> stateCount;
        for (const auto& row : jobResults.rows) {
            stateCount[row[2]]++;
            cout << "JobID: " << row[0] << ", Name: " << row[1] << ", State: " << row[2] << endl;
        }

        cout << "=====================" << endl;
        cout << "Summary by state:" << endl;
        for (const auto& entry : stateCount) {
            cout << entry.first << ": " << entry.second << " jobs" << endl;
        }
        cout << "Total remaining: " << jobResults.size() << " jobs" << endl;
    }

} __StressTest;