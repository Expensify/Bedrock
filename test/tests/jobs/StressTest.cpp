#include <iostream>
#include <unistd.h>
#include <random>
#include <thread>
#include <mutex>
#include <atomic>
#include <vector>

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
    const int NUM_POLICY_HARVESTER_JOBS = 1000;
    const int JOBS_PER_BATCH = 25;
    const int MAX_TIMEOUT_SECONDS = 10;
    const int NUM_THREADS = 16;

    // Threading synchronization
    std::atomic<bool> allJobsProcessed{false};
    std::atomic<int> activeThreads{0};
    std::atomic<int> totalJobsProcessed{0};
    std::mutex coutMutex;

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
        threadSafePrint("Starting PolicyHarvester stress test with " + to_string(NUM_POLICY_HARVESTER_JOBS) + " jobs...");

        // Step 1: Create PolicyHarvester jobs
        threadSafePrint("Creating " + to_string(NUM_POLICY_HARVESTER_JOBS) + " PolicyHarvester jobs...");
        createPolicyHarvesterJobs();

        // Step 2: Process jobs in batches using GetJobs with 32 threads
        threadSafePrint("Processing jobs with " + to_string(NUM_THREADS) + " threads, batches of " + to_string(JOBS_PER_BATCH) + "...");
        processJobBatchesThreaded();

        // Step 3: Wait for all jobs to complete
        threadSafePrint("Waiting for all jobs to complete...");
        waitForJobCompletion();

        threadSafePrint("PolicyHarvester stress test completed successfully!");
    }

private:
    void threadSafePrint(const string& message) {
        std::lock_guard<std::mutex> lock(coutMutex);
        cout << message << endl;
    }

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

    void processJobBatchesThreaded() {
        // Reset synchronization variables
        allJobsProcessed = false;
        activeThreads = 0;
        totalJobsProcessed = 0;

        // Launch worker threads
        vector<thread> threads;
        for (int i = 0; i < NUM_THREADS; i++) {
            threads.emplace_back([this, i]() {
                this->workerThread(i);
            });
        }

        // Wait for all threads to complete
        for (auto& t : threads) {
            t.join();
        }

        threadSafePrint("All " + to_string(NUM_THREADS) + " threads completed. Total jobs processed: " + to_string(totalJobsProcessed.load()));
    }

    void workerThread(int threadId) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> childCountDist(1, 50);

        activeThreads++;
        threadSafePrint("Thread " + to_string(threadId) + " started");

        int jobsProcessedByThread = 0;

        while (!allJobsProcessed) {
            // Get a batch of jobs
            SData getJobsRequest("GetJobs");
            getJobsRequest["name"] = "*";
            getJobsRequest["numResults"] = to_string(JOBS_PER_BATCH);

            STable response;
            try {
                response = tester->executeWaitVerifyContentTable(getJobsRequest);
            } catch (const SException& e) {
                if (string(e.what()).find("404 No job found") != string::npos) {
                    // No more jobs - check if we should exit
                    break;
                }
                threadSafePrint("Thread " + to_string(threadId) + " GetJobs error: " + string(e.what()));
                // Sleep briefly and continue
                usleep(100000); // 100ms
                continue;
            }

            // Parse the jobs from the response
            list<string> jobsArray = SParseJSONArray(response["jobs"]);
            if (jobsArray.empty()) {
                // No jobs in this batch - exit
                break;
            }

            threadSafePrint("Thread " + to_string(threadId) + " processing batch of " + to_string(jobsArray.size()) + " jobs");

            // Process each job in the batch
            for (const string& jobStr : jobsArray) {
                STable job = SParseJSONObject(jobStr);
                string jobName = job["name"];
                string jobID = job["jobID"];

                if (jobName.find("PolicyHarvester") != string::npos) {
                    // Handle PolicyHarvester job
                    handlePolicyHarvesterJobThreadSafe(job, childCountDist(gen), threadId);
                } else if (jobName.find("UserHarvester") != string::npos) {
                    // Handle UserHarvester job - simply finish it
                    handleUserHarvesterJobThreadSafe(jobID, threadId);
                }

                jobsProcessedByThread++;
                totalJobsProcessed++;
            }

            // Small delay to reduce contention between threads
            usleep(10000); // 10ms
        }

        activeThreads--;
        threadSafePrint("Thread " + to_string(threadId) + " finished. Jobs processed: " + to_string(jobsProcessedByThread));

        // If this is the last active thread, mark all jobs as processed
        if (activeThreads == 0) {
            allJobsProcessed = true;
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

    void handlePolicyHarvesterJobThreadSafe(const STable& job, int childCount, int threadId) {
        auto jobIDIt = job.find("jobID");
        auto jobDataIt = job.find("data");
        ASSERT_TRUE(jobIDIt != job.end() && jobDataIt != job.end());

        string jobID = jobIDIt->second;
        STable jobData = SParseJSONObject(jobDataIt->second);
        string policyID = jobData["policyID"];
        bool isWaiting = (jobData["isWaiting"] == "true");

        if (isWaiting) {
            // Job is resuming after children completed - finish it
            threadSafePrint("Thread " + to_string(threadId) + " resuming PolicyHarvester job " + jobID + " (policy " + policyID + ")");

            // Update job to set isWaiting = false
            STable updatedData = jobData;
            updatedData["isWaiting"] = "false";

            SData finishRequest("FinishJob");
            finishRequest["jobID"] = jobID;
            finishRequest["data"] = SComposeJSONObject(updatedData);

            try {
                tester->executeWaitVerifyContent(finishRequest);
            } catch (const SException& e) {
                threadSafePrint("Thread " + to_string(threadId) + " ERROR finishing PolicyHarvester job " + jobID + ": " + string(e.what()));
                // Continue processing other jobs instead of crashing
            }
        } else {
            // Job is starting - create child UserHarvester jobs
            threadSafePrint("Thread " + to_string(threadId) + " creating " + to_string(childCount) + " UserHarvester children for PolicyHarvester " + jobID + " (policy " + policyID + ")");

            try {
                createUserHarvesterJobs(jobID, policyID, childCount);

                // Mark the parent as waiting and finish it (will become PAUSED)
                STable updatedData = jobData;
                updatedData["isWaiting"] = "true";

                SData finishRequest("FinishJob");
                finishRequest["jobID"] = jobID;
                finishRequest["data"] = SComposeJSONObject(updatedData);
                tester->executeWaitVerifyContent(finishRequest);
            } catch (const SException& e) {
                threadSafePrint("Thread " + to_string(threadId) + " ERROR processing PolicyHarvester job " + jobID + ": " + string(e.what()));
                // Continue processing other jobs instead of crashing
            }
        }
    }

    void handleUserHarvesterJobThreadSafe(const string& jobID, int threadId) {
        // Simply finish the UserHarvester job
        threadSafePrint("Thread " + to_string(threadId) + " finishing UserHarvester job " + jobID);
        SData finishRequest("FinishJob");
        finishRequest["jobID"] = jobID;

        try {
            tester->executeWaitVerifyContent(finishRequest);
        } catch (const SException& e) {
            threadSafePrint("Thread " + to_string(threadId) + " ERROR finishing UserHarvester job " + jobID + ": " + string(e.what()));
            // Continue processing other jobs instead of crashing
        }
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
                threadSafePrint("All jobs completed successfully!");
                return;
            }

            threadSafePrint("Still waiting for " + to_string(jobResults.size()) + " jobs to complete...");

            // Sleep for 1 second and try again
            sleep(1);
            timeoutSeconds--;
        }

        // Timeout reached - report remaining jobs and fail
        threadSafePrint("TIMEOUT: Jobs still remaining after " + to_string(MAX_TIMEOUT_SECONDS) + " seconds:");
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

                threadSafePrint("Remaining jobs report:");
        threadSafePrint("=====================");

        map<string, int> stateCount;
        for (const auto& row : jobResults.rows) {
            stateCount[row[2]]++;
            threadSafePrint("JobID: " + row[0] + ", Name: " + row[1] + ", State: " + row[2]);
        }

        threadSafePrint("=====================");
        threadSafePrint("Summary by state:");
        for (const auto& entry : stateCount) {
            threadSafePrint(entry.first + ": " + to_string(entry.second) + " jobs");
        }
        threadSafePrint("Total remaining: " + to_string(jobResults.size()) + " jobs");
    }

} __StressTest;