#include <iostream>
#include <unistd.h>

#include <libstuff/SData.h>
#include <libstuff/SQResult.h>
#include <test/lib/BedrockTester.h>
#include <test/tests/jobs/JobTestHelper.h>

struct SequentialJobTest : tpunit::TestFixture
{
    SequentialJobTest()
        : tpunit::TestFixture("SequentialJob",
                              BEFORE_CLASS(SequentialJobTest::setupClass),
                              TEST(SequentialJobTest::createFirstJobWithSequentialKey),
                              TEST(SequentialJobTest::createSecondJobWaitsWhenFirstIsQueued),
                              TEST(SequentialJobTest::createSecondJobWaitsWhenFirstIsRunning),
                              TEST(SequentialJobTest::finishJobPromotesWaitingJob),
                              TEST(SequentialJobTest::failJobPromotesWaitingJob),
                              TEST(SequentialJobTest::deleteJobPromotesWaitingJob),
                              TEST(SequentialJobTest::multipleJobsPreserveOrder),
                              TEST(SequentialJobTest::differentSequentialKeysDontInterfere),
                              AFTER(SequentialJobTest::tearDown),
                              AFTER_CLASS(SequentialJobTest::tearDownClass))
    {
    }

    BedrockTester* tester;

    void setupClass()
    {
        tester = new BedrockTester({{"-plugins", "Jobs,DB"}}, {});
    }

    // Reset the jobs table
    void tearDown()
    {
        SData command("Query");
        command["query"] = "DELETE FROM jobs WHERE jobID > 0;";
        tester->executeWaitVerifyContent(command);
    }

    void tearDownClass()
    {
        delete tester;
    }

    // First job with sequentialKey should be QUEUED
    void createFirstJobWithSequentialKey()
    {
        SData command("CreateJob");
        command["name"] = "testSequential";
        command["sequentialKey"] = "test_key_1";
        STable response = tester->executeWaitVerifyContentTable(command);
        ASSERT_GREATER_THAN(stol(response["jobID"]), 0);

        SQResult result;
        tester->readDB("SELECT state, sequentialKey FROM jobs WHERE jobID = " + response["jobID"] + ";", result);
        ASSERT_EQUAL(result.size(), 1);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        ASSERT_EQUAL(result[0][1], "test_key_1");
    }

    // Second job with same sequentialKey should be WAITING when first is QUEUED
    void createSecondJobWaitsWhenFirstIsQueued()
    {
        // Create first job
        SData command("CreateJob");
        command["name"] = "testSequential1";
        command["sequentialKey"] = "test_key_2";
        STable response1 = tester->executeWaitVerifyContentTable(command);
        string jobID1 = response1["jobID"];

        // Create second job with same sequentialKey
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testSequential2";
        command["sequentialKey"] = "test_key_2";
        STable response2 = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response2["jobID"];

        // Verify first job is QUEUED
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID1 + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");

        // Verify second job is WAITING
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "WAITING");
    }

    // Second job with same sequentialKey should be WAITING when first is RUNNING
    void createSecondJobWaitsWhenFirstIsRunning()
    {
        // Create first job
        SData command("CreateJob");
        command["name"] = "testSequential1";
        command["sequentialKey"] = "test_key_3";
        STable response1 = tester->executeWaitVerifyContentTable(command);
        string jobID1 = response1["jobID"];

        // Get the first job to put it in RUNNING state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "testSequential1";
        tester->executeWaitVerifyContent(command);

        // Verify first job is RUNNING
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID1 + ";", result);
        ASSERT_EQUAL(result[0][0], "RUNNING");

        // Create second job with same sequentialKey
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testSequential2";
        command["sequentialKey"] = "test_key_3";
        STable response2 = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response2["jobID"];

        // Verify second job is WAITING
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "WAITING");
    }

    // FinishJob should promote the next WAITING job with same sequentialKey
    void finishJobPromotesWaitingJob()
    {
        // Create first job
        SData command("CreateJob");
        command["name"] = "testSequential1";
        command["sequentialKey"] = "test_key_4";
        STable response1 = tester->executeWaitVerifyContentTable(command);
        string jobID1 = response1["jobID"];

        // Create second job
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testSequential2";
        command["sequentialKey"] = "test_key_4";
        STable response2 = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response2["jobID"];

        // Verify second job is WAITING
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "WAITING");

        // Get first job to put it in RUNNING state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "testSequential1";
        tester->executeWaitVerifyContent(command);

        // Finish first job
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID1;
        tester->executeWaitVerifyContent(command);

        // Verify first job is deleted
        tester->readDB("SELECT * FROM jobs WHERE jobID = " + jobID1 + ";", result);
        ASSERT_TRUE(result.empty());

        // Verify second job is now QUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }

    // FailJob should promote the next WAITING job with same sequentialKey
    void failJobPromotesWaitingJob()
    {
        // Create first job
        SData command("CreateJob");
        command["name"] = "testSequential1";
        command["sequentialKey"] = "test_key_5";
        STable response1 = tester->executeWaitVerifyContentTable(command);
        string jobID1 = response1["jobID"];

        // Create second job
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testSequential2";
        command["sequentialKey"] = "test_key_5";
        STable response2 = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response2["jobID"];

        // Verify second job is WAITING
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "WAITING");

        // Get first job to put it in RUNNING state
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "testSequential1";
        tester->executeWaitVerifyContent(command);

        // Fail first job
        command.clear();
        command.methodLine = "FailJob";
        command["jobID"] = jobID1;
        tester->executeWaitVerifyContent(command);

        // Verify first job is FAILED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID1 + ";", result);
        ASSERT_EQUAL(result[0][0], "FAILED");

        // Verify second job is now QUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }

    // DeleteJob should promote the next WAITING job with same sequentialKey
    void deleteJobPromotesWaitingJob()
    {
        // Create first job
        SData command("CreateJob");
        command["name"] = "testSequential1";
        command["sequentialKey"] = "test_key_6";
        STable response1 = tester->executeWaitVerifyContentTable(command);
        string jobID1 = response1["jobID"];

        // Create second job
        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testSequential2";
        command["sequentialKey"] = "test_key_6";
        STable response2 = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response2["jobID"];

        // Verify second job is WAITING
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "WAITING");

        // Delete first job
        command.clear();
        command.methodLine = "DeleteJob";
        command["jobID"] = jobID1;
        tester->executeWaitVerifyContent(command);

        // Verify first job is deleted
        tester->readDB("SELECT * FROM jobs WHERE jobID = " + jobID1 + ";", result);
        ASSERT_TRUE(result.empty());

        // Verify second job is now QUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }

    // Multiple jobs with same sequentialKey should run in order
    void multipleJobsPreserveOrder()
    {
        // Create three jobs
        SData command("CreateJob");
        command["name"] = "testSequential1";
        command["sequentialKey"] = "test_key_7";
        STable response1 = tester->executeWaitVerifyContentTable(command);
        string jobID1 = response1["jobID"];

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testSequential2";
        command["sequentialKey"] = "test_key_7";
        STable response2 = tester->executeWaitVerifyContentTable(command);
        string jobID2 = response2["jobID"];

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testSequential3";
        command["sequentialKey"] = "test_key_7";
        STable response3 = tester->executeWaitVerifyContentTable(command);
        string jobID3 = response3["jobID"];

        // Verify states: first QUEUED, rest WAITING
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID1 + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "WAITING");
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID3 + ";", result);
        ASSERT_EQUAL(result[0][0], "WAITING");

        // Get and finish first job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "testSequential1";
        tester->executeWaitVerifyContent(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID1;
        tester->executeWaitVerifyContent(command);

        // Verify second job is now QUEUED, third still WAITING
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID2 + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID3 + ";", result);
        ASSERT_EQUAL(result[0][0], "WAITING");

        // Get and finish second job
        command.clear();
        command.methodLine = "GetJob";
        command["name"] = "testSequential2";
        tester->executeWaitVerifyContent(command);
        command.clear();
        command.methodLine = "FinishJob";
        command["jobID"] = jobID2;
        tester->executeWaitVerifyContent(command);

        // Verify third job is now QUEUED
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobID3 + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }

    // Jobs with different sequentialKeys should not interfere with each other
    void differentSequentialKeysDontInterfere()
    {
        // Create jobs with different sequentialKeys
        SData command("CreateJob");
        command["name"] = "testSequentialA";
        command["sequentialKey"] = "key_A";
        STable responseA = tester->executeWaitVerifyContentTable(command);
        string jobIDA = responseA["jobID"];

        command.clear();
        command.methodLine = "CreateJob";
        command["name"] = "testSequentialB";
        command["sequentialKey"] = "key_B";
        STable responseB = tester->executeWaitVerifyContentTable(command);
        string jobIDB = responseB["jobID"];

        // Both should be QUEUED since they have different keys
        SQResult result;
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobIDA + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
        tester->readDB("SELECT state FROM jobs WHERE jobID = " + jobIDB + ";", result);
        ASSERT_EQUAL(result[0][0], "QUEUED");
    }
} __SequentialJobTest;
