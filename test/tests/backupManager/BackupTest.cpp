#include <test/lib/BedrockTester.h>

struct BackupTest : tpunit::TestFixture
{
    BackupTest() : tpunit::TestFixture(BEFORE_CLASS(BackupTest::setupClass),
                                            TEST(BackupTest::upload),
                                            TEST(BackupTest::download),
                                            AFTER_CLASS(BackupTest::tearDownClass))
    {
        NAME(CompleteBackup);
    }

    BedrockTester* uploadTester;
    BedrockTester* downloadTester;
    string manifestFileName;
    char cwd[1024];

    void setupClass()
    {
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }
        // Load our SQL file and run the queries from inside of it
        string dbSql = SFileLoad(string(cwd) + "/tests/backupManager/data/db.sql");
        const list<string>& queries = SParseList(dbSql, ';');
        uploadTester = new BedrockTester({{"-db", "/tmp/ebmtest_upload.db"},
                                          {"-backupKeyFile", string(cwd) + "/tests/backupManager/data/key.key"},
                                          {"-plugins", "BackupManager"}},
                                         queries);
    }

    void tearDownClass()
    {
        delete downloadTester;
    }

    void upload()
    {
        // Do the backup
        SData command("BeginBackup");
        command["key"] = "a77477f1609c0e184427f8b39a02eb1c8c1fa3d509b131ba101d7b4019eb81a1";
        command["threads"] = "4";
        command["chunkSize"] = "1048576";
        auto results = uploadTester->executeWaitMultipleData({command}, 1, true);
        ASSERT_EQUAL(results[0].methodLine, "200 OK");
        manifestFileName = results[0].nameValueMap["manifestFileName"];
        ASSERT_TRUE(!manifestFileName.empty());

        // Wait up to 50s for it to come back up.
        int count = 0;
        int secondsToWait = 50;
        while (count++ < secondsToWait) {
            SData cmd("Status");
            try {
                string response = uploadTester->executeWaitVerifyContent(cmd);
                STable json = SParseJSONObject(response);
                if (json["state"] == "MASTERING") {
                    break;
                }
            } catch (const SException& e) {
                if (count == secondsToWait) {
                    STHROW("Never finished uploading.");
                }
            }

            // Give it another second...
            sleep(1);
        }
    }

    void download()
    {
        // Delete and recreate our tester with the bootstrap flag.
        delete uploadTester;
        downloadTester = new BedrockTester({{"-db", "/tmp/ebmtest_download.db"},
                                            {"-backupKeyFile", string(cwd) + "/tests/backupManager/data/key.key"},
                                            {"-plugins", "BackupManager"},
                                            {"-bootstrap", manifestFileName}},
                                           {});

        // Wait up to 50s for it to come back up.
        int count = 0;
        int secondsToWait = 50;
        while (count++ < secondsToWait) {
            SData cmd("Status");
            try {
                string response = downloadTester->executeWaitVerifyContent(cmd);
                STable json = SParseJSONObject(response);
                if (json["state"] == "MASTERING") {
                    break;
                }
            } catch (const SException& e) {
                if (count == secondsToWait) {
                    STHROW("Never finished downloading.");
                }
            }

            // Give it another second...
            sleep(1);
        }
    }

} __BackupTest;
