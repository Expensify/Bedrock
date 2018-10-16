#include <test/lib/BedrockTester.h>

struct BackupTest : tpunit::TestFixture
{
    BackupTest() : tpunit::TestFixture(BEFORE_CLASS(BackupTest::setupClass),
                                            TEST(BackupTest::upload),
                                            TEST(BackupTest::download),
                                            AFTER_CLASS(BackupTest::tearDownClass))
    {
        NAME(UploadBackup);
    }

    BedrockTester* uploadTester;
    BedrockTester* downloadTester;
    string manifestFileName;

    void setupClass()
    {
        // Load our SQL file and run the queries from inside of it
        string dbSql = SFileLoad("data/db.sql");
        const list<string>& queries = SParseList(dbSql, ';');
        uploadTester = new BedrockTester({{"-plugins", "../expensifyBackupManager.so"},
                                          {"-db", "/tmp/ebmtest_upload.db"},
                                          {"-backupKeyFile", "data/key.key"}}, queries);
    }

    void tearDownClass()
    {
        delete downloadTester;
    }

    void upload()
    {
        // Do the backup
        SData command("BeginExpensifyBackup");
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
        downloadTester = new BedrockTester({{"-plugins", "../expensifyBackupManager.so"},
                                            {"-db", "/tmp/ebmtest_download.db"},
                                            {"-backupKeyFile", "data/key.key"},
                                            {"-bootstrap", manifestFileName}}, {});

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
