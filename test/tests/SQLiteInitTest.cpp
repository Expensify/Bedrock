#include <libstuff/libstuff.h>
#include <libstuff/SQResult.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/BedrockTester.h>

#include <sys/wait.h>
#include <unistd.h>

struct SQLiteInitTest : tpunit::TestFixture
{
    SQLiteInitTest()
        : tpunit::TestFixture("SQLiteInit",
                              TEST(SQLiteInitTest::testBusyTimeoutSetBeforeJournalInit))
    {
    }

    // Verifies that busy_timeout is configured before initializeJournal runs in the SQLite constructor.
    //
    // A child process holds an exclusive lock on the database for 5 seconds. The parent
    // process constructs a SQLite object during that time. The test proves that busy_timeout causes sqlite to
    // wait for the lock and succeed.
    void testBusyTimeoutSetBeforeJournalInit()
    {
        // Create a temp file for our test database.
        char filenameTemplate[] = "/tmp/br_init_XXXXXX";
        int fileDescriptor = mkstemp(filenameTemplate);
        close(fileDescriptor);
        string filename(filenameTemplate);

        // Set up the database with a journal table using raw sqlite3.
        // This ensures initializeJournal has a table to discover.
        {
            sqlite3* setupDB = nullptr;
            ASSERT_EQUAL(sqlite3_open_v2(filename.c_str(), &setupDB,
                                         SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL),
                         SQLITE_OK);
            // Use the exact CREATE TABLE statement that initializeJournal expects.
            // SQVerifyTable compares the stored SQL in sqlite_master with the expected string.
            ASSERT_EQUAL(sqlite3_exec(setupDB,
                                      "CREATE TABLE journal ( id INTEGER PRIMARY KEY, query TEXT, hash TEXT );",
                                      NULL, NULL, NULL),
                         SQLITE_OK);
            sqlite3_close(setupDB);
        }

        // Create a pipe so the child can signal when the lock is acquired.
        int pipeFileDescriptors[2];
        ASSERT_EQUAL(pipe(pipeFileDescriptors), 0);

        // Fork a child process to hold an exclusive lock on the database.
        // POSIX advisory locks only block between different processes, not between threads.
        pid_t lockProcessID = fork();
        if (lockProcessID == 0) {
            // The child process won't read from the pipe, so close that end
            close(pipeFileDescriptors[0]);

            // Hold the exclusive lock on the database.
            // Write to the database to actually acquire the lock since SQLite is lazy.
            sqlite3* lockDB = nullptr;
            sqlite3_open_v2(filename.c_str(), &lockDB,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, NULL);
            sqlite3_exec(lockDB, "BEGIN EXCLUSIVE;", NULL, NULL, NULL);
            sqlite3_exec(lockDB, "INSERT INTO journal VALUES(1, 'test', 'hash');", NULL, NULL, NULL);

            // Signal parent that lock is acquired.
            write(pipeFileDescriptors[1], "L", 1);
            close(pipeFileDescriptors[1]);

            // Hold the lock for 5 seconds. This is longer than SQuery's 3 retry attempts (~3 seconds
            // with 1-second sleeps). Without busy_timeout, the parent's SQuery would give up and SASSERT.
            // With busy_timeout (120s), sqlite waits internally and succeeds once we release the lock.
            sleep(5);

            sqlite3_exec(lockDB, "COMMIT;", NULL, NULL, NULL);
            sqlite3_close(lockDB);

            // Exit the child process and skip cleanup to prevent problems running the parent's cleanup code.
            _exit(0);
        }

        ASSERT_GREATER_THAN(lockProcessID, 0);

        // Parent process: Close the write end of the pipe because the parent only reads.
        close(pipeFileDescriptors[1]);

        // Wait for the child process to signal that the lock is acquired.
        char signalByte;
        read(pipeFileDescriptors[0], &signalByte, 1);
        close(pipeFileDescriptors[0]);

        // Construct a SQLite object while the database is locked by the child process.
        SQLite db(filename, 1000, 5000, -1, 0, false, "PASSIVE");

        // Wait for lock-holding child to exit.
        int status = 0;
        waitpid(lockProcessID, &status, 0);

        // Verify that the SQLite object was constructed successfully and the parent waited for the child process to exit.
        ASSERT_EQUAL(status, 0);

        // Clean up temp files.
        unlink(filename.c_str());
        unlink((filename + "-wal").c_str());
        unlink((filename + "-shm").c_str());
        unlink((filename + "-journal").c_str());
    }
} __SQLiteInitTest;
