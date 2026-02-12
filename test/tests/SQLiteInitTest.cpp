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
    // Without the fix, busy_timeout was only set in commonConstructorInitialization() (constructor body),
    // but initializeJournal() runs earlier in the member initializer list. If the database was locked,
    // SQuery would fail with SQLITE_BUSY after 3 retries and trigger SASSERT -> abort().
    //
    // Test approach: We use two child processes to isolate the test from crashes:
    //   - Child A holds an exclusive lock on the database for 5 seconds.
    //   - Child B attempts to construct a SQLite object while the lock is held.
    // The parent checks child B's exit status. If child B was killed by SIGABRT, the fix is missing.
    //
    // We need separate processes (not threads) because POSIX advisory locks only block across processes.
    // Child B is forked after child A acquires the lock, so child B does not inherit any lock-related
    // file descriptors — this avoids POSIX lock inheritance issues that would prevent the lock from
    // blocking child B's queries.
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

        // Create a pipe so child A can signal when the lock is acquired.
        int lockPipeFDs[2];
        ASSERT_EQUAL(pipe(lockPipeFDs), 0);

        // Fork child A: holds an exclusive lock on the database.
        pid_t lockHolderProcessID = fork();
        if (lockHolderProcessID == 0) {
            close(lockPipeFDs[0]);

            sqlite3* lockDB = nullptr;
            sqlite3_open_v2(filename.c_str(), &lockDB,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, NULL);
            sqlite3_exec(lockDB, "BEGIN EXCLUSIVE;", NULL, NULL, NULL);
            sqlite3_exec(lockDB, "INSERT INTO journal VALUES(1, 'test', 'hash');", NULL, NULL, NULL);

            // Signal parent that lock is acquired.
            write(lockPipeFDs[1], "L", 1);
            close(lockPipeFDs[1]);

            // Hold the lock for 5 seconds. This is longer than SQuery's 3 retry attempts (~3 seconds
            // with 1-second sleeps). Without busy_timeout, child B's SQuery gives up and SASSERT aborts.
            // With busy_timeout (120s), sqlite waits internally and succeeds once we release the lock.
            sleep(5);

            sqlite3_exec(lockDB, "COMMIT;", NULL, NULL, NULL);
            sqlite3_close(lockDB);
            _exit(0);
        }

        ASSERT_GREATER_THAN(lockHolderProcessID, 0);

        // Parent: wait for child A to signal that the lock is acquired.
        close(lockPipeFDs[1]);
        char signalByte;
        read(lockPipeFDs[0], &signalByte, 1);
        close(lockPipeFDs[0]);

        // Fork child B: attempts to construct a SQLite object while the database is locked.
        // We run this in a child process so that if SASSERT fires (abort), only child B crashes,
        // and the parent can detect and report the failure properly.
        // Child B is forked AFTER child A opened the lock DB, so child B does not inherit any of
        // child A's lock-related file descriptors.
        pid_t constructorProcessID = fork();
        if (constructorProcessID == 0) {
            // The constructor calls initializeJournal -> SQVerifyTable -> SQuery on sqlite_master.
            // With the fix (busy_timeout set in initializeDB), sqlite waits for the lock and succeeds.
            // Without the fix, SQuery fails with SQLITE_BUSY after 3 retries -> SASSERT -> abort.
            SQLite db(filename, 1000, 5000, -1, 0, false, "PASSIVE");
            _exit(0);
        }

        ASSERT_GREATER_THAN(constructorProcessID, 0);

        // Wait for child B (the SQLite constructor attempt) to finish.
        int constructorStatus = 0;
        waitpid(constructorProcessID, &constructorStatus, 0);

        // Wait for child A (the lock holder) to finish.
        int lockHolderStatus = 0;
        waitpid(lockHolderProcessID, &lockHolderStatus, 0);

        // Clean up temp files.
        unlink(filename.c_str());
        unlink((filename + "-wal").c_str());
        unlink((filename + "-shm").c_str());
        unlink((filename + "-journal").c_str());

        // Verify child B exited cleanly (not killed by SIGABRT from SASSERT).
        if (WIFSIGNALED(constructorStatus)) {
            TESTINFO("SQLite constructor was killed by signal " + to_string(WTERMSIG(constructorStatus)) +
                     " — busy_timeout was not set before initializeJournal");
            ABORT();
        }
        ASSERT_TRUE(WIFEXITED(constructorStatus));
        ASSERT_EQUAL(WEXITSTATUS(constructorStatus), 0);
    }
} __SQLiteInitTest;
