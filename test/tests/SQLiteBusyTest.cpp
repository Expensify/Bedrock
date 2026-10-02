#include <cstdarg>
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/qrf.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/tpunit++.hpp>

namespace {
struct BusyTestFile
{
    char filename[24] = "/tmp/br_busy_XXXXXX";

    BusyTestFile()
    {
        close(mkstemp(filename));
    }

    ~BusyTestFile()
    {
        unlink(filename);
        unlink((string(filename) + "-wal").c_str());
        unlink((string(filename) + "-wal2").c_str());
        unlink((string(filename) + "-shm").c_str());
    }
};

struct BusyTestConnection
{
    sqlite3* db = nullptr;

    explicit BusyTestConnection(const char* filename)
    {
        SASSERT(sqlite3_open(filename, &db) == SQLITE_OK);
    }

    ~BusyTestConnection()
    {
        sqlite3_close(db);
    }
};

struct BusyLogCapture;
thread_local BusyLogCapture* busyLogCapture = nullptr;

struct BusyLogCapture
{
    vector<string> messages;
    decltype(SSyslogFunc.load()) previous = SSyslogFunc.exchange(capture);
    int previousMask = _g_SLogMask.fetch_or(1 << LOG_INFO);

    BusyLogCapture()
    {
        busyLogCapture = this;
    }

    ~BusyLogCapture()
    {
        busyLogCapture = nullptr;
        SSyslogFunc = previous;
        _g_SLogMask = previousMask;
    }

    static void capture(int priority, const char* format, ...)
    {
        char message[8192];
        va_list args;
        va_start(args, format);
        vsnprintf(message, sizeof(message), format, args);
        va_end(args);
        if (busyLogCapture) {
            busyLogCapture->messages.emplace_back(message);
        } else {
            syslog(priority, "%s", message);
        }
    }

    size_t count(const string& text) const
    {
        return count_if(messages.begin(), messages.end(), [&](const string& message) {
            return message.find(text) != string::npos;
        });
    }
};

struct ReleaseOnBusy
{
    sqlite3* db;
    sqlite3* blocker;
    int calls = 0;
    int releaseAfter = 1;
    int releaseResult = SQLITE_ERROR;

    static int handle(void* context, int count)
    {
        auto& state = *static_cast<ReleaseOnBusy*>(context);
        state.calls++;
        int retry = SQueryBusyHandler(state.db, count);
        if (state.calls == state.releaseAfter) {
            // A nested SQuery on another connection must preserve the outer query's wait accounting.
            state.releaseResult = SQuery(state.blocker, "ROLLBACK");
        }
        return retry;
    }
};
}

struct SQLiteBusyTest : tpunit::TestFixture
{
    SQLiteBusyTest()
        : tpunit::TestFixture("SQLiteBusy",
                              TEST(SQLiteBusyTest::logsRecoveredCommitContention),
                              TEST(SQLiteBusyTest::keepsTheOuterRetryOnExhaustion),
                              TEST(SQLiteBusyTest::doesNotWaitOnSnapshotConflicts),
                              TEST(SQLiteBusyTest::logsFormattedQueryContention))
    {
    }

    void logsRecoveredCommitContention()
    {
        BusyTestFile file;
        SQLite db(file.filename, 1000, 1000, 1);
        ASSERT_EQUAL(SQuery(db.getDBHandle(), "CREATE TABLE test(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO test VALUES(1, 0);"), SQLITE_OK);
        BusyTestConnection blocker(file.filename);
        ASSERT_EQUAL(SQuery(blocker.db, "BEGIN IMMEDIATE"), SQLITE_OK);
        ReleaseOnBusy state{db.getDBHandle(), blocker.db};
        state.releaseAfter = 3;
        ASSERT_EQUAL(sqlite3_busy_handler(db.getDBHandle(), ReleaseOnBusy::handle, &state), SQLITE_OK);

        BusyLogCapture logs;
        ASSERT_TRUE(db.writeLocalUnreplicated("UPDATE test SET value = value + 1 WHERE id = 1;"));
        ASSERT_EQUAL(state.releaseResult, SQLITE_OK);
        ASSERT_EQUAL(state.calls, 3);
        ASSERT_EQUAL(logs.count("SQLite busy contention cleared"), 1);
        ASSERT_EQUAL(logs.count("sqlite3 returned SQLITE_BUSY"), 0);

        vector<string> matches;
        for (const auto& message : logs.messages) {
            if (SREMatch("SQLite busy contention cleared after ([0-9]+)us of waiting across ([0-9]+) retries\\.", message, true, true, &matches)) {
                ASSERT_TRUE(SToUInt64(matches[1]) >= 8000);
                ASSERT_EQUAL(matches[2], "3");
            }
        }
        ASSERT_EQUAL(db.read("SELECT value FROM test WHERE id = 1;"), "1");
        ASSERT_EQUAL(logs.count("SQLite busy contention cleared"), 1);
    }

    void keepsTheOuterRetryOnExhaustion()
    {
        BusyTestFile file;
        SQLite db(file.filename, 1000, 1000, 1);
        ASSERT_EQUAL(SQuery(db.getDBHandle(), "CREATE TABLE test(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO test VALUES(1, 0);"), SQLITE_OK);
        BusyTestConnection blocker(file.filename);
        ASSERT_EQUAL(SQuery(blocker.db, "BEGIN IMMEDIATE"), SQLITE_OK);
        ASSERT_EQUAL(SQuery(db.getDBHandle(), "BEGIN CONCURRENT; UPDATE test SET value = 1 WHERE id = 1;"), SQLITE_OK);

        BusyLogCapture logs;
        ASSERT_EQUAL(SQuery(db.getDBHandle(), "COMMIT"), SQLITE_BUSY);
        ASSERT_EQUAL(logs.count("sqlite3 returned SQLITE_BUSY"), 3);
        ASSERT_EQUAL(logs.count("Sleeping 1 second"), 2);
        ASSERT_EQUAL(logs.count("SQLite busy contention cleared"), 0);
        ASSERT_EQUAL(SQuery(db.getDBHandle(), "ROLLBACK"), SQLITE_OK);
        ASSERT_EQUAL(SQuery(blocker.db, "ROLLBACK"), SQLITE_OK);
        ASSERT_TRUE(db.writeLocalUnreplicated("UPDATE test SET value = 2 WHERE id = 1;"));
        ASSERT_EQUAL(db.read("SELECT value FROM test WHERE id = 1;"), "2");
        ASSERT_EQUAL(logs.count("SQLite busy contention cleared"), 0);
    }

    void doesNotWaitOnSnapshotConflicts()
    {
        BusyTestFile file;
        SQLite db(file.filename, 1000, 1000, 1);
        ASSERT_EQUAL(SQuery(db.getDBHandle(), "CREATE TABLE test(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO test VALUES(1, 0);"), SQLITE_OK);
        BusyTestConnection other(file.filename);
        ASSERT_EQUAL(SQuery(db.getDBHandle(), "BEGIN CONCURRENT; UPDATE test SET value = 1 WHERE id = 1;"), SQLITE_OK);
        ASSERT_EQUAL(SQuery(other.db, "BEGIN CONCURRENT; UPDATE test SET value = 2 WHERE id = 1; COMMIT;"), SQLITE_OK);
        ReleaseOnBusy state{db.getDBHandle(), other.db};
        ASSERT_EQUAL(sqlite3_busy_handler(db.getDBHandle(), ReleaseOnBusy::handle, &state), SQLITE_OK);

        BusyLogCapture logs;
        ASSERT_EQUAL(SQuery(db.getDBHandle(), "COMMIT"), SQLITE_BUSY_SNAPSHOT);
        ASSERT_EQUAL(state.calls, 0);
        ASSERT_EQUAL(logs.count("SQLite busy contention cleared"), 0);
        ASSERT_EQUAL(logs.count("sqlite3 returned SQLITE_BUSY"), 0);
        ASSERT_EQUAL(SQuery(db.getDBHandle(), "ROLLBACK"), SQLITE_OK);
    }

    void logsFormattedQueryContention()
    {
        BusyTestFile file;
        BusyTestConnection db(file.filename);
        ASSERT_EQUAL(SQuery(db.db, "CREATE TABLE test(value INTEGER); INSERT INTO test VALUES(42);"), SQLITE_OK);
        BusyTestConnection blocker(file.filename);
        ASSERT_EQUAL(SQuery(blocker.db, "BEGIN EXCLUSIVE"), SQLITE_OK);
        ReleaseOnBusy state{db.db, blocker.db};
        ASSERT_EQUAL(sqlite3_busy_handler(db.db, ReleaseOnBusy::handle, &state), SQLITE_OK);
        char* output = nullptr;
        sqlite3_qrf_spec spec{};
        spec.iVersion = 1;
        spec.eStyle = QRF_STYLE_Csv;
        spec.bTitles = QRF_No;
        spec.pzOutput = &output;

        BusyLogCapture logs;
        const int result = SQuery(db.db, "SELECT value FROM test;", &spec);
        const string text = output ? output : "";
        sqlite3_free(output);
        ASSERT_EQUAL(result, SQLITE_OK);
        ASSERT_EQUAL(text, "42\r\n");
        ASSERT_EQUAL(state.releaseResult, SQLITE_OK);
        ASSERT_EQUAL(logs.count("SQLite busy contention cleared"), 1);
        ASSERT_EQUAL(SQuery(db.db, "SELECT 1;"), SQLITE_OK);
        ASSERT_EQUAL(logs.count("SQLite busy contention cleared"), 1);
    }
} __SQLiteBusyTest;
