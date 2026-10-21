/*
 * Standalone HC-Tree follower visibility test. Links only to SQLite, not Bedrock.
 * All connections are used serially. Every statement is consumed and finalized.
 * Exit 0: all checks passed; 1: visibility/recovery mismatch; 2: setup/API error.
 * Database files are retained in the printed artifact directory.
 */
#include <sqlite3.h>
#include <sqlite3hct.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct Connection {
    sqlite3* db;
    const char* name;
} Connection;

static int failures;
static int traceEnabled;
static int bedrockProbes;
static const char* beginSQL = "BEGIN CONCURRENT";
static const char* insertSQL = "INSERT INTO t VALUES (1, 'replayed');";

static void checkAPI(Connection* connection, int rc, const char* operation)
{
    if (rc != SQLITE_OK) {
        fprintf(stderr, "API ERROR [%s] %s: rc=%d extended=%d message=%s\n",
                connection->name, operation, rc,
                connection->db ? sqlite3_extended_errcode(connection->db) : rc,
                connection->db ? sqlite3_errmsg(connection->db) : "no database handle");
        exit(2);
    }
}

static void expectInteger(const char* context, sqlite3_int64 actual, sqlite3_int64 expected)
{
    if (actual != expected) {
        fprintf(stderr, "FAIL %s: actual=%lld expected=%lld\n", context,
                (long long) actual, (long long) expected);
        ++failures;
    }
}

static int traceSQL(unsigned event, void* context, void* statement, void* sql)
{
    (void) event;
    (void) statement;
    fprintf(stderr, "SQL [%s] %s\n", (const char*) context, (const char*) sql);
    return 0;
}

static void execute(Connection* connection, const char* sql)
{
    checkAPI(connection, sqlite3_exec(connection->db, sql, NULL, NULL, NULL), sql);
}

static sqlite3_int64 scalar(Connection* connection, const char* sql)
{
    sqlite3_stmt* statement = NULL;
    checkAPI(connection, sqlite3_prepare_v2(connection->db, sql, -1, &statement, NULL), sql);
    int rc = sqlite3_step(statement);
    if (rc != SQLITE_ROW) {
        checkAPI(connection, rc == SQLITE_DONE ? SQLITE_ERROR : rc, sql);
    }
    sqlite3_int64 result = sqlite3_column_int64(statement, 0);
    rc = sqlite3_step(statement);
    if (rc != SQLITE_DONE) {
        checkAPI(connection, rc == SQLITE_ROW ? SQLITE_ERROR : rc, sql);
    }
    checkAPI(connection, sqlite3_finalize(statement), "finalize scalar");
    return result;
}

static void checkIdle(Connection* connection)
{
    expectInteger("autocommit", sqlite3_get_autocommit(connection->db), 1);
    expectInteger("transaction state", sqlite3_txn_state(connection->db, "main"), SQLITE_TXN_NONE);
    expectInteger("unfinalized statements", sqlite3_next_stmt(connection->db, NULL) != NULL, 0);
}

static Connection openConnection(const char* path, const char* name, int create)
{
    Connection connection = {NULL, name};
    char* uri = create ? sqlite3_mprintf("file:%s?hctree=1", path) : sqlite3_mprintf("%s", path);
    if (!uri) {
        fprintf(stderr, "Out of memory constructing database URI\n");
        exit(2);
    }
    checkAPI(&connection, sqlite3_open_v2(uri, &connection.db,
             SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI | SQLITE_OPEN_NOMUTEX, NULL), "open");
    sqlite3_free(uri);
    checkAPI(&connection, sqlite3_extended_result_codes(connection.db, 1), "extended result codes");
    checkAPI(&connection, sqlite3_busy_timeout(connection.db, 5000), "busy timeout");
    if (traceEnabled) {
        checkAPI(&connection, sqlite3_trace_v2(connection.db, SQLITE_TRACE_STMT, traceSQL, (void*) name), "SQL trace");
    }
    if (bedrockProbes) {
        execute(&connection, "PRAGMA legacy_file_format = OFF;");
        execute(&connection, "PRAGMA cache_size = -1000000;");
        (void) scalar(&connection, "PRAGMA schema_version;");
    }
    return connection;
}

static void closeConnection(Connection* connection)
{
    checkIdle(connection);
    checkAPI(connection, sqlite3_close(connection->db), "close");
    connection->db = NULL;
}

static void follow(Connection* connection)
{
    checkIdle(connection);
    checkAPI(connection, sqlite3_hct_journal_setmode(connection->db, SQLITE_HCT_FOLLOWER), "set follower mode");
}

static sqlite3_int64 snapshot(Connection* connection)
{
    sqlite3_int64 value = -1;
    checkAPI(connection, sqlite3_hct_journal_snapshot(connection->db, &value), "available snapshot");
    return value;
}

static void commitTransaction(Connection* connection, sqlite3_int64 cid, sqlite3_int64 dependency, const char* sql)
{
    checkIdle(connection);
    execute(connection, beginSQL);
    if (sql[0]) {
        execute(connection, sql);
    }
    if (bedrockProbes) {
        (void) scalar(connection, "PRAGMA page_count;");
    }
    /* Keep the opaque payload non-NULL even for a blank journal entry. */
    const char* payload = sql[0] ? sql : ":";
    int rc = sqlite3_hct_journal_follower_commit(connection->db,
             (const unsigned char*) payload, (int) strlen(payload), cid, dependency);
    checkAPI(connection, rc, "follower commit");
    if (bedrockProbes) {
        (void) scalar(connection, "PRAGMA page_count;");
    }
    checkIdle(connection);
}

static sqlite3_int64 journalEntries(Connection* connection, sqlite3_int64 cid)
{
    char* sql = sqlite3_mprintf("SELECT COUNT(*) FROM hct_journal WHERE cid = %lld", (long long) cid);
    sqlite3_int64 count = scalar(connection, sql);
    sqlite3_free(sql);
    return count;
}

static void dumpJournal(Connection* connection, sqlite3_int64 prefix)
{
    char* sql = sqlite3_mprintf("SELECT cid, snapshot, hex(query) FROM hct_journal WHERE cid >= %lld ORDER BY cid",
                               (long long) prefix);
    sqlite3_stmt* statement = NULL;
    checkAPI(connection, sqlite3_prepare_v2(connection->db, sql, -1, &statement, NULL), sql);
    int rc;
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        printf("JOURNAL [%s] cid=%lld dependency=%lld payload=%s\n", connection->name,
               (long long) sqlite3_column_int64(statement, 0),
               (long long) sqlite3_column_int64(statement, 1), sqlite3_column_text(statement, 2));
    }
    checkAPI(connection, rc == SQLITE_DONE ? SQLITE_OK : rc, sql);
    checkAPI(connection, sqlite3_finalize(statement), "finalize journal scan");
    sqlite3_free(sql);
    checkIdle(connection);
}

static void observe(Connection* connection, const char* stage, sqlite3_int64 expectedSnapshot, sqlite3_int64 expectedRows)
{
    checkIdle(connection);
    sqlite3_int64 available = snapshot(connection);
    sqlite3_int64 count = scalar(connection, "SELECT COUNT(*) FROM t;");
    sqlite3_int64 lookup = scalar(connection, "SELECT COUNT(*) FROM t WHERE id = 1 AND value = 'replayed';");
    sqlite3_stmt* statement = NULL;
    const char* sql = "SELECT id, value FROM t NOT INDEXED ORDER BY id;";
    checkAPI(connection, sqlite3_prepare_v2(connection->db, sql, -1, &statement, NULL), sql);
    sqlite3_int64 scanned = 0;
    int rc;
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        printf("ROW [%s/%s] id=%lld value=%s\n", stage, connection->name,
               (long long) sqlite3_column_int64(statement, 0), sqlite3_column_text(statement, 1));
        ++scanned;
    }
    checkAPI(connection, rc == SQLITE_DONE ? SQLITE_OK : rc, sql);
    checkAPI(connection, sqlite3_finalize(statement), "finalize application scan");
    printf("READ [%s/%s] mode=%d snapshot=%lld autocommit=%d txn=%d count=%lld lookup=%lld scan=%lld\n",
           stage, connection->name, sqlite3_hct_journal_mode(connection->db), (long long) available,
           sqlite3_get_autocommit(connection->db), sqlite3_txn_state(connection->db, "main"),
           (long long) count, (long long) lookup, (long long) scanned);
    char context[160];
    snprintf(context, sizeof(context), "%s/%s snapshot", stage, connection->name);
    expectInteger(context, available, expectedSnapshot);
    snprintf(context, sizeof(context), "%s/%s count", stage, connection->name);
    expectInteger(context, count, expectedRows);
    snprintf(context, sizeof(context), "%s/%s lookup", stage, connection->name);
    expectInteger(context, lookup, expectedRows);
    snprintf(context, sizeof(context), "%s/%s scan", stage, connection->name);
    expectInteger(context, scanned, expectedRows);
    checkIdle(connection);
}

static void runCase(const char* directory, const char* caseName, sqlite3_int64 prefix)
{
    int before = failures;
    int hasTail = strcmp(caseName, "no-tail-reopen") != 0;
    int reopen = strcmp(caseName, "gap-no-reopen") != 0;
    char* path = sqlite3_mprintf("%s/%s.db", directory, caseName);
    printf("\nCASE %s prefix=%lld begin=%s probes=%d database=%s\n", caseName,
           (long long) prefix, beginSQL, bedrockProbes, path);
    Connection base = openConnection(path, "base", 1);
    checkAPI(&base, sqlite3_hct_journal_init(base.db), "initialize native journal");
    follow(&base);
    commitTransaction(&base, 2, 1, "CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT);");
    for (sqlite3_int64 cid = 3; cid <= prefix; ++cid) {
        commitTransaction(&base, cid, cid - 1, "");
    }
    Connection writer = openConnection(path, "writer", 0);
    if (hasTail) {
        commitTransaction(&writer, prefix + 2, prefix, insertSQL);
        expectInteger("out-of-order journal entry", journalEntries(&base, prefix + 2), 1);
        observe(&base, "gapped", prefix, 0);
    }
    if (reopen) {
        closeConnection(&writer);
        closeConnection(&base);
        base = openConnection(path, "recovered-base", 0);
        /* NORMAL -> FOLLOWER happens with no other connections or active transactions. */
        follow(&base);
        expectInteger("recovered journal tail", journalEntries(&base, prefix + 2), 0);
        observe(&base, "recovered", prefix, 0);
        writer = openConnection(path, "replay-writer", 0);
    }
    Connection earlyReader = openConnection(path, "reader-before-replay", 0);
    observe(&earlyReader, "before-replay", prefix, 0);
    commitTransaction(&writer, prefix + 1, prefix, "");
    if (reopen) {
        commitTransaction(&writer, prefix + 2, prefix, insertSQL);
    }
    observe(&base, "after-replay", prefix + 2, 1);
    observe(&writer, "after-replay", prefix + 2, 1);
    observe(&earlyReader, "after-replay", prefix + 2, 1);
    Connection freshReader = openConnection(path, "reader-after-replay", 0);
    observe(&freshReader, "after-replay", prefix + 2, 1);
    dumpJournal(&base, prefix);
    closeConnection(&freshReader);
    closeConnection(&earlyReader);
    closeConnection(&writer);
    closeConnection(&base);
    printf("RESULT %s: %s (%d mismatches)\n", caseName, failures == before ? "PASS" : "FAIL", failures - before);
    sqlite3_free(path);
}

int main(int argc, char** argv)
{
    sqlite3_int64 prefix = 157;
    const char* selected = "all";
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--trace")) {
            traceEnabled = 1;
        } else if (!strcmp(argv[i], "--bedrock-probes")) {
            bedrockProbes = 1;
        } else if (!strcmp(argv[i], "--plain-begin")) {
            beginSQL = "BEGIN";
        } else if (!strcmp(argv[i], "--case") && i + 1 < argc) {
            selected = argv[++i];
        } else if (!strcmp(argv[i], "--prefix") && i + 1 < argc) {
            char* end = NULL;
            errno = 0;
            prefix = strtoll(argv[++i], &end, 10);
            if (errno || !end || *end || prefix < 2 || prefix > 100000) {
                fprintf(stderr, "--prefix must be between 2 and 100000\n");
                return 2;
            }
        } else {
            fprintf(stderr, "Usage: %s [--case all|gap-reopen|no-tail-reopen|gap-no-reopen] "
                    "[--prefix N] [--plain-begin] [--bedrock-probes] [--trace]\n", argv[0]);
            return 2;
        }
    }
    const char* cases[] = {"gap-reopen", "no-tail-reopen", "gap-no-reopen"};
    int valid = !strcmp(selected, "all");
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        valid |= !strcmp(selected, cases[i]);
    }
    if (!valid) {
        fprintf(stderr, "Unknown case: %s\n", selected);
        return 2;
    }
    setvbuf(stdout, NULL, _IOLBF, 0);
    const char* temporary = getenv("TMPDIR");
    char* directory = sqlite3_mprintf("%s/hctree-visibility-XXXXXX", temporary ? temporary : "/tmp");
    if (!directory || !mkdtemp(directory)) {
        perror("create artifact directory");
        return 2;
    }
    printf("SQLite version: %s\nSQLite source ID: %s\nCompiler: %s\nArtifacts: %s\n",
           sqlite3_libversion(), sqlite3_sourceid(), __VERSION__, directory);
    for (int i = 0; sqlite3_compileoption_get(i); ++i) {
        printf("COMPILE_OPTION %s\n", sqlite3_compileoption_get(i));
    }
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        if (!strcmp(selected, "all") || !strcmp(selected, cases[i])) {
            runCase(directory, cases[i], prefix);
        }
    }
    printf("\nSUMMARY %s: %d mismatches; artifacts retained at %s\n", failures ? "FAIL" : "PASS", failures, directory);
    sqlite3_free(directory);
    return failures ? 1 : 0;
}
