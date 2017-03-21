/// /svn/src/sqlitecluster/SQLite.cpp
/// =================================
#include <libstuff/libstuff.h>
#include "SQLite.h"

// --------------------------------------------------------------------------
#define DBINFO(_MSG_) SINFO("{" << _filename << "} " << _MSG_)
#define DBWARN(_MSG_) SWARN("{" << _filename << "} " << _MSG_)

#define DB_WRITE_OPEN_FLAGS SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX
#define DB_READ_OPEN_FLAGS SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX

bool SQLite::sqliteInitialized = false;

extern "C" void sqlite3_spellfix_init();

// --------------------------------------------------------------------------
void SQLite::sqliteLogCallback(void* pArg, int iErrCode, const char* zMsg) {
    SSYSLOG(LOG_INFO, SWHEREAMI << "[info] "
                                << "{SQLITE} Code: " << iErrCode << ", Message: " << zMsg);
}

// --------------------------------------------------------------------------
SQLite::SQLite(const string& filename, int cacheSize, int autoCheckpoint, bool readOnly, int maxJournalSize) {
    // Initialize
    SINFO("Opening " << (readOnly ? "Read Only" : "Writable") << " sqlite connection");
    SASSERT(!filename.empty());
    SASSERT(cacheSize > 0);
    SASSERT(autoCheckpoint >= 0);
    SASSERT(maxJournalSize > 0);
    _filename = filename;
    _insideTransaction = false;
    _lastWriteChanged = false;
    _readOnly = readOnly;
    _maxJournalSize = maxJournalSize;
    _beginElapsed = 0;
    _readElapsed = 0;
    _writeElapsed = 0;
    _prepareElapsed = 0;
    _commitElapsed = 0;
    _rollbackElapsed = 0;

    // Initialize logging on first use.
    if (!sqliteInitialized) {
        sqlite3_config(SQLITE_CONFIG_LOG, sqliteLogCallback, 0);
        sqliteInitialized = true;

        // Load the spellfix extension for all future DB connections.
        // NOTE: sqlite3_auto_extension implicitly calls sqlite3_initialize, so it's no longer called explicitly
        //       See: https://www.sqlite.org/loadext.html
        sqlite3_auto_extension(sqlite3_spellfix_init);
    }

    // Initialize sqlite multithreaded magic.
    // **NOTE: each thread will need to open its own connection.
    SASSERT(sqlite3_threadsafe()); // Make sure sqlite is compiled in threadsafe mode.

    // Disabled by default, but lets really beat it in.  This way checkpointing
    // does not need to wait on locks created in this thread.
    SASSERT(sqlite3_enable_shared_cache(0) == SQLITE_OK);

    // Open or create the database
    if (SFileExists(_filename)) {
        DBINFO("Opening database '" << _filename << "'");
    } else {
        DBINFO("Creating new database '" << _filename << "'");
    }
    if (!readOnly) {
        // Open a read/write database
        SASSERT(!sqlite3_open_v2(filename.c_str(), &_db, DB_WRITE_OPEN_FLAGS, NULL));
        _setPragmas(_db, autoCheckpoint);
        if (SQVerifyTable(_db, "journal", "CREATE TABLE journal ( id INTEGER PRIMARY KEY, query TEXT, hash TEXT )")) {
            SHMMM("Created journal table.");
        }
    } else {
        // Open a read-only database
        SASSERT(!sqlite3_open_v2(filename.c_str(), &_db, DB_READ_OPEN_FLAGS, NULL));
    }

    // Set a one-second timeout for automatic retries in case of SQLITE_BUSY.
    sqlite3_busy_timeout(_db, 1000);

    // Update the cache
    SINFO("Setting cache_size to " << cacheSize << "KB");
    SQuery(_db, "increasing cache size",
           "PRAGMA cache_size = -" + SQ(cacheSize) + ";"); // -size means KB; +size means pages

    // Look up the current state of the database.
    SQResult result;
    SASSERT(SQuery(_db, "getting commit count", "SELECT maxID, (maxID-minID)+1 FROM (SELECT (SELECT MAX(id) FROM "
                                                "journal) as maxID, (SELECT MIN(id) FROM journal) as minID)",
                   result));
    _commitCount = SToUInt64(result[0][0]);
    _journalSize = SToUInt64(result[0][1]);
    const string& query = "SELECT hash FROM journal WHERE id=" + SToStr(_commitCount);
    SASSERT(SQuery(_db, "getting DB hash", query.c_str(), result));
    if (!result.empty()) {
        _committedHash = result[0][0];
    }
    DBINFO("Database opened, commitCount=" << _commitCount << ", journalSize=" << _journalSize << " of "
                                           << _maxJournalSize << " max (" << _committedHash << ")");
}

// --------------------------------------------------------------------------
SQLite::~SQLite() {
    // Close the database -- first rollback any incomplete transaction
    if (!_uncommittedQuery.empty())
        rollback();

    // Now verify its hash and commit count matches our expectations
    DBINFO("Closing database '" << _filename << "', commitCount=" << _commitCount << "(" << _committedHash << ")");
    SASSERTWARN(_uncommittedQuery.empty());
    if (!_readOnly) {
        // If we're the read/write thread, also confirm that the database
        // is in the correct state.  First, confirm that our commit count
        // matches the final commit count in the databae.
        SQResult result;
        SASSERT(SQuery(_db, "verifying commit count", "SELECT MAX(id) FROM journal", result));
        SASSERTEQUALS(_commitCount, SToUInt64(result[0][0]));

        // Next confirm that the final hash matches what we think the
        // incremental hash should be for that commit (if we have one).
        if (!_committedHash.empty()) {
            // Verify the hash is right
            const string& query = (string) "SELECT hash FROM journal WHERE id=" + SToStr(_commitCount);
            SASSERT(SQuery(_db, "verifying DB hash", query.c_str(), result));
            SASSERTWARN(!result.empty());
            SASSERTWARN(_committedHash == result[0][0]);
        }
    }

    // Close the DB.
    SASSERT(!sqlite3_close(_db));
    DBINFO("Database closed.");
}

// --------------------------------------------------------------------------
bool SQLite::beginTransaction() {
    SASSERT(!_readOnly);
    SASSERT(!_insideTransaction);
    SASSERT(_uncommittedHash.empty());
    SASSERT(_uncommittedQuery.empty());
    // Begin the next transaction
    SDEBUG("Beginning transaction #" << _commitCount + 1);
    uint64_t before = STimeNow();
    _insideTransaction = SQuery(_db, "starting db transaction", "BEGIN TRANSACTION");
    _beginElapsed = STimeNow() - before;
    _readElapsed = 0;
    _writeElapsed = 0;
    _prepareElapsed = 0;
    _commitElapsed = 0;
    _rollbackElapsed = 0;
    return _insideTransaction;
}

// --------------------------------------------------------------------------
bool SQLite::verifyTable(const string& tableName, const string& sql, bool& created) {
    SASSERT(!SEndsWith(sql, ";")); // sqlite trims semicolon, so let's not supply it else we get confused later
    // First, see if it's there
    SQResult result;
    SASSERT(read("SELECT sql FROM sqlite_master WHERE type='table' AND tbl_name=" + SQ(tableName) + ";", result));
    const string& collapsedSQL = SCollapse(sql);
    if (result.empty()) {
        // Table doesn't already exist, create it
        SINFO("Creating '" << tableName << "': " << collapsedSQL);
        SASSERT(write(collapsedSQL + ";"));
        created = true;
        return true; // New table was created to spec
    } else {
        // Table exists, verify it's correct.  Now, this can be a little tricky.
        // We'll count "correct" as having all the correct columns, in the correct
        // order.  However, the whitespace can differ.
        SASSERT(!result[0].empty());
        created = false;
        const string& collapsedResult = SCollapse(result[0][0]);
        if (SStrip(collapsedResult, " ", false) == SStrip(collapsedSQL, " ", false)) {
            // Looking good
            SINFO("'" << tableName << "' already exists with correct schema.");
            return true;
        } else {
            // Not right -- need to upgrade?
            SHMMM("'" << tableName << "' has incorrect schema, need to upgrade? Is '" << collapsedResult << "'");
            return false;
        }
    }
}

// --------------------------------------------------------------------------
bool SQLite::addColumn(const string& tableName, const string& column, const string& columnType) {
    // Add a column to the table if it does not exist.  Totally freak out on error.
    const string& sql =
        SCollapse(read("SELECT sql FROM sqlite_master WHERE type='table' AND tbl_name='" + tableName + "';"));
    if (!SContains(sql, " " + column + " ")) {
        // Add column
        SINFO("Adding " << column << " " << columnType << " to " << tableName);
        SASSERT(write("ALTER TABLE " + tableName + " ADD COLUMN " + column + " " + columnType + ";"));
        return true;
    }
    SWARN("Schema upgrade failed for table " << tableName << ", unrecognized sql '" << sql << "'");
    return false;
}

// --------------------------------------------------------------------------
string SQLite::read(const string& query) {
    // Execute the read-only query
    SQResult result;
    if (!read(query, result))
        return "";
    if (result.empty() || result[0].empty())
        return "";
    return result[0][0];
}

// --------------------------------------------------------------------------
bool SQLite::read(const string& query, SQResult& result) {
    // Execute the read-only query
    SASSERTWARN(!SContains(SToUpper(query), "INSERT "));
    SASSERTWARN(!SContains(SToUpper(query), "UPDATE "));
    SASSERTWARN(!SContains(SToUpper(query), "DELETE "));
    uint64_t before = STimeNow();
    bool queryResult = SQuery(_db, "read only query", query, result);
    _readElapsed += STimeNow() - before;
    return queryResult;
}

// --------------------------------------------------------------------------
bool SQLite::write(const string& query) {
    SASSERT(!_readOnly);
    SASSERT(_insideTransaction);
    SASSERT(SEndsWith(query, ";"));                                         // Must finish everything with semicolon
    SASSERTWARN(SToUpper(query).find("CURRENT_TIMESTAMP") == string::npos); // Else will be replayed wrong
    // First, check our current state
    SQResult results;
    SASSERT(SQuery(_db, "looking up schema version", "PRAGMA schema_version;", results));
    SASSERT(!results.empty() && !results[0].empty());
    uint64_t schemaBefore = SToUInt64(results[0][0]);
    uint64_t changesBefore = sqlite3_total_changes(_db);

    // Try to execute the query
    uint64_t before = STimeNow();
    bool result = SQuery(_db, "read/write transaction", query);
    _writeElapsed += STimeNow() - before;
    if (!result) {
        return false;
    }

    // See if the query changed anything
    SASSERT(SQuery(_db, "looking up schema version", "PRAGMA schema_version;", results));
    SASSERT(!results.empty() && !results[0].empty());
    uint64_t schemaAfter = SToUInt64(results[0][0]);
    uint64_t changesAfter = sqlite3_total_changes(_db);

    // Did something change.
    if (schemaAfter > schemaBefore || changesAfter > changesBefore) {
        // Changed, add to the uncommitted query
        _uncommittedQuery += query;
        _lastWriteChanged = true;
    } else {
        // Nothing changed
        _lastWriteChanged = false;
    }
    return true;
}

// --------------------------------------------------------------------------
bool SQLite::prepare() {
    SASSERT(_insideTransaction);
    // Queue up the journal entry
    _uncommittedHash = SToHex(SHashSHA1(_committedHash + _uncommittedQuery));
    uint64_t before = STimeNow();
    bool result = SQuery(_db, "updating journal", "INSERT INTO journal VALUES ( NULL, " + SQ(_uncommittedQuery) + ", " +
                                                      SQ(_uncommittedHash) + " )");
    _prepareElapsed += STimeNow() - before;
    if (!result) {
        // Couldn't insert into the journal; roll back the original commit
        SWARN("Unable to prepare transaction, rolling back: " << _uncommittedQuery);
        rollback();
        return false;
    }

    // Ready to commit
    SDEBUG("Prepared transaction");
    return true;
}

// --------------------------------------------------------------------------
void SQLite::commit() {
    SASSERT(_insideTransaction);
    SASSERT(!_uncommittedHash.empty()); // Must prepare first
    // Do we need to truncate as we go?
    bool truncating = false;
    if (_journalSize + 1 > _maxJournalSize) {
        // Delete the oldest entry
        truncating = true;
        uint64_t before = STimeNow();
        SASSERT(SQuery(_db, "Deleting oldest row",
                       "DELETE FROM journal WHERE id=" + SToStr(_commitCount - _journalSize + 1)));
        _writeElapsed += STimeNow() - before;
    }

    // Make sure one is ready to commit
    SDEBUG("Committing transaction");
    uint64_t before = STimeNow();
    SASSERT(SQuery(_db, "committing db transaction", "COMMIT"));
    _commitElapsed += STimeNow() - before;

    // Successful commit
    ++_commitCount;
    _journalSize += !truncating; // Only increase if we didn't truncate by a row
    _committedHash = _uncommittedHash;
    _insideTransaction = false;
    _uncommittedHash.clear();
    _uncommittedQuery.clear();
}

// --------------------------------------------------------------------------
void SQLite::rollback() {
    // Make sure we're actually inside a transaction
    if (_insideTransaction) {
        // Cancel this transaction
        SINFO("Rolling back transaction: " << _uncommittedQuery);
        uint64_t before = STimeNow();
        SASSERT(SQuery(_db, "rolling back db transaction", "ROLLBACK"));
        _rollbackElapsed += STimeNow() - before;
        if (!_uncommittedQuery.empty() && _rollbackElapsed > 1000 * STIME_US_PER_MS) {
            SALERT("[opszy] Freak out! Rolled back an actual query in " << (_rollbackElapsed / STIME_US_PER_MS)
                                                                        << "ms.");
        }
    } else {
        SWARN("Rolling back but not inside transaction, ignoring.");
    }
    _insideTransaction = false;
    _uncommittedHash.clear();
    _uncommittedQuery.clear();
}

// --------------------------------------------------------------------------
uint64_t SQLite::getLastTransactionTiming(uint64_t& begin, uint64_t& read, uint64_t& write, uint64_t& prepare,
                                          uint64_t& commit, uint64_t& rollback) {
    // Just populate and return
    begin = _beginElapsed;
    read = _readElapsed;
    write = _writeElapsed;
    prepare = _prepareElapsed;
    commit = _commitElapsed;
    rollback = _rollbackElapsed;
    return begin + read + write + prepare + commit + rollback;
}

// --------------------------------------------------------------------------
bool SQLite::getCommit(uint64_t id, string& query, string& hash) {
    SASSERTWARN(SWITHIN(1, id, _commitCount));
    // Look up the query and hash for the given commit
    SDEBUG("Getting commit #" << id);
    const string& sql = "SELECT query, hash FROM journal WHERE id=" + SToStr(id);
    SQResult result;
    if (!SQuery(_db, "getting commit", sql, result))
        return false;
    SASSERTWARN(result.size() == 1);
    if (result.size() != 1)
        return false;
    SASSERTWARN(result[0].size() == 2);
    if (result[0].size() != 2)
        return false;
    query = result[0][0];
    hash = result[0][1];
    SASSERTWARN(!query.empty());
    SASSERTWARN(!hash.empty());
    return (!query.empty() && !hash.empty());
}

// --------------------------------------------------------------------------
bool SQLite::getCommits(uint64_t fromIndex, uint64_t toIndex, SQResult& result) {
    SASSERTWARN(SWITHIN(1, fromIndex, toIndex));
    SASSERTWARN(SWITHIN(fromIndex, toIndex, _commitCount));
    // Look up all the queries within that range
    SDEBUG("Getting commits #" << fromIndex << "-" << toIndex);
    const string& sql = "SELECT hash, query FROM journal WHERE id >= " + SToStr(fromIndex) + " AND id <= " +
                        SToStr(toIndex) + " ORDER BY id";
    return SQuery(_db, "getting commits", sql, result);
}

// --------------------------------------------------------------------------
int64_t SQLite::getLastInsertRowID() {
    // Make sure it *does* happen after an INSERT, but not with a IGNORE
    SASSERTWARN(SContains(_uncommittedQuery, "INSERT") || SContains(_uncommittedQuery, "REPLACE"));
    SASSERTWARN(!SContains(_uncommittedQuery, "IGNORE"));
    int64_t sqliteRowID = (int64_t)sqlite3_last_insert_rowid(_db);
    return sqliteRowID;
}

// --------------------------------------------------------------------------
uint64_t SQLite::getCommitCount() {
    if (_readOnly) {
        SQResult result;
        SASSERT(SQuery(_db, "getting commit count", "SELECT MAX(id) FROM journal", result));
        return !result.empty() ? SToUInt64(result[0][0]) : 0;
    } else
        return _commitCount;
}

// --------------------------------------------------------------------------
void SQLite::_setPragmas(sqlite3* db, int autoCheckpoint) {
    // Occasionally a read-only thread will complain that the database is locked. This is only seems to
    // happen at startup when we open many sqlite3 connections at once.  SQLite will use an exponential
    // backoff to keep trying to query until 1000ms has passed. At that point SQLITE_BUSY is returned.
    SASSERT(SQuery(db, "Setting busy timeout to 1000ms", "PRAGMA busy_timeout = 1000;"));

    // WAL is what allows simultanous read/writing.
    SASSERT(SQuery(db, "enabling write ahead logging", "PRAGMA journal_mode = WAL;"));

    // PRAGMA legacy_file_format=OFF sets the default for creating new
    // databases, so it must be called before creating any tables to be
    // effective.
    SASSERT(SQuery(db, "new file format for DESC indexes", "PRAGMA legacy_file_format = OFF"));

    // These other pragma only relate to read/write databases.
    if (!_readOnly) {
        SASSERT(SQuery(db, "disabling synchronous commits", "PRAGMA synchronous = OFF;"));
        SASSERT(SQuery(db, "disabling change counting", "PRAGMA count_changes = OFF;"));
        SASSERT(autoCheckpoint >= 0);
        DBINFO("Enabling automatic checkpointing every " << SToStr(autoCheckpoint) << " pages.");
        SASSERT(SQuery(db, "enabling auto-checkpointing", "PRAGMA wal_autocheckpoint = " + SQ(autoCheckpoint) + ";"));
    }
}

size_t SQLite::getLastWriteChangeCount() {
    int count = sqlite3_changes(_db);
    return count > 0 ? (size_t)count : 0;
}
