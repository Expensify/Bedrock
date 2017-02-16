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
recursive_mutex SQLite::_commitLock;
SLockTimer<recursive_mutex> SQLite::commitLock("Commit Lock", _commitLock);
recursive_mutex SQLite::_hashLock;
atomic<uint64_t> SQLite::_commitCount(0);
atomic<int> SQLite::_dbInitialized(0);

map<uint64_t, pair<string, string>> SQLite::_inFlightTransactions;
set<uint64_t> SQLite::_committedtransactionIDs;
string SQLite::_lastCommittedHash;

void SQLite::sqliteLogCallback(void* pArg, int iErrCode, const char* zMsg) {
    SSYSLOG(LOG_INFO, SWHEREAMI << "[info] "
                                << "{SQLITE} Code: " << iErrCode << ", Message: " << zMsg);
}

SQLite::SQLite(const string& filename, int cacheSize, int autoCheckpoint, bool readOnly, int maxJournalSize,
               int journalTable, int minJournalTables) {
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
        sqlite3_initialize();
        sqliteInitialized = true;
    }

    // Initialize sqlite multithreaded magic.
    // **NOTE: each thread will need to open its own connection.
    SASSERT(sqlite3_threadsafe()); // Make sure sqlite is compiled in threadsafe mode.

    // Disabled by default, but lets really beat it in.  This way checkpointing
    // does not need to wait on locks created in this thread.
    SASSERT(sqlite3_enable_shared_cache(0) == SQLITE_OK);

    // Set our journal table name.
    _journalName = getJournalTableName(journalTable);

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
        for (int i = -1; i <= minJournalTables; i++) {
            if (SQVerifyTable(_db, getJournalTableName(i), "CREATE TABLE " + getJournalTableName(i) +
                              " ( id INTEGER PRIMARY KEY, query TEXT, hash TEXT )")) {
                SHMMM("Created " << getJournalTableName(i) << " table.");
            }
        }
    } else {
        // Open a read-only database
        SASSERT(!sqlite3_open_v2(filename.c_str(), &_db, DB_READ_OPEN_FLAGS, NULL));
    }

    // Figure out which journal tables actually exist. They must be sequential.
    int currentJounalTable = -1;
    while(true) {
        string name = getJournalTableName(currentJounalTable);
        if (SQVerifyTableExists(_db, name)) {
            _allJournalNames.push_back(name);
            currentJounalTable++;
        } else {
            // That's all of them.
            break;
        }
    }

    // Set a one-second timeout for automatic retries in case of SQLITE_BUSY.
    sqlite3_busy_timeout(_db, 1000);

    // Update the cache
    SINFO("Setting cache_size to " << cacheSize << "KB");
    SQuery(_db, "increasing cache size",
           "PRAGMA cache_size = -" + SQ(cacheSize) + ";"); // -size means KB; +size means pages

    // We just keep track of the number of rows in each journal, and delete if we have too many.
    SQResult result;
    SASSERT(!SQuery(_db, "getting commit count", "SELECT COUNT(*) FROM " + _journalName + ";", result));
    _journalSize = SToUInt64(result[0][0]);

    // If we're the first thread to get to it, we'll initialize the DB.

    // First we grab the commit and hash locks so nobody else can try to commit, or read the last hash.
    SQLITE_COMMIT_AUTOLOCK;
    lock_guard<recursive_mutex> h_lock(_hashLock);

    // Now we'll see if the DB's initialized.
    int alreadyInitialized = _dbInitialized.fetch_add(1);

    // Nobody'd initialized this before! Let's do it now.
    if (!alreadyInitialized) {
        // Read the highest commit count from the database, and store it in _commitCount.
        uint64_t commitCount = _getCommitCount();
        _commitCount.store(commitCount);

        // And then read the hash for that transaction.
        string lastCommittedHash, ignore;
        getCommit(commitCount, ignore, lastCommittedHash);
        _lastCommittedHash = lastCommittedHash;

        // If we have a commit count, we should have a hash as well.
        if (commitCount && lastCommittedHash.empty()) {
            SWARN("[TYLER] Loaded commit count " << commitCount << " with empty hash!");
        } else {
            SWARN("[TYLER] Loaded commit count " << commitCount << " with hash " << _lastCommittedHash);
        }
    }

    // Alright, done, we'll release locks and continue on!
}

string SQLite::getJournalQuery(const list<string>& queryParts, bool append) {
    list<string> queries;
    for (string& name : _allJournalNames) {
        queries.emplace_back(SComposeList(queryParts, " " + name + " ") + (append ? " " + name : ""));
    }
    string query = SComposeList(queries, " UNION ");
    return query;
}

string SQLite::getJournalTableName(int journalTableID)
{
    string name = "journal";
    if (journalTableID >= 0 && journalTableID <= 9) {
        name += "0";
    }
    if (journalTableID >= 0) {
        name += to_string(journalTableID);
    }
    return name;
}

SQLite::~SQLite() {
    // Close the database -- first rollback any incomplete transaction
    if (!_uncommittedQuery.empty()) {
        rollback();
    }

    DBINFO("Closing database '" << _filename << ".");
    SASSERTWARN(_uncommittedQuery.empty());

    // Close the DB.
    SASSERT(!sqlite3_close(_db));
    DBINFO("Database closed.");
}

bool SQLite::beginTransaction() {
    SASSERT(!_readOnly);
    SASSERT(!_insideTransaction);
    SASSERT(_uncommittedHash.empty());
    SASSERT(_uncommittedQuery.empty());
    // Begin the next transaction
    SDEBUG("Beginning transaction");
    uint64_t before = STimeNow();
    _insideTransaction = !SQuery(_db, "starting db transaction", "BEGIN TRANSACTION");
    _beginElapsed = STimeNow() - before;
    _readElapsed = 0;
    _writeElapsed = 0;
    _prepareElapsed = 0;
    _commitElapsed = 0;
    _rollbackElapsed = 0;
    return _insideTransaction;
}

bool SQLite::beginConcurrentTransaction() {
    SASSERT(!_readOnly);
    SASSERT(!_insideTransaction);
    SASSERT(_uncommittedHash.empty());
    SASSERT(_uncommittedQuery.empty());
    // Begin the next transaction
    SDEBUG("[concurrent] Beginning transaction");
    uint64_t before = STimeNow();
    // This breaks for a variety of tests we'll need to fix.
    _insideTransaction = !SQuery(_db, "starting db transaction", "BEGIN CONCURRENT");
    _beginElapsed = STimeNow() - before;
    _readElapsed = 0;
    _writeElapsed = 0;
    _prepareElapsed = 0;
    _commitElapsed = 0;
    _rollbackElapsed = 0;
    return _insideTransaction;
}

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

string SQLite::read(const string& query) {
    // Execute the read-only query
    SQResult result;
    if (!read(query, result))
        return "";
    if (result.empty() || result[0].empty())
        return "";
    return result[0][0];
}

bool SQLite::read(const string& query, SQResult& result) {
    // Execute the read-only query
    SASSERTWARN(!SContains(SToUpper(query), "INSERT "));
    SASSERTWARN(!SContains(SToUpper(query), "UPDATE "));
    SASSERTWARN(!SContains(SToUpper(query), "DELETE "));
    uint64_t before = STimeNow();
    bool queryResult = !SQuery(_db, "read only query", query, result);
    _readElapsed += STimeNow() - before;
    return queryResult;
}

bool SQLite::write(const string& query, bool dontCheckSchema) {
    SASSERT(!_readOnly);
    SASSERT(_insideTransaction);
    SASSERT(SEndsWith(query, ";"));                                         // Must finish everything with semicolon
    SASSERTWARN(SToUpper(query).find("CURRENT_TIMESTAMP") == string::npos); // Else will be replayed wrong
    // First, check our current state
    SQResult results;
    uint64_t schemaBefore = 0;
    uint64_t schemaAfter = 0;

    if (!dontCheckSchema) {
        SASSERT(!SQuery(_db, "looking up schema version", "PRAGMA schema_version;", results));
        SASSERT(!results.empty() && !results[0].empty());
        schemaBefore = SToUInt64(results[0][0]);
    }
    uint64_t changesBefore = sqlite3_total_changes(_db);

    // Try to execute the query
    uint64_t before = STimeNow();
    bool result = !SQuery(_db, "read/write transaction", query);
    _writeElapsed += STimeNow() - before;
    if (!result) {
        return false;
    }

    // See if the query changed anything
    if (!dontCheckSchema) {
        SASSERT(!SQuery(_db, "looking up schema version", "PRAGMA schema_version;", results));
        SASSERT(!results.empty() && !results[0].empty());
        schemaAfter = SToUInt64(results[0][0]);
    }
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

bool SQLite::prepare() {
    SASSERT(_insideTransaction);

    // We lock this here, so that we can guarantee the order in which commits show up in the database.
    commitLock.lock();

    // Now that we've locked anybody else from committing, look up the state of the database.
    string committedQuery, committedHash;
    uint64_t commitCount = _commitCount.load();

    // Queue up the journal entry
    string lastCommittedHash = getCommittedHash();
    _uncommittedHash = SToHex(SHashSHA1(lastCommittedHash + _uncommittedQuery));
    _uncommittedTransaction = true;
    uint64_t before = STimeNow();

    // Let the DB auto-increment this.
    string query = "INSERT INTO " + _journalName + " VALUES (" + SQ(commitCount + 1) + ", " + SQ(_uncommittedQuery) + ", " + SQ(_uncommittedHash) + " )";

    // These are the values we're currently operating on, until we either commit or rollback.
    _uncommittedID = commitCount + 1;
    _inFlightTransactions[commitCount + 1] = make_pair(_uncommittedQuery, _uncommittedHash);

    int result = SQuery(_db, "updating journal", query);
    _prepareElapsed += STimeNow() - before;
    if (result) {
        // Couldn't insert into the journal; roll back the original commit
        SWARN("Unable to prepare transaction, got result: " << result << ". Rolling back: " << _uncommittedQuery);
        rollback();
        commitLock.unlock();
        return false;
    }

    // Ready to commit
    SDEBUG("Prepared transaction");

    // We're still holding commitLock now, and will until the commit is complete.
    return true;
}

int SQLite::commit() {
    SASSERT(_insideTransaction);
    SASSERT(!_uncommittedHash.empty()); // Must prepare first

    int result = 0;
    // Do we need to truncate as we go?
    bool truncating = false;
    if (_journalSize + 1 > _maxJournalSize) {
        // Delete the oldest entry
        truncating = true;
        uint64_t before = STimeNow();
        string query = "DELETE FROM" + _journalName + " WHERE id = MIN(id);";
        SASSERT(!SQuery(_db, "Deleting oldest row", query));
        _writeElapsed += STimeNow() - before;
    }

    // Make sure one is ready to commit
    SDEBUG("Committing transaction");
    uint64_t before = STimeNow();
    SWARN("[SLOW2] pre COMMIT");
    result = SQuery(_db, "committing db transaction", "COMMIT");
    SWARN("[SLOW2] COMMITTED");

    // If there were conflicting commits, will return SQLITE_BUSY_SNAPSHOT
    SASSERT(result == SQLITE_OK || result == SQLITE_BUSY_SNAPSHOT);

    if (result == SQLITE_OK) {
        _commitElapsed += STimeNow() - before;
        // Successful commit
        _journalSize += !truncating; // Only increase if we didn't truncate by a row

        _committedtransactionIDs.insert(_uncommittedID);
        {
            // Update atomically.
            lock_guard<recursive_mutex> lock(_hashLock);
            _lastCommittedHash = _uncommittedHash;
            SINFO("[TYLER] Updated last committed hash to: " << _lastCommittedHash << " (commit: " << _uncommittedID << ").");
        }

        // Ok, someone else can commit now.
        // Incrementing the commit count.
        _commitCount++;
        SINFO("Commit successful (" << _commitCount.load() << "), releasing commitLock.");

        _insideTransaction = false;
        _uncommittedTransaction = false;
        _uncommittedHash.clear();
        _uncommittedQuery.clear();
        commitLock.unlock();
    } else {
        SINFO("Commit failed, waiting for rollback.");
    }

    // if we got SQLITE_BUSY_SNAPSHOT, then we're *still* holding commitLock, and it will need to be unlocked by
    // calling rollback().
    return result;
}

map<uint64_t, pair<string,string>> SQLite::getCommittedTransactions() {
    SQLITE_COMMIT_AUTOLOCK;

    map<uint64_t, pair<string,string>> result;
    if (_committedtransactionIDs.empty()) {
        return result;
    }

    for (uint64_t key : _committedtransactionIDs) {
        result[key] = move(_inFlightTransactions.at(key));
        _inFlightTransactions.erase(key);
    }

    _committedtransactionIDs.clear();
    return result;
}

void SQLite::rollback() {
    // Make sure we're actually inside a transaction
    if (_insideTransaction) {
        // Cancel this transaction
        SINFO("Rolling back transaction: " << _uncommittedQuery);
        uint64_t before = STimeNow();
        SASSERT(!SQuery(_db, "rolling back db transaction", "ROLLBACK"));
        _rollbackElapsed += STimeNow() - before;

        // Finally done with this.
        _insideTransaction = false;
        _uncommittedTransaction = false;
        _uncommittedHash.clear();
        _uncommittedQuery.clear();
        SINFO("Rollback successful, releasing commitLock for.");
        commitLock.unlock();
    } else {
        SWARN("Rolling back but not inside transaction, ignoring.");
    }
}

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



bool SQLite::getCommit(uint64_t id, string& query, string& hash) {

    // TODO: This can fail if called after `BEGIN TRANSACTION`.

    // Look up the query and hash for the given commit
    string q= getJournalQuery({"SELECT query, hash FROM", "WHERE id = " + SQ(id)}); 
    SQResult result;
    SASSERT(!SQuery(_db, "getting commit", q, result));

    if (!result.empty()) {
        query = result[0][0];
        hash = result[0][1];
    } else {
        query = "";
        hash = "";
    }
    if (id) {
        SASSERTWARN(!query.empty());
        SASSERTWARN(!hash.empty());
    }
    return (!query.empty() && !hash.empty());
}

string SQLite::getCommittedHash() { 
    lock_guard<recursive_mutex> lock(_hashLock);
    return _lastCommittedHash;
}

bool SQLite::getCommits(uint64_t fromIndex, uint64_t toIndex, SQResult& result) {

    SASSERTWARN(SWITHIN(1, fromIndex, toIndex));

    string query = getJournalQuery({"SELECT hash, query FROM", "WHERE id >= " + SQ(fromIndex) +
                                    (toIndex ? " AND id <= " + SQ(toIndex) : "")});

    SDEBUG("Getting commits #" << fromIndex << "-" << toIndex);
    return !SQuery(_db, "getting commits", query, result);
}

int64_t SQLite::getLastInsertRowID() {
    // Make sure it *does* happen after an INSERT, but not with a IGNORE
    SASSERTWARN(SContains(_uncommittedQuery, "INSERT") || SContains(_uncommittedQuery, "REPLACE"));
    SASSERTWARN(!SContains(_uncommittedQuery, "IGNORE"));
    int64_t sqliteRowID = (int64_t)sqlite3_last_insert_rowid(_db);
    return sqliteRowID;
}

uint64_t SQLite::getCommitCount() {
    return _commitCount.load();
}

uint64_t SQLite::_getCommitCount() {
    string query = getJournalQuery({"SELECT MAX(id) as maxIDs FROM"}, true);
    query = "SELECT MAX(maxIDs) FROM (" + query + ") ORDER BY maxIDs desc";
    SQResult result;
    SASSERT(!SQuery(_db, "getting commit count", query, result));

    if (result.empty()) {
        return 0;
    }
    return SToUInt64(result[0][0]);
}

void SQLite::_setPragmas(sqlite3* db, int autoCheckpoint) {
    // Occasionally a read-only thread will complain that the database is locked. This is only seems to
    // happen at startup when we open many sqlite3 connections at once.  SQLite will use an exponential
    // backoff to keep trying to query until 1000ms has passed. At that point SQLITE_BUSY is returned.
    SASSERT(!SQuery(db, "Setting busy timeout to 1000ms", "PRAGMA busy_timeout = 1000;"));

    // WAL is what allows simultanous read/writing.
    SASSERT(!SQuery(db, "enabling write ahead logging", "PRAGMA journal_mode = WAL;"));

    // PRAGMA legacy_file_format=OFF sets the default for creating new
    // databases, so it must be called before creating any tables to be
    // effective.
    SASSERT(!SQuery(db, "new file format for DESC indexes", "PRAGMA legacy_file_format = OFF"));

    // These other pragma only relate to read/write databases.
    if (!_readOnly) {
        SASSERT(!SQuery(db, "disabling synchronous commits", "PRAGMA synchronous = OFF;"));
        SASSERT(!SQuery(db, "disabling change counting", "PRAGMA count_changes = OFF;"));
        SASSERT(autoCheckpoint >= 0);
        DBINFO("Enabling automatic checkpointing every " << SToStr(autoCheckpoint) << " pages.");
        SASSERT(!SQuery(db, "enabling auto-checkpointing", "PRAGMA wal_autocheckpoint = " + SQ(autoCheckpoint) + ";"));
    }
}

size_t SQLite::getLastWriteChangeCount() {
    int count = sqlite3_changes(_db);
    return count > 0 ? (size_t)count : 0;
}
