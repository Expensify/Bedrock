#include <libstuff/libstuff.h>
#include "SQLite.h"

#define DBINFO(_MSG_) SINFO("{" << _filename << "} " << _MSG_)

// Create all of our static variables.
atomic<uint64_t>                    SQLite::_commitCount(0);
recursive_mutex                     SQLite::_commitLock;
set<uint64_t>                       SQLite::_committedTransactionIDs;
map<uint64_t, pair<string, string>> SQLite::_inFlightTransactions;
atomic<string>                      SQLite::_lastCommittedHash;
atomic_flag                         SQLite::_sqliteInitialized = ATOMIC_FLAG_INIT;

// This is our only public static variable. It needs to be initialized after `_commitLock`.
SLockTimer<recursive_mutex> SQLite::g_commitLock("Commit Lock", SQLite::_commitLock);

SQLite::SQLite(const string& filename, int cacheSize, int autoCheckpoint, int maxJournalSize, int journalTable,
               int maxRequiredJournalTableID) :
    whitelist(nullptr)
{
    // Initialize
    SINFO("Opening sqlite database");
    SASSERT(!filename.empty());
    SASSERT(cacheSize > 0);
    SASSERT(autoCheckpoint >= 0);
    SASSERT(maxJournalSize > 0);
    _filename = filename;
    _insideTransaction = false;
    _maxJournalSize = maxJournalSize;
    _beginElapsed = 0;
    _readElapsed = 0;
    _writeElapsed = 0;
    _prepareElapsed = 0;
    _commitElapsed = 0;
    _rollbackElapsed = 0;

    // Set our journal table name.
    _journalName = _getJournalTableName(journalTable);

    // There are several initialization tasks that need to be performed only by the *first* thread to initialize the
    // DB. We grab this lock and set a flag so that only the first thread to reach this point can perform this
    // operation, and any other threads will be blocked until it's complete.
    SQLITE_COMMIT_AUTOLOCK;
    bool initializer = !_sqliteInitialized.test_and_set();

    // We need to initialize sqlite. Only the first thread to get here will do this.
    if (initializer) {
        sqlite3_config(SQLITE_CONFIG_LOG, _sqliteLogCallback, 0);
        sqlite3_initialize();
        SASSERT(sqlite3_threadsafe());

        // Disabled by default, but lets really beat it in. This way checkpointing does not need to wait on locks
        // created in this thread.
        SASSERT(sqlite3_enable_shared_cache(0) == SQLITE_OK);
    }

    // Open the DB in read-write mode.
    DBINFO((SFileExists(_filename) ? "Opening" : "Creating") << " database '" << _filename << "'.");
    const int DB_WRITE_OPEN_FLAGS = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX;
    SASSERT(!sqlite3_open_v2(filename.c_str(), &_db, DB_WRITE_OPEN_FLAGS, NULL));

    // Set a one-second timeout for automatic retries in case of SQLITE_BUSY.
    sqlite3_busy_timeout(_db, 1000);

    // WAL is what allows simultaneous read/writing.
    SASSERT(!SQuery(_db, "enabling write ahead logging", "PRAGMA journal_mode = WAL;"));

    // PRAGMA legacy_file_format=OFF sets the default for creating new databases, so it must be called before creating
    // any tables to be effective.
    SASSERT(!SQuery(_db, "new file format for DESC indexes", "PRAGMA legacy_file_format = OFF"));

    // These other pragmas only relate to read/write databases.
    SASSERT(!SQuery(_db, "disabling synchronous commits", "PRAGMA synchronous = OFF;"));
    SASSERT(!SQuery(_db, "disabling change counting", "PRAGMA count_changes = OFF;"));
    DBINFO("Enabling automatic checkpointing every " << autoCheckpoint << " pages.");
    sqlite3_wal_autocheckpoint(_db, autoCheckpoint);

    // Update the cache. -size means KB; +size means pages
    SINFO("Setting cache_size to " << cacheSize << "KB");
    SQuery(_db, "increasing cache size", "PRAGMA cache_size = -" + SQ(cacheSize) + ";");

    // Now we verify (and create if non-existent) all of our required journal tables.
    for (int i = -1; i <= maxRequiredJournalTableID; i++) {
        if (SQVerifyTable(_db, _getJournalTableName(i), "CREATE TABLE " + _getJournalTableName(i) +
                          " ( id INTEGER PRIMARY KEY, query TEXT, hash TEXT )")) {
            SHMMM("Created " << _getJournalTableName(i) << " table.");
        }
    }

    // And we'll figure out which journal tables actually exist, which may be more than we require. They must be
    // sequential.
    int currentJounalTable = -1;
    while(true) {
        string name = _getJournalTableName(currentJounalTable);
        if (SQVerifyTableExists(_db, name)) {
            _allJournalNames.push_back(name);
            currentJounalTable++;
        } else {
            break;
        }
    }

    // We keep track of the number of rows in the journal, so that we can delete old entries when we're over our size
    // limit.
    // We want the min of all journal tables.
    string minQuery = _getJournalQuery({"SELECT MIN(id) AS id FROM"}, true);
    minQuery = "SELECT MIN(id) AS id FROM (" + minQuery + ")";

    // And the max.
    string maxQuery = _getJournalQuery({"SELECT MAX(id) AS id FROM"}, true);
    maxQuery = "SELECT MAX(id) AS id FROM (" + maxQuery + ")";

    // Look up the min and max values in the database.
    SQResult result;
    SASSERT(!SQuery(_db, "getting commit min", minQuery, result));
    uint64_t min = SToUInt64(result[0][0]);
    SASSERT(!SQuery(_db, "getting commit max", maxQuery, result));
    uint64_t max = SToUInt64(result[0][0]);

    // And save the difference as the size of the journal.
    _journalSize = max - min;

    // Now that the DB's all up and running, we can load our global data from it, if we're the initializer thread.
    if (initializer) {
        // Read the highest commit count from the database, and store it in _commitCount.
        uint64_t commitCount = _getCommitCount();
        _commitCount.store(commitCount);

        // And then read the hash for that transaction.
        string lastCommittedHash, ignore;
        getCommit(commitCount, ignore, lastCommittedHash);
        _lastCommittedHash.store(lastCommittedHash);

        // If we have a commit count, we should have a hash as well.
        if (commitCount && lastCommittedHash.empty()) {
            SWARN("Loaded commit count " << commitCount << " with empty hash.");
        }
    }

    // Register the authorizer callback which allows callers to whitelist particular data in the DB.
    sqlite3_set_authorizer(_db, SQLite::_sqliteAuthorizerCallback, this);
}

void SQLite::_sqliteLogCallback(void* pArg, int iErrCode, const char* zMsg) {
    SSYSLOG(LOG_INFO, SWHEREAMI << "[info] " << "{SQLITE} Code: " << iErrCode << ", Message: " << zMsg);
}

string SQLite::_getJournalQuery(const list<string>& queryParts, bool append) {
    list<string> queries;
    for (string& name : _allJournalNames) {
        queries.emplace_back(SComposeList(queryParts, " " + name + " ") + (append ? " " + name : ""));
    }
    string query = SComposeList(queries, " UNION ");
    return query;
}

string SQLite::_getJournalTableName(int journalTableID) {
    if (journalTableID < 0) {
        return "journal";
    }
    char buff[12] = {0};
    sprintf(buff, "journal%04d", journalTableID);
    return buff;
}

SQLite::~SQLite() {
    // First rollback any incomplete transaction
    if (!_uncommittedQuery.empty()) {
        rollback();
    }

    // Close the DB.
    DBINFO("Closing database '" << _filename << ".");
    SASSERTWARN(_uncommittedQuery.empty());
    SASSERT(!sqlite3_close(_db));
    DBINFO("Database closed.");
}

bool SQLite::beginTransaction() {
    SASSERT(!_insideTransaction);
    SASSERT(_uncommittedHash.empty());
    SASSERT(_uncommittedQuery.empty());
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
    SASSERT(!_insideTransaction);
    SASSERT(_uncommittedHash.empty());
    SASSERT(_uncommittedQuery.empty());
    SDEBUG("[concurrent] Beginning transaction");
    uint64_t before = STimeNow();
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
    // sqlite trims semicolon, so let's not supply it else we get confused later
    SASSERT(!SEndsWith(sql, ";"));

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
    if (!read(query, result)) {
        return "";
    }
    if (result.empty() || result[0].empty()) {
        return "";
    }
    return result[0][0];
}

bool SQLite::read(const string& query, SQResult& result) {
    // Execute the read-only query
    SASSERTWARN(!SContains(SToUpper(query), "INSERT "));
    SASSERTWARN(!SContains(SToUpper(query), "UPDATE "));
    SASSERTWARN(!SContains(SToUpper(query), "DELETE "));
    SASSERTWARN(!SContains(SToUpper(query), "REPLACE "));
    uint64_t before = STimeNow();
    bool queryResult = !SQuery(_db, "read only query", query, result);
    _readElapsed += STimeNow() - before;
    return queryResult;
}

bool SQLite::write(const string& query) {
    SASSERT(_insideTransaction);
    SASSERT(SEndsWith(query, ";"));                                         // Must finish everything with semicolon
    SASSERTWARN(SToUpper(query).find("CURRENT_TIMESTAMP") == string::npos); // Else will be replayed wrong

    // First, check our current state
    SQResult results;
    SASSERT(!SQuery(_db, "looking up schema version", "PRAGMA schema_version;", results));
    SASSERT(!results.empty() && !results[0].empty());
    uint64_t schemaBefore = SToUInt64(results[0][0]);
    uint64_t changesBefore = sqlite3_total_changes(_db);

    // Try to execute the query
    uint64_t before = STimeNow();
    bool result = !SQuery(_db, "read/write transaction", query);
    _writeElapsed += STimeNow() - before;
    if (!result) {
        return false;
    }

    // See if the query changed anything
    SASSERT(!SQuery(_db, "looking up schema version", "PRAGMA schema_version;", results));
    SASSERT(!results.empty() && !results[0].empty());
    uint64_t schemaAfter = SToUInt64(results[0][0]);
    uint64_t changesAfter = sqlite3_total_changes(_db);

    // Did something change.
    if (schemaAfter > schemaBefore || changesAfter > changesBefore) {
        // Changed, add to the uncommitted query
        _uncommittedQuery += query;
    }
    return true;
}

bool SQLite::prepare() {
    SASSERT(_insideTransaction);

    // We lock this here, so that we can guarantee the order in which commits show up in the database.
    g_commitLock.lock();
    _mutexLocked = true;

    // Now that we've locked anybody else from committing, look up the state of the database.
    string committedQuery, committedHash;
    uint64_t commitCount = _commitCount.load();

    // Queue up the journal entry
    string lastCommittedHash = getCommittedHash();
    _uncommittedHash = SToHex(SHashSHA1(lastCommittedHash + _uncommittedQuery));
    uint64_t before = STimeNow();

    // Crete our query.
    string query = "INSERT INTO " + _journalName + " VALUES (" + SQ(commitCount + 1) + ", " + SQ(_uncommittedQuery) + ", " + SQ(_uncommittedHash) + " )";

    // These are the values we're currently operating on, until we either commit or rollback.
    _inFlightTransactions[commitCount + 1] = make_pair(_uncommittedQuery, _uncommittedHash);

    int result = SQuery(_db, "updating journal", query);
    _prepareElapsed += STimeNow() - before;
    if (result) {
        // Couldn't insert into the journal; roll back the original commit
        SWARN("Unable to prepare transaction, got result: " << result << ". Rolling back: " << _uncommittedQuery);
        rollback();
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
    uint64_t newJournalSize = _journalSize + 1;
    if (newJournalSize > _maxJournalSize) {
        // Delete the oldest entry
        uint64_t before = STimeNow();
        string query = "DELETE FROM " + _journalName + " "
                       "WHERE id < (SELECT MAX(id) FROM " + _journalName + ") - " + SQ(_maxJournalSize) + " "
                       "LIMIT 10";
        SASSERT(!SQuery(_db, "Deleting oldest journal rows", query));

        // Figure out the new journal size.
        SQResult result;
        SASSERT(!SQuery(_db, "getting commit min", "SELECT MIN(id) AS id FROM " + _journalName, result));
        uint64_t min = SToUInt64(result[0][0]);
        SASSERT(!SQuery(_db, "getting commit max", "SELECT MAX(id) AS id FROM " + _journalName, result));
        uint64_t max = SToUInt64(result[0][0]);
        newJournalSize = max - min;

        // Log timing info.
        _writeElapsed += STimeNow() - before;
    }

    // Make sure one is ready to commit
    SDEBUG("Committing transaction");
    uint64_t before = STimeNow();
    result = SQuery(_db, "committing db transaction", "COMMIT");

    // If there were conflicting commits, will return SQLITE_BUSY_SNAPSHOT
    SASSERT(result == SQLITE_OK || result == SQLITE_BUSY_SNAPSHOT);
    if (result == SQLITE_OK) {
        _commitElapsed += STimeNow() - before;
        _journalSize = newJournalSize;
        _commitCount++;
        _committedTransactionIDs.insert(_commitCount.load());
        _lastCommittedHash.store(_uncommittedHash);
        SDEBUG("Commit successful (" << _commitCount.load() << "), releasing commitLock.");
        _insideTransaction = false;
        _uncommittedHash.clear();
        _uncommittedQuery.clear();
        _mutexLocked = false;
        g_commitLock.unlock();
    } else {
        SINFO("Commit failed, waiting for rollback.");
    }

    // if we got SQLITE_BUSY_SNAPSHOT, then we're *still* holding commitLock, and it will need to be unlocked by
    // calling rollback().
    return result;
}

map<uint64_t, pair<string,string>> SQLite::getCommittedTransactions() {
    SQLITE_COMMIT_AUTOLOCK;

    // Maps a committed transaction ID to the correct query and hash for that transaction.
    map<uint64_t, pair<string,string>> result;

    // If nothing's been committed, nothing to return.
    if (_committedTransactionIDs.empty()) {
        return result;
    }

    // For each transaction that we've committed, we'll remove the that transaction from the "in flight" list, and
    // return that to the caller. This lets SQLiteNode get a list of transactions that have been committed since the
    // last time it called this function, so that it can replicate them to peers.
    for (uint64_t key : _committedTransactionIDs) {
        result[key] = move(_inFlightTransactions.at(key));
        _inFlightTransactions.erase(key);
    }

    // There are no longer any outstanding transactions, so we can clear this.
    _committedTransactionIDs.clear();
    return result;
}

void SQLite::rollback() {
    // Make sure we're actually inside a transaction
    if (_insideTransaction) {
        // Cancel this transaction
        SINFO("Rolling back transaction: " << _uncommittedQuery.substr(0, 100));
        uint64_t before = STimeNow();
        SASSERT(!SQuery(_db, "rolling back db transaction", "ROLLBACK"));
        _rollbackElapsed += STimeNow() - before;

        // Finally done with this.
        _insideTransaction = false;
        _uncommittedHash.clear();
        _uncommittedQuery.clear();
        SINFO("Rollback successful.");

        // Only unlock the mutex if we've previously locked it. We can call `rollback` to cancel a transaction without
        // ever having called `prepare`, which would have locked our mutex.
        if (_mutexLocked) {
            _mutexLocked = false;
            g_commitLock.unlock();
        }
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
    // TODO: This can fail if called after `BEGIN TRANSACTION`, if the id we want to look up was committed by another
    // thread. We may or may never need to handle this case.
    // Look up the query and hash for the given commit
    string q= _getJournalQuery({"SELECT query, hash FROM", "WHERE id = " + SQ(id)});
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
    return _lastCommittedHash.load();
}

bool SQLite::getCommits(uint64_t fromIndex, uint64_t toIndex, SQResult& result) {
    // Look up all the queries within that range
    SASSERTWARN(SWITHIN(1, fromIndex, toIndex));
    string query = _getJournalQuery({"SELECT id, hash, query FROM", "WHERE id >= " + SQ(fromIndex) +
                                    (toIndex ? " AND id <= " + SQ(toIndex) : "")});
    SDEBUG("Getting commits #" << fromIndex << "-" << toIndex);
    query = "SELECT hash, query FROM (" + query  + ") ORDER BY id";
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
    string query = _getJournalQuery({"SELECT MAX(id) as maxIDs FROM"}, true);
    query = "SELECT MAX(maxIDs) FROM (" + query + ")";
    SQResult result;
    SASSERT(!SQuery(_db, "getting commit count", query, result));
    if (result.empty()) {
        return 0;
    }
    return SToUInt64(result[0][0]);
}

size_t SQLite::getLastWriteChangeCount() {
    int count = sqlite3_changes(_db);
    return count > 0 ? (size_t)count : 0;
}

int SQLite::_sqliteAuthorizerCallback(void* pUserData, int actionCode, const char* detail1, const char* detail2,
                                      const char* detail3, const char* detail4)
{
    SQLite* db = static_cast<SQLite*>(pUserData);
    return db->_authorize(actionCode, detail1, detail2);
}

int SQLite::_authorize(int actionCode, const char* table, const char* column) {
    // If the whitelist isn't set, we always return OK.
    if (!whitelist) {
        return SQLITE_OK;
    }

    switch (actionCode) {
        // The following are *always* disallowed in whitelist mode.
        case SQLITE_CREATE_INDEX:
        case SQLITE_CREATE_TABLE:
        case SQLITE_CREATE_TEMP_INDEX:
        case SQLITE_CREATE_TEMP_TABLE:
        case SQLITE_CREATE_TEMP_TRIGGER:
        case SQLITE_CREATE_TEMP_VIEW:
        case SQLITE_CREATE_TRIGGER:
        case SQLITE_CREATE_VIEW:
        case SQLITE_DELETE:
        case SQLITE_DROP_INDEX:
        case SQLITE_DROP_TABLE:
        case SQLITE_DROP_TEMP_INDEX:
        case SQLITE_DROP_TEMP_TABLE:
        case SQLITE_DROP_TEMP_TRIGGER:
        case SQLITE_DROP_TEMP_VIEW:
        case SQLITE_DROP_TRIGGER:
        case SQLITE_DROP_VIEW:
        case SQLITE_INSERT:
        case SQLITE_PRAGMA:
        case SQLITE_TRANSACTION:
        case SQLITE_UPDATE:
        case SQLITE_ATTACH:
        case SQLITE_DETACH:
        case SQLITE_ALTER_TABLE:
        case SQLITE_REINDEX:
        case SQLITE_CREATE_VTABLE:
        case SQLITE_DROP_VTABLE:
        case SQLITE_SAVEPOINT:
        case SQLITE_COPY:
        case SQLITE_RECURSIVE:
            return SQLITE_DENY;
            break;

        // The following are *always* allowed in whitelist mode.
        case SQLITE_SELECT:
        case SQLITE_ANALYZE:
        case SQLITE_FUNCTION:
            return SQLITE_OK;
            break;

        // The following is the only special case where the whitelist actually applies.
        case SQLITE_READ:
        {
            // See if there's an entry in the whitelist for this table.
            auto tableIt = whitelist->find(table);
            if (tableIt != whitelist->end()) {
                // If so, see if there's an entry for this column.
                auto columnIt = tableIt->second.find(column);
                if (columnIt != tableIt->second.end()) {
                    // If so, then this column is whitelisted.
                    return SQLITE_OK;
                }
            }

            // If we didn't find it, not whitelisted.
            SWARN("[security] Non-whitelisted column: " << column << " in table " << table << ".");
            return SQLITE_IGNORE;
        }
    }
    return SQLITE_DENY;
}
