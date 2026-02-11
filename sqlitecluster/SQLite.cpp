#include "SQLite.h"

#include <linux/limits.h>
#include <string.h>

#include <libstuff/libstuff.h>
#include <libstuff/SDeburr.h>
#include <libstuff/SQResult.h>
#include <string>
#include <format>

#define DBINFO(_MSG_) SINFO("{" << _filename << "} " << _MSG_)

// Tracing can only be enabled or disabled globally, not per object.
atomic<bool> SQLite::enableTrace(false);

sqlite3* SQLite::getDBHandle()
{
    return _db;
}

thread_local string SQLite::_mostRecentSQLiteErrorLog;
thread_local int64_t SQLite::_conflictPage;
thread_local string SQLite::_conflictLocation;

const string SQLite::getMostRecentSQLiteErrorLog() const
{
    return _mostRecentSQLiteErrorLog;
}

string SQLite::initializeFilename(const string& filename)
{
    // Canonicalize our filename and save that version.
    if (filename == ":memory:") {
        // This path is special, it exists in memory. This doesn't actually work correctly with journaling and such, as
        // we'll act as if they're all referencing the same file when we're not. This should therefore only be used
        // with a single SQLite object.
        return filename;
    } else {
        char resolvedPath[PATH_MAX];
        char* result = realpath(filename.c_str(), resolvedPath);
        if (!result) {
            if (errno == ENOENT) {
                return filename;
            }
            SERROR("Couldn't resolve pathname for: " << filename);
        }
        return resolvedPath;
    }
}

const set<string>& SQLite::getTablesUsed() const
{
    return _tablesUsed;
}

SQLite::SharedData& SQLite::initializeSharedData(sqlite3* db, const string& filename, const vector<string>& journalNames, bool hctree)
{
    static struct SharedDataLookupMapType
    {
        map<string, SharedData*> m;
        ~SharedDataLookupMapType()
        {
            for (auto& p : m) {
                delete(p.second);
            }
            m.clear();
        }
    } sharedDataLookupMap;

    static mutex instantiationMutex;
    lock_guard<mutex> lock(instantiationMutex);
    auto sharedDataIterator = sharedDataLookupMap.m.find(filename);
    if (sharedDataIterator == sharedDataLookupMap.m.end()) {
        SharedData* sharedData = new SharedData(); // This is never deleted.

        // Look up the existing wal setting for this DB.
        SQResult result;
        SQuery(db, "PRAGMA journal_mode;", result);
        bool isDBCurrentlyUsingWAL2 = result.size() && result[0][0] == "wal2";

        // If the intended wal setting doesn't match the existing wal setting, change it.
        if (!hctree && !isDBCurrentlyUsingWAL2) {
            SASSERT(!SQuery(db, "PRAGMA journal_mode = delete;", result));
            SASSERT(!SQuery(db, "PRAGMA journal_mode = WAL2;", result));
        }

        // Read the highest commit count from the database, and store it in commitCount.
        string query = "SELECT MAX(maxIDs) FROM (" + _getJournalQuery(journalNames, {"SELECT MAX(id) as maxIDs FROM"}, true) + ")";
        SASSERT(!SQuery(db, query, result));
        uint64_t commitCount = result.empty() ? 0 : SToUInt64(result[0][0]);
        sharedData->commitCount = commitCount;

        // And then read the hash for that transaction.
        string lastCommittedHash, ignore;
        getCommit(db, journalNames, commitCount, ignore, lastCommittedHash);
        sharedData->lastCommittedHash.store(lastCommittedHash);

        // If we have a commit count, we should have a hash as well.
        if (commitCount && lastCommittedHash.empty()) {
            SERROR("Loaded commit count " << commitCount << " with empty hash.");
        }

        // Insert our SharedData object into the global map.
        sharedDataLookupMap.m.emplace(filename, sharedData);
        return *sharedData;
    } else {
        // Otherwise, use the existing one.
        return *(sharedDataIterator->second);
    }
}

bool SQLite::validateDBFormat(const string& filename, bool hctree)
{
    // Open the DB in read-write mode.
    const bool fileExists = SFileExists(filename);
    const bool fileIsEmpty = !SFileSize(filename);

    // If the file either doesn't exist, or is empty, we'll create it, or initialize it. However, if it does exist, we'll use the existing
    // file format.
    // Read enough data to determine the format of the file.
    if (fileExists && !fileIsEmpty) {
        FILE* fp = fopen(filename.c_str(), "rb");
        if (fp) {
            char readBuffer[32];
            fread(readBuffer, 1, sizeof(readBuffer), fp);
            readBuffer[31] = 0;
            string headerString(readBuffer);
            SINFO("Existing database format: " << headerString);
            if (SStartsWith(headerString, "Hctree database version")) {
                SINFO("Existing HCTree db");
                hctree = true;
            } else if (SStartsWith(headerString, "SQLite format 3")) {
                SINFO("Existing WAL2 db");
                hctree = false;
            } else {
                SWARN("Invalid db format");
            }
            fclose(fp);
        } else {
            SWARN("Failed to read existing file.");
        }
        SINFO("Opening database '" << filename << "'.");
    } else {
        SINFO("Creating database '" << filename << "'.");
    }

    return hctree;
}

sqlite3* SQLite::initializeDB(const string& filename, int64_t mmapSizeGB, bool hctree)
{
    sqlite3* db;
    string completeFilename = filename;
    if (hctree) {
        // Per the docs here: https://sqlite.org/hctree/doc/hctree/doc/hctree/index.html
        // We only need to specify the full URL when creating new DBs. Existing DBs will be auto-detected as HC-Tree or not.
        completeFilename = "file://" + completeFilename + "?hctree=1";
    }
    int result = sqlite3_open_v2(completeFilename.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_URI, NULL);
    if (result) {
        SERROR("sqlite3_open_v2 returned " << result << ", Extended error code: " << sqlite3_extended_errcode(db));
    }

    // Set busy_timeout immediately after opening the database so that all subsequent initialization queries
    // (e.g., initializeJournal reading sqlite_master) can wait for locks instead of failing with SQLITE_BUSY.
    // This is critical during startup when another process may hold WAL locks during recovery or checkpoint.
    // Without this, initializeJournal would crash with SASSERT if the database was locked.
    sqlite3_busy_timeout(db, 120'000);

    // PRAGMA legacy_file_format=OFF sets the default for creating new databases, so it must be called before creating
    // any tables to be effective.
    SASSERT(!SQuery(db, "PRAGMA legacy_file_format = OFF"));

    return db;
}

vector<string> SQLite::initializeJournal(sqlite3* db, int minJournalTables)
{
    // Make sure we don't try and create more journals than we can name.
    SASSERT(minJournalTables < 10'000);

    // First, we create all of the tables through `minJournalTables` if they don't exist.
    for (int currentJounalTable = -1; currentJounalTable <= minJournalTables; currentJounalTable++) {
        char tableName[27] = {0};
        if (currentJounalTable < 0) {
            // The `-1` entry is just plain "journal".
            snprintf(tableName, 27, "journal");
        } else {
            snprintf(tableName, 27, "journal%04i", currentJounalTable);
        }
        if (SQVerifyTable(db, tableName, "CREATE TABLE " + string(tableName) + " ( id INTEGER PRIMARY KEY, query TEXT, hash TEXT )")) {
            SHMMM("Created " << tableName << " table.");
        }
    }

    // And we'll figure out which journal tables actually exist, which may be more than we require. They must be
    // sequential.
    int currentJounalTable = -1;
    vector<string> journalNames;
    while (true) {
        char tableName[27] = {0};
        if (currentJounalTable < 0) {
            // The `-1` entry is just plain "journal".
            snprintf(tableName, 27, "journal");
        } else {
            snprintf(tableName, 27, "journal%04i", currentJounalTable);
        }

        if (SQVerifyTableExists(db, tableName)) {
            journalNames.push_back(tableName);
            currentJounalTable++;
        } else {
            break;
        }
    }
    return journalNames;
}

void SQLite::commonConstructorInitialization(bool hctree)
{
    // Perform sanity checks.
    SASSERT(!_filename.empty());
    SASSERT(_maxJournalSize > 0);

    // WAL is what allows simultaneous read/writing.
    if (!hctree) {
        SASSERT(!SQuery(_db, "PRAGMA journal_mode = wal2;"));
    }

    if (_mmapSizeGB) {
        SASSERT(!SQuery(_db, "PRAGMA mmap_size=" + to_string(_mmapSizeGB * 1024 * 1024 * 1024) + ";"));
    }

    // Enable tracing for performance analysis.
    sqlite3_trace_v2(_db, SQLITE_TRACE_STMT, _sqliteTraceCallback, this);

    // Update the cache. -size means KB; +size means pages
    if (_cacheSize) {
        SINFO("Setting cache_size to " << _cacheSize << "KB");
        SQuery(_db, "PRAGMA cache_size = -" + SQ(_cacheSize) + ";");
    }

    // Register the authorizer callback which allows callers to whitelist particular data in the DB.
    sqlite3_set_authorizer(_db, _sqliteAuthorizerCallback, this);

    // Register application-defined deburr function.
    SDeburr::registerSQLite(_db);

    // We saw queries where the progress counter never exceeds 551,000, so we're setting it to a lower number
    // based on Richard Hipp's recommendation.
    sqlite3_progress_handler(_db, 100'000, _progressHandlerCallback, this);

    // Setting a wal hook prevents auto-checkpointing.
    sqlite3_wal_hook(_db, _walHookCallback, this);

    // For non-passive checkpoints, we must set a busy timeout in order to wait on any readers.
    // We set it to 2 minutes as the majority of transactions should take less than that.
    if (_checkpointMode != SQLITE_CHECKPOINT_PASSIVE) {
        sqlite3_busy_timeout(_db, 120'000);
    }
}

SQLite::SQLite(const string& filename, int cacheSize, int maxJournalSize,
               int minJournalTables, int64_t mmapSizeGB, bool hctree, const string& checkpointMode) :
    _filename(initializeFilename(filename)),
    _maxJournalSize(maxJournalSize),
    _hctree(validateDBFormat(_filename, hctree)),
    _db(initializeDB(_filename, mmapSizeGB, _hctree)),
    _journalNames(initializeJournal(_db, minJournalTables)),
    _sharedData(initializeSharedData(_db, _filename, _journalNames, _hctree)),
    _transactionTimer("transaction timer"),
    _cacheSize(cacheSize),
    _mmapSizeGB(mmapSizeGB),
    _checkpointMode(getCheckpointModeFromString(checkpointMode))
{
    commonConstructorInitialization(_hctree);
}

SQLite::SQLite(const SQLite& from) :
    _filename(from._filename),
    _maxJournalSize(from._maxJournalSize),
    _hctree(from._hctree),
    _db(initializeDB(_filename, from._mmapSizeGB, false)), // Create a *new* DB handle from the same filename, don't copy the existing handle.
    _journalNames(from._journalNames),
    _sharedData(from._sharedData),
    _transactionTimer("transaction timer"),
    _cacheSize(from._cacheSize),
    _mmapSizeGB(from._mmapSizeGB),
    _checkpointMode(from._checkpointMode)
{
    commonConstructorInitialization(_hctree);
}

int SQLite::_progressHandlerCallback(void* arg)
{
    SQLite* sqlite = static_cast<SQLite*>(arg);
    uint64_t now = STimeNow();
    if (sqlite->_timeoutLimit && now > sqlite->_timeoutLimit) {
        // Timeout! We don't throw here, we let `read` and `write` do it so we don't throw out of the middle of a
        // sqlite3 operation.
        sqlite->_timeoutError = now - sqlite->_timeoutStart;

        // Return non-zero causes sqlite to interrupt the operation.
        return 1;
    }

    // This has a known race condition, in that calling `setAbortRef` or `clearAbortRef` while this is running would potentially
    // cause unexpected behavior (including a segfault), but this is avoided by calling those functions in accordance with
    // thier documentation and *not using them* in the middle of a running query.
    if (sqlite->_shouldAbortPtr && sqlite->_shouldAbortPtr->load()) {
        return 1;
    }

    return 0;
}

void SQLite::setAbortRef(atomic<bool>& abortVar)
{
    _shouldAbortPtr = &abortVar;
}

void SQLite::clearAbortRef()
{
    _shouldAbortPtr = nullptr;
}

int SQLite::_walHookCallback(void* sqliteObject, sqlite3* db, const char* name, int walFileSize)
{
    SQLite* sqlite = static_cast<SQLite*>(sqliteObject);
    sqlite->_sharedData.outstandingFramesToCheckpoint = walFileSize;
    sqlite->_sharedData.knownOutstandingFramesToCheckpoint = walFileSize;
    return SQLITE_OK;
}

void SQLite::_sqliteLogCallback(void* pArg, int iErrCode, const char* zMsg)
{
    // Skip logging this as it generates a lot of noise and we don't use it.
    if (strstr(zMsg, "automatic index on")) {
        return;
    }

    _mostRecentSQLiteErrorLog = "{SQLITE} Code: "s + to_string(iErrCode) + ", Message: "s + zMsg;
    SRedactSensitiveValues(_mostRecentSQLiteErrorLog);
    SINFO(_mostRecentSQLiteErrorLog);

    // This is sort of hacky to parse this from the logging info. If it works we could ask sqlite for a better interface to get this info.
    if (SStartsWith(zMsg, "cannot commit")) {
        // 17 is the length of "conflict at page" and the following space.
        const char* pageOffset = strstr(zMsg, "conflict at page") + 17;
        _conflictPage = atol(pageOffset);

        // Sample conflict log lines:
        // {SQLITE} Code: 0, Message: cannot commit CONCURRENT transaction - conflict at page 1854553 (read/write page; part of db table reports; content=0D00000009007100...)
        // {SQLITE} Code: 0, Message: cannot commit CONCURRENT transaction - conflict at page 1594810 (read/write page; part of db index reportActions.reportActionsAccountIDCreatedComment; content=0A045B006A00EB00...)
        const string tableOrIndexName = SREReplace("^.*part of db (table|index) (.*?);.*$", zMsg, "$2");
        if (!tableOrIndexName.empty()) {
            _conflictLocation = tableOrIndexName;
        }
    }
}

int SQLite::_sqliteTraceCallback(unsigned int traceCode, void* c, void* p, void* x)
{
    if (enableTrace && traceCode == SQLITE_TRACE_STMT) {
        SINFO("NORMALIZED_SQL:" << sqlite3_normalized_sql((sqlite3_stmt*) p));
    }
    return 0;
}

string SQLite::_getJournalQuery(const list<string>& queryParts, bool append)
{
    return _getJournalQuery(_journalNames, queryParts, append);
}

string SQLite::_getJournalQuery(const vector<string>& journalNames, const list<string>& queryParts, bool append)
{
    list<string> queries;
    for (const string& name : journalNames) {
        queries.emplace_back(SComposeList(queryParts, " " + name + " ") + (append ? " " + name : ""));
    }
    string query = SComposeList(queries, " UNION ");
    return query;
}

SQLite::~SQLite()
{
    // First, rollback any incomplete transaction.
    if (!_uncommittedQuery.empty()) {
        SINFO("Rolling back in destructor.");
        rollback();
        SINFO("Rollback in destructor complete.");
    }

    // Finally, Close the DB.
    DBINFO("Closing database '" << _filename << ".");
    SASSERTWARN(_uncommittedQuery.empty());
    SASSERT(!sqlite3_close(_db));
    DBINFO("Database closed.");
}

void SQLite::exclusiveLockDB()
{
    // This order is important and not intuitive. It seems like we should lock `writeLock` before `commitLock` because that's the order these occur in a typical database operation,
    // however, there are two possible flows here. For a non-blocking transaction (i.e., one run in parallel with other transactions), the lock order is:
    // writeLock, commitLock, but importantly, in this case, these are not locked simultaneously. writeLock is only locked for the time of the actual DB write query, and then released.
    // So for a non-blocking transaction, this becomes unimportant, these are used singly.
    // However, for a blocking transaction (one run by the blocking commit thread) the commit lock is acquired at the BEGIN of the transaction, and critically - held for the duration
    // of the transaction. So in these instances, we lock commitLock first, and then writeLock, but the critical difference is we hold the commitLock through the entire duration of all
    // writes in this case.
    // So when these are both locked by the same thread at the same time, `commitLock` is always locked first, and we do it the same way here to avoid deadlocks.
    try {
        SINFO("Locking commitLock");
        _sharedData.commitLock.lock();
        SINFO("commitLock Locked");
    } catch (const system_error& e) {
        SWARN("Caught system_error calling _sharedData.commitLock, code: " << e.code() << ", message: " << e.what());
        throw;
    }
    try {
        SINFO("Locking writeLock");
        _sharedData.writeLock.lock();
        SINFO("writeLock Locked");
    } catch (const system_error& e) {
        SWARN("Caught system_error calling _sharedData.writeLock, code: " << e.code() << ", message: " << e.what());
        throw;
    }
}

void SQLite::exclusiveUnlockDB()
{
    _sharedData.writeLock.unlock();
    _sharedData.commitLock.unlock();
}

SQLite::TRANSACTION_TYPE SQLite::getLastTransactionType()
{
    return _lastTransactionType;
}

bool SQLite::beginTransaction(SQLite::TRANSACTION_TYPE type, bool beginOnly)
{
    _lastTransactionType = type;
    _transactionTimer.start("BEGIN_TRANSACTION");
    if (type == TRANSACTION_TYPE::EXCLUSIVE) {
        if (isSyncThread) {
            // Blocking the sync thread has catastrophic results (forking) and so we either get this quickly, or we fail the transaction.
            if (!_sharedData.commitLock.try_lock_for(5s)) {
                SWARN("Failed to acquire commit lock in sync thread exclusive transaction!");
                STHROW("512 Internal Lock Timeout");
            }
        } else {
            _sharedData.commitLock.lock();
        }
        _sharedData._commitLockTimer.start("EXCLUSIVE");
        _mutexLocked = true;
    }

    // The most likely case for hitting this is that we forgot to roll back a transaction when we were finished with it
    // during the last use of this DB handle. In that case, `_insideTransaction` is likely true, and possibly
    // _uncommittedHash or _uncommittedQuery is set. Rollback should put this DB handle back into a usable state,
    // but it breaks the current transaction on this handle. We throw and fail the one transaction and hopefully have
    // fixed the handle for the next use
    if (_insideTransaction) {
        rollback();
        STHROW("Attempted to begin transaction while in invalid state: already inside transaction");
    }
    if (!_uncommittedHash.empty()) {
        rollback();
        STHROW("Attempted to begin transaction while in invalid state: _uncommittedHash not empty");
    }
    if (!_uncommittedQuery.empty()) {
        rollback();
        STHROW("Attempted to begin transaction while in invalid state: _uncommittedQuery not empty");
    }

    // Reset before the query, as it's possible the query sets these.
    _autoRolledBack = false;

    // We actively track transaction counts incrementing and decrementing to log the number of active open transactions at any given moment.
    _sharedData.openTransactionCount++;

    if (_sharedData.openTransactionCount > 10) {
        SINFO("Beginning transaction - open transaction count: " << (_sharedData.openTransactionCount));
    }
    uint64_t before = STimeNow();

    string beginQuery = (beginOnly && _hctree) ? "BEGIN" : "BEGIN CONCURRENT";
    _insideTransaction = !SQuery(_db, beginQuery);

    // Because some other thread could commit once we've run `BEGIN CONCURRENT`, this value can be slightly behind
    // where we're actually able to start such that we know we shouldn't get a conflict if this commits successfully on
    // leader. However, this is perfectly safe, it just adds the possibility that threads on followers wait for an
    // extra transaction to complete before starting, which is an anti-optimization, but the alternative is wrapping
    // the above `BEGIN CONCURRENT` and the `getCommitCount` call in a lock, which is worse.
    _dbCountAtStart = getCommitCount();
    _queryCache.clear();
    _tablesUsed.clear();
    _readQueryCount = 0;
    _writeQueryCount = 0;
    _cacheHits = 0;
    _beginElapsed = STimeNow() - before;
    _readElapsed = 0;
    _writeElapsed = 0;
    _prepareElapsed = 0;
    _commitElapsed = 0;
    _commitLockElapsed = 0;
    _rollbackElapsed = 0;
    _lastConflictPage = 0;
    _lastConflictLocation = "";
    _totalTransactionElapsed = 0;
    return _insideTransaction;
}

bool SQLite::verifyTable(const string& tableName, const string& sql, bool& created, const string& type)
{
    // sqlite trims semicolon, so let's not supply it else we get confused later
    SASSERT(!SEndsWith(sql, ";"));

    // First, see if it's there
    SQResult result;
    SASSERT(read("SELECT sql FROM sqlite_master WHERE type=" + SQ(type) + " AND tbl_name=" + SQ(tableName) + ";", result));
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
            SHMMM("'" << tableName << "' has incorrect schema, need to upgrade? Is '" << collapsedResult << "' expected  '" << collapsedSQL << "'");
            return false;
        }
    }
}

bool SQLite::verifyIndex(const string& indexName, const string& tableName, const string& indexSQLDefinition, bool isUnique, bool createIfNotExists)
{
    SINFO("Verifying index", {{"indexName", indexName}, {"isUnique", to_string(isUnique)}});
    SQResult result;
    SASSERT(read("SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=" + SQ(tableName) + " AND name=" + SQ(indexName) + ";", result));

    string createSQL = "CREATE" + string(isUnique ? " UNIQUE " : " ") + "INDEX " + indexName + " ON " + tableName + " " + indexSQLDefinition;
    if (result.empty()) {
        if (!createIfNotExists) {
            SINFO("Index '" << indexName << "' does not exist on table '" << tableName << "'.");
            return false;
        }
        SINFO("Creating index '" << indexName << "' on table '" << tableName << "': " << indexSQLDefinition << ". Executing '" << createSQL << "'.");
        SASSERT(write(createSQL + ";"));
        return true;
    } else {
        // Index exists, verify it is correct. Ignore spaces.
        SASSERT(!result[0].empty());
        return SIEquals(SReplace(createSQL, " ", ""), SReplace(result[0][0], " ", ""));
    }
}

bool SQLite::addColumn(const string& tableName, const string& column, const string& columnType)
{
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

string SQLite::read(const string& query) const
{
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

int SQLite::read(const string& query, sqlite3_qrf_spec* spec) const
{
    // Execute the read-only query. Skips caching.
    uint64_t before = STimeNow();
    int queryResult = SQuery(_db, query, spec);
    _checkInterruptErrors("SQLite::read"s);
    _readElapsed += STimeNow() - before;
    return queryResult;
}

bool SQLite::read(const string& query, SQResult& result, bool skipInfoWarn) const
{
    uint64_t before = STimeNow();
    bool queryResult = false;
    _readQueryCount++;
    auto foundQuery = _queryCache.find(query);
    if (foundQuery != _queryCache.end()) {
        result = foundQuery->second;
        _cacheHits++;
        queryResult = true;
    } else {
        _isDeterministicQuery = true;
        queryResult = !SQuery(_db, query, result, 2000 * STIME_US_PER_MS, skipInfoWarn);
        if (_isDeterministicQuery && queryResult && insideTransaction()) {
            _queryCache.emplace(make_pair(query, result));
        }
    }
    _checkInterruptErrors("SQLite::read"s);
    _readElapsed += STimeNow() - before;
    return queryResult;
}

void SQLite::_checkInterruptErrors(const string& error) const
{
    // Local error code.
    int errorCode = 0;
    uint64_t time = 0;

    // Check timeout.
    if (_timeoutLimit) {
        uint64_t now = STimeNow();
        if (now > _timeoutLimit) {
            _timeoutError = now - _timeoutStart;
        }
        if (_timeoutError) {
            time = _timeoutError;
            errorCode = 1;
        }
    }

    // If we had an interrupt error, and were inside a transaction, and autocommit is now on, we have been auto-rolled
    // back, we won't need to actually do a rollback for this transaction.
    if (errorCode && _insideTransaction && sqlite3_get_autocommit(_db)) {
        SHMMM("Transaction automatically rolled back. Setting _autoRolledBack = true");
        _autoRolledBack = true;
    }

    if (errorCode == 1) {
        throw timeout_error("timeout in "s + error, time);
    }

    // Otherwise, no error.
}

bool SQLite::write(const string& query)
{
    if (_noopUpdateMode) {
        SALERT("Non-idempotent write in _noopUpdateMode. Query: " << query);
        return true;
    }

    // This is literally identical to the idempotent version except for the check for _noopUpdateMode.
    SQResult ignore;
    return _writeIdempotent(query, ignore);
}

bool SQLite::write(const string& query, SQResult& result)
{
    if (_noopUpdateMode) {
        SALERT("Non-idempotent write in _noopUpdateMode. Query: " << query);
        return true;
    }

    // This is literally identical to the idempotent version except for the check for _noopUpdateMode.
    return _writeIdempotent(query, result);
}

bool SQLite::writeIdempotent(const string& query)
{
    SQResult ignore;
    return _writeIdempotent(query, ignore);
}

bool SQLite::writeIdempotent(const string& query, SQResult& result)
{
    return _writeIdempotent(query, result);
}

bool SQLite::writeUnmodified(const string& query)
{
    SQResult ignore;
    return _writeIdempotent(query, ignore, true);
}

bool SQLite::_writeIdempotent(const string& query, SQResult& result, bool alwaysKeepQueries)
{
    if (!_insideTransaction) {
        STHROW("500 Attempted to write outside of transaction");
    }

    _queryCache.clear();
    _writeQueryCount++;

    // Must finish everything with semicolon.
    SASSERT(query.empty() || SEndsWith(query, ";"));

    // First, check our current state
    SQResult results;
    SASSERT(!SQuery(_db, "PRAGMA schema_version;", results));
    SASSERT(!results.empty() && !results[0].empty());
    uint64_t schemaBefore = SToUInt64(results[0][0]);
    uint64_t changesBefore = sqlite3_total_changes(_db);

    _currentlyWriting = true;
    // Try to execute the query
    uint64_t before = STimeNow();
    bool usedRewrittenQuery = false;
    int resultCode = 0;
    {
        shared_lock<shared_mutex> lock(_sharedData.writeLock);
        if (_enableRewrite) {
            resultCode = SQuery(_db, query, result, 2'000'000, true);
            if (resultCode == SQLITE_AUTH) {
                // Run re-written query.
                _currentlyRunningRewritten = true;
                SASSERT(SEndsWith(_rewrittenQuery, ";"));
                resultCode = SQuery(_db, _rewrittenQuery);
                usedRewrittenQuery = true;
                _currentlyRunningRewritten = false;
            }
        } else {
            resultCode = SQuery(_db, query, result);
        }
    }

    // If we got a constraints error, throw that.
    if (resultCode == SQLITE_CONSTRAINT) {
        _currentlyWriting = false;
        throw constraint_error();
    }

    _checkInterruptErrors("SQLite::write"s);
    _writeElapsed += STimeNow() - before;
    if (resultCode) {
        _currentlyWriting = false;
        return false;
    }

    // See if the query changed anything
    SASSERT(!SQuery(_db, "PRAGMA schema_version;", results));
    SASSERT(!results.empty() && !results[0].empty());
    uint64_t schemaAfter = SToUInt64(results[0][0]);
    uint64_t changesAfter = sqlite3_total_changes(_db);

    // If something changed, or we're always keeping queries, then save this.
    if (alwaysKeepQueries || (schemaAfter > schemaBefore) || (changesAfter > changesBefore)) {
        _uncommittedQuery += usedRewrittenQuery ? _rewrittenQuery : query;
    }

    _currentlyWriting = false;
    return true;
}

bool SQLite::prepare(uint64_t* transactionID, string* transactionhash)
{
    SASSERT(_insideTransaction);

    // Pick a journal for this transaction.
    const int64_t journalID = _sharedData.nextJournalCount++;
    _journalName = _journalNames[journalID % _journalNames.size()];

    // Look up the oldest commit in our chosen journal, and compute the oldest commit we intend to keep.
    SQResult journalLookupResult;
    SASSERT(!SQuery(_db, "SELECT MIN(id) FROM " + _journalName, journalLookupResult));
    uint64_t minJournalEntry = journalLookupResult.size() ? SToUInt64(journalLookupResult[0][0]) : 0;

    // Note that this can change before we hold the lock on _sharedData.commitLock, but it doesn't matter yet, as we're only
    // using it to truncate the journal. We'll reset this value once we acquire that lock.
    uint64_t commitCount = _sharedData.commitCount;

    // If the commitCount is less than the max journal size, keep everything. Otherwise, keep everything from
    // commitCount - _maxJournalSize forward. We can't just do the last subtraction part because it overflows our unsigned
    // int.
    uint64_t oldestCommitToKeep = commitCount < _maxJournalSize ? 0 : commitCount - _maxJournalSize;

    // We limit deletions to a relatively small number to avoid making this extremely slow for some transactions in the case
    // where this journal in particular has accumulated a large backlog.
    static const size_t deleteLimit = 10;
    if (minJournalEntry < oldestCommitToKeep) {
        shared_lock<shared_mutex> lock(_sharedData.writeLock);
        string query = "DELETE FROM " + _journalName + " WHERE id < " + SQ(oldestCommitToKeep) + " LIMIT " + SQ(deleteLimit);
        SASSERT(!SQuery(_db, query));
    }

    // We lock this here, so that we can guarantee the order in which commits show up in the database.
    if (!_mutexLocked) {
        auto start = STimeNow();
        _sharedData.commitLock.lock();
        auto end = STimeNow();
        if (end - start > 5'000) {
            SINFO("Waited " << (end - start) << "us for commit lock.");
        }
        _sharedData._commitLockTimer.start("SHARED");
        _mutexLocked = true;
    }

    // We pass the journal number selected to the handler so that a caller can utilize the
    // same method bedrock does for accessing 1 table per thread, in order to attempt to
    // reduce conflicts on tables that are written to on every command
    if (_shouldNotifyPluginsOnPrepare) {
        (*_onPrepareHandler)(*this, journalID);
    }

    // Now that we've locked anybody else from committing, look up the state of the database. We don't need to lock the
    // SharedData object to get these values as we know it can't currently change.
    commitCount = _sharedData.commitCount;

    // Queue up the journal entry
    string lastCommittedHash = getCommittedHash(); // This is why we need the lock.
    _uncommittedHash = SToHex(SHashSHA1(lastCommittedHash + _uncommittedQuery));
    uint64_t before = STimeNow();

    // Update the passed-in reference values
    if (transactionID) {
        *transactionID = commitCount + 1;
    }
    if (transactionhash) {
        *transactionhash = _uncommittedHash;
    }

    // Create our query.
    string query = "INSERT INTO " + _journalName + " VALUES (" + SQ(commitCount + 1) + ", " + SQ(_uncommittedQuery) + ", " + SQ(_uncommittedHash) + " )";

    // These are the values we're currently operating on, until we either commit or rollback.
    _sharedData.prepareTransactionInfo(commitCount + 1, _uncommittedQuery, _uncommittedHash, _dbCountAtStart);
    if (_uncommittedQuery.empty()) {
        SINFO("Will commmit blank query");
    }

    int result = SQuery(_db, query);
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

int SQLite::commit(const string& description, const string& commandName, function<void()>* preCheckpointCallback)
{
    // If commits have been disabled, return an error without attempting the commit.
    if (!_sharedData._commitEnabled) {
        return COMMIT_DISABLED;
    }

    SASSERT(_insideTransaction);
    SASSERT(!_uncommittedHash.empty()); // Must prepare first
    int result = 0;

    // Make sure one is ready to commit
    SDEBUG("Committing transaction");

    // Record DB pages before commit to see how many the commit touches.
    int startPages, dummy;
    sqlite3_db_status(_db, SQLITE_DBSTATUS_CACHE_WRITE, &startPages, &dummy, 0);

    _conflictPage = 0;
    _conflictLocation = "";
    uint64_t before = STimeNow();
    uint64_t beforeCommit = STimeNow();
    if (_hctree) {
        SQResult pageCountResult;
        SQuery(_db, "PRAGMA page_count;", pageCountResult);
        if (!pageCountResult.empty()) {
            _pageCountDifference = SToInt64(pageCountResult[0][0]);
        }
    }
    result = SQuery(_db, "COMMIT");
    if (_hctree) {
        SQResult pageCountResult;
        SQuery(_db, "PRAGMA page_count;", pageCountResult);
        if (!pageCountResult.empty()) {
            _pageCountDifference = SToUInt64(pageCountResult[0][0]) - _pageCountDifference;
        }
    }

    // In HCTree mode, we log extra info for slow commits.

    /* This is disabled because the diagnostic logging is unreasonably expensive (30+ seconds for the query).
     * if (_hctree) {
     *  uint64_t afterCommit = STimeNow();
     *  // log for any commit over 1 second.
     *  if ((afterCommit - beforeCommit) > 1'000'000) {
     *      SQResult slowCommitResult;
     *      SQuery(_db, "SELECT * FROM hctvalid", slowCommitResult);
     *      SINFO("SLOW HCTREE COMMIT " << (slowCommitResult.size() + 1) << " lines to follow");
     *      string headers = SComposeList(slowCommitResult.headers);
     *      SINFO("SLOW HCTREE COMMIT HEADERS: " << headers);
     *      for (size_t i = 0; i < slowCommitResult.size(); i++) {
     *          SINFO("SLOW HCTREE COMMIT ROW: " << i << ": " << SComposeList(slowCommitResult[i]));
     *      }
     *  }
     * }
     */

    _lastConflictPage = _conflictPage;
    _lastConflictLocation = _conflictLocation;

    // If there were conflicting commits, will return SQLITE_BUSY_SNAPSHOT
    SASSERT(result == SQLITE_OK || result == SQLITE_BUSY_SNAPSHOT);
    if (result == SQLITE_OK) {
        char time[16];
        snprintf(time, 16, "%.2fms", (double) (STimeNow() - beforeCommit) / 1000.0);

        // And record pages after the commit.
        int endPages;
        sqlite3_db_status(_db, SQLITE_DBSTATUS_CACHE_WRITE, &endPages, &dummy, 0);

        // Similarly, record WAL file size.
        sqlite3_file* pWal = 0;
        sqlite3_int64 sz = 0;
        sqlite3_file_control(_db, "main", SQLITE_FCNTL_JOURNAL_POINTER, &pWal);
        if (pWal && pWal->pMethods) {
            // This is not set for HC-tree DBs.
            pWal->pMethods->xFileSize(pWal, &sz);
        }

        _commitElapsed += STimeNow() - before;
        _commitLockElapsed += _sharedData._commitLockTimer.stop();
        _totalTransactionElapsed = _transactionTimer.stop();
        _sharedData.incrementCommit(_uncommittedHash);
        _insideTransaction = false;
        _uncommittedHash.clear();
        _uncommittedQuery.clear();
        _sharedData.commitLock.unlock();
        _mutexLocked = false;
        _queryCache.clear();

        _sharedData.openTransactionCount--;

        if (preCheckpointCallback != nullptr) {
            (*preCheckpointCallback)();
        }

        // If we are the first to set it (i.e., test_and_set returned `false` as the previous value), we'll start a checkpoint.
        if (!_sharedData.checkpointInProgress.test_and_set()) {
            if (_sharedData.outstandingFramesToCheckpoint) {
                auto start = STimeNow();
                int framesCheckpointed = 0;
                sqlite3_wal_checkpoint_v2(_db, 0, _checkpointMode, NULL, &framesCheckpointed);
                auto end = STimeNow();
                SINFO("Checkpoint with type=" << _checkpointMode << " complete with " << framesCheckpointed << " frames checkpointed of " << _sharedData.outstandingFramesToCheckpoint << " frames outstanding in " << (end - start) << "us.");

                // It might not actually be 0, but we'll just let sqlite tell us what it is next time _walHookCallback runs.
                _sharedData.outstandingFramesToCheckpoint = 0;
            }
            _sharedData.checkpointInProgress.clear();
        }
        logLastTransactionTiming(
            format("{} COMMIT {} complete in {}. Wrote {} pages. WAL file size is {} bytes. {} read queries attempted, {} write queries attempted, {} served from cache. Used journal {}.{}",
                description,
                SToStr(_sharedData.commitCount),
                time,
                   (endPages - startPages),
                sz,
                _readQueryCount,
                _writeQueryCount,
                _cacheHits,
                _journalName,
                   (_hctree ? format(" HC-Tree pages added: {}", _pageCountDifference) : "")),
            commandName);
        _readQueryCount = 0;
        _writeQueryCount = 0;
        _cacheHits = 0;
        _dbCountAtStart = 0;
        _lastConflictPage = 0;
        _lastConflictLocation = "";
    } else {
        // The commit failed, we will rollback.
    }

    // if we got SQLITE_BUSY_SNAPSHOT, then we're *still* holding commitLock, and it will need to be unlocked by
    // calling rollback().
    return result;
}

int SQLite::getCheckpointModeFromString(const string& checkpointModeString)
{
    if (checkpointModeString == "PASSIVE") {
        return SQLITE_CHECKPOINT_PASSIVE;
    }
    if (checkpointModeString == "FULL") {
        return SQLITE_CHECKPOINT_FULL;
    }
    if (checkpointModeString == "RESTART") {
        return SQLITE_CHECKPOINT_RESTART;
    }
    if (checkpointModeString == "TRUNCATE") {
        return SQLITE_CHECKPOINT_TRUNCATE;
    }
    SERROR("Invalid checkpoint type: " << checkpointModeString);
}

map<uint64_t, tuple<string, string, uint64_t>> SQLite::popCommittedTransactions()
{
    return _sharedData.popCommittedTransactions();
}

void SQLite::rollback(const string& commandName)
{
    // Make sure we're actually inside a transaction
    if (_insideTransaction) {
        // Store the total transaction time only if we were inside one.
        _totalTransactionElapsed = _transactionTimer.stop();

        // Cancel this transaction
        if (_autoRolledBack) {
            SINFO("Transaction was automatically rolled back, not sending 'ROLLBACK'.");
            _autoRolledBack = false;
        } else {
            if (_uncommittedQuery.size()) {
                SINFO("Rolling back transaction: " << _uncommittedQuery.substr(0, 100));
            }
            uint64_t before = STimeNow();
            SASSERT(!SQuery(_db, "ROLLBACK"));
            _rollbackElapsed += STimeNow() - before;
        }

        _sharedData.openTransactionCount--;

        // Finally done with this.
        _insideTransaction = false;
        _uncommittedHash.clear();
        _uncommittedQuery.clear();

        // Only unlock the mutex if we've previously locked it. We can call `rollback` to cancel a transaction without
        // ever having called `prepare`, which would have locked our mutex.
        if (_mutexLocked) {
            _mutexLocked = false;
            _commitLockElapsed += _sharedData._commitLockTimer.stop();
            _sharedData.commitLock.unlock();
        }
    } else {
        // Stop the timer without storing the time spent since transaction as already rolled back.
        _transactionTimer.stop();
        _totalTransactionElapsed = 0;
        SINFO("Rolling back but not inside transaction, ignoring.");
    }
    _queryCache.clear();
    logLastTransactionTiming(
        format("Transaction rollback with {} read queries attempted, {} write queries attempted, {} served from cache.", _readQueryCount, _writeQueryCount, _cacheHits),
        commandName
    );
    _readQueryCount = 0;
    _writeQueryCount = 0;
    _cacheHits = 0;
    _dbCountAtStart = 0;
}

void SQLite::logLastTransactionTiming(const string& message, const string& commandName)
{
    // We don't want to add `commitLockElapsed` and `totalTransactionElapsed` to the total elapsed time since they overlap with parts of the transaction
    // and that could double-count certain times (i.e. `commitElapsed` occurs simultaneously with `commitLockElapsed`)
    uint64_t totalElapsed = _beginElapsed + _readElapsed + _writeElapsed + _prepareElapsed + _commitElapsed + _rollbackElapsed;
    SINFO(message, {
        {"command", commandName},
        {"totalElapsed", to_string(totalElapsed / 1000)},
        {"readElapsed", to_string(_readElapsed / 1000)},
        {"writeElapsed", to_string(_writeElapsed / 1000)},
        {"prepareElapsed", to_string(_prepareElapsed / 1000)},
        {"commitElapsed", to_string(_commitElapsed / 1000)},
        {"rollbackElapsed", to_string(_rollbackElapsed / 1000)},
        {"totalTransactionElapsed", to_string(_totalTransactionElapsed / 1000)},
        {"beginElapsed", to_string(_beginElapsed / 1000)},
        {"commitLockElapsed", to_string(_commitLockElapsed / 1000)},
    });
}

bool SQLite::getCommit(uint64_t id, string& query, string& hash)
{
    return getCommit(_db, _journalNames, id, query, hash);
}

bool SQLite::getCommit(sqlite3* db, const vector<string>& journalNames, uint64_t id, string& query, string& hash)
{
    // TODO: This can fail if called after `BEGIN TRANSACTION`, if the id we want to look up was committed by another
    // thread. We may or may never need to handle this case.
    // Look up the query and hash for the given commit
    string internalQuery = _getJournalQuery(journalNames, {"SELECT query, hash FROM", "WHERE id = " + SQ(id)});
    SQResult result;
    SASSERT(!SQuery(db, internalQuery, result));
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

    // If we found a hash, we assume this was a good commit, as we'll allow an empty commit.
    return !hash.empty();
}

string SQLite::getCommittedHash()
{
    return _sharedData.lastCommittedHash.load();
}

int SQLite::getCommits(uint64_t fromIndex, uint64_t toIndex, SQResult& result, uint64_t timeoutLimitUS)
{
    // Look up all the queries within that range
    SASSERTWARN(SWITHIN(1, fromIndex, toIndex));
    string query = _getJournalQuery({"SELECT id, hash, query FROM", "WHERE id >= " + SQ(fromIndex) +
                                     (toIndex ? " AND id <= " + SQ(toIndex) : "")});
    SDEBUG("Getting commits #" << fromIndex << "-" << toIndex);
    query = "SELECT hash, query FROM (" + query + ") ORDER BY id";
    if (timeoutLimitUS) {
        setTimeout(timeoutLimitUS);
    }
    int queryResult = SQuery(_db, query, result);
    clearTimeout();
    return queryResult;
}

int64_t SQLite::getLastInsertRowID()
{
    // Make sure it *does* happen after an INSERT, but not with a IGNORE
    SASSERTWARN(SContains(_uncommittedQuery, "INSERT") || SContains(_uncommittedQuery, "REPLACE"));
    SASSERTWARN(!SContains(_uncommittedQuery, "IGNORE"));
    int64_t sqliteRowID = (int64_t) sqlite3_last_insert_rowid(_db);
    return sqliteRowID;
}

uint64_t SQLite::getCommitCount() const
{
    return _sharedData.commitCount;
}

uint64_t SQLite::getOutstandingFramesToCheckpoint() const
{
    return _sharedData.knownOutstandingFramesToCheckpoint;
}

size_t SQLite::getLastWriteChangeCount()
{
    int count = sqlite3_changes(_db);
    return count > 0 ? (size_t) count : 0;
}

void SQLite::enableRewrite(bool enable)
{
    _enableRewrite = enable;
}

void SQLite::setRewriteHandler(bool (*handler)(int, const char*, string&))
{
    _rewriteHandler = handler;
}

void SQLite::enablePrepareNotifications(bool enable)
{
    _shouldNotifyPluginsOnPrepare = enable;
}

void SQLite::setOnPrepareHandler(void (*handler)(SQLite& _db, int64_t tableID))
{
    _onPrepareHandler = handler;
}

int SQLite::_sqliteAuthorizerCallback(void* pUserData, int actionCode, const char* detail1, const char* detail2,
                                      const char* detail3, const char* detail4)
{
    SQLite* db = static_cast<SQLite*>(pUserData);
    return db->_authorize(actionCode, detail1, detail2, detail3, detail4);
}

int SQLite::_authorize(int actionCode, const char* detail1, const char* detail2, const char* detail3, const char* detail4)
{
    // If we've enabled re-writing, see if we need to re-write this query.
    if (_enableRewrite && !_currentlyRunningRewritten && (*_rewriteHandler)(actionCode, detail1, _rewrittenQuery)) {
        // Deny the original query, we'll re-run on the re-written version.
        return SQLITE_DENY;
    }

    // Record all tables touched.
    if (set<int>{SQLITE_INSERT, SQLITE_DELETE, SQLITE_READ, SQLITE_UPDATE}.count(actionCode)) {
        _tablesUsed.insert(detail1);
    }

    // Here's where we can check for non-deterministic functions for the cache.
    if (actionCode == SQLITE_FUNCTION && detail2) {
        if (!strcmp(detail2, "random") ||
            !strcmp(detail2, "date") ||
            !strcmp(detail2, "time") ||
            !strcmp(detail2, "datetime") ||
            !strcmp(detail2, "julianday") ||
            !strcmp(detail2, "strftime") ||
            !strcmp(detail2, "changes") ||
            !strcmp(detail2, "last_insert_rowid") ||
            !strcmp(detail2, "sqlite_version")
        ) {
            _isDeterministicQuery = false;
        }

        // Prevent using certain non-deterministic functions in writes which could cause synchronization with followers to
        // result in inconsistent data. Some are not included here because they can be used in a deterministic way that is valid.
        // i.e. you can do UPDATE x = DATE('2024-01-01') and its deterministic whereas UPDATE x = DATE('now') is not. It's up to
        // callers to prevent using these functions inappropriately.
        if (!strcmp(detail2, "current_timestamp") ||
            !strcmp(detail2, "random") ||
            !strcmp(detail2, "last_insert_rowid") ||
            !strcmp(detail2, "changes") ||
            !strcmp(detail2, "sqlite_version")) {
            if (_currentlyWriting) {
                return SQLITE_DENY;
            }
        }
    }

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

        case SQLITE_PRAGMA:
        {
            string normalizedTable = SToLower(detail1);
            // We allow this particularly because we call it ourselves in `write`, and so if it's not allowed, all
            // write queries will always fail. We specifically check that `column` is empty, because if it's set, that
            // means the caller has tried to specify a schema version, which we disallow, as it can cause DB
            // corruption. Note that this still allows `PRAGMA schema_version = 1;` to crash the process. This needs to
            // get caught sooner.
            if (normalizedTable == "schema_version" && detail2 == 0) {
                return SQLITE_OK;
            } else {
                return SQLITE_DENY;
            }
            break;
        }

        case SQLITE_READ:
        {
            // See if there's an entry in the whitelist for this table.
            auto tableIt = whitelist->find(detail1);
            if (tableIt != whitelist->end()) {
                // If so, see if there's an entry for this column.
                auto columnIt = tableIt->second.find(detail2);
                if (columnIt != tableIt->second.end()) {
                    // If so, then this column is whitelisted.
                    return SQLITE_OK;
                }
            }

            // If we didn't find it, not whitelisted.
            SWARN("[security] Non-whitelisted column: " << detail2 << " in table " << detail1 << ".");
            return SQLITE_IGNORE;
        }
    }
    return SQLITE_DENY;
}

void SQLite::setTimeout(uint64_t timeLimitUS)
{
    _timeoutStart = STimeNow();
    _timeoutLimit = _timeoutStart + timeLimitUS;
    _timeoutError = 0;
}

void SQLite::clearTimeout()
{
    _timeoutLimit = 0;
    _timeoutStart = 0;
    _timeoutError = 0;
}

void SQLite::setUpdateNoopMode(bool enabled)
{
    if (_noopUpdateMode == enabled) {
        return;
    }

    // Enable or disable this query.
    string query = "PRAGMA noop_update="s + (enabled ? "ON" : "OFF") + ";";
    SQuery(_db, query);
    _noopUpdateMode = enabled;

    // If we're inside a transaction, make sure this gets saved so it can be replicated.
    // If we're not (i.e., a transaction's already been rolled back), no need, there's nothing to replicate.
    if (_insideTransaction) {
        _uncommittedQuery += query;
    }
}

bool SQLite::getUpdateNoopMode() const
{
    return _noopUpdateMode;
}

uint64_t SQLite::getDBCountAtStart() const
{
    return _dbCountAtStart;
}

void SQLite::setCommitEnabled(bool enable)
{
    _sharedData.setCommitEnabled(enable);
}

int SQLite::getPreparedStatements(const string& query, list<sqlite3_stmt*>& statements)
{
    // We need a pointer to a prepared statement.
    sqlite3_stmt* ppStmt = nullptr;

    // And a pointer to the remainder of the query string (which is relevant if we're passed a string with multiple
    // statements). It starts at the beginning of the query.
    const char* pzTail = query.c_str();

    // Now loop as long as pzTail doesn't point at a null terminator.
    while (*pzTail != 0) {
        // Run prepare on the query.
        int result = sqlite3_prepare_v3(_db, pzTail, -1, 0, &ppStmt, &pzTail);

        // If it generated a statement, add it to our list.
        if (ppStmt != nullptr) {
            statements.push_back(ppStmt);
        }

        // If it was an errror, return early. We only generate more statements if the preceding ones succeed.
        if (result != SQLITE_OK) {
            return result;
        }
    }

    // If we made it through the whole thing, we're done.
    return SQLITE_OK;
}

void SQLite::setQueryOnly(bool enabled)
{
    SQResult result;
    string query = "PRAGMA query_only = "s + (enabled ? "true" : "false") + ";";
    SQuery(_db, query, result);
}

int64_t SQLite::getLastConflictPage() const
{
    return _lastConflictPage;
}

string SQLite::getLastConflictLocation() const
{
    return _lastConflictLocation;
}

SQLite::SharedData::SharedData() :
    nextJournalCount(0),
    _commitEnabled(true),
    openTransactionCount(0),
    _commitLockTimer("commit lock timer", {
    {"EXCLUSIVE", chrono::steady_clock::duration::zero()},
    {"SHARED", chrono::steady_clock::duration::zero()},
})
{
}

void SQLite::SharedData::setCommitEnabled(bool enable)
{
    if (_commitEnabled == enable) {
        // Exit early without grabbing the lock. It's possible during highly congested times for getting the lock to take long enough to time out the cluster.
        return;
    }

    lock_guard<decltype(commitLock)> lock(commitLock);
    _commitEnabled = enable;
}

void SQLite::SharedData::incrementCommit(const string& commitHash)
{
    lock_guard<decltype(_internalStateMutex)> lock(_internalStateMutex);
    commitCount++;
    commitTransactionInfo(commitCount);
    lastCommittedHash.store(commitHash);
}

void SQLite::SharedData::prepareTransactionInfo(uint64_t commitID, const string& query, const string& hash, uint64_t dbCountAtTransactionStart)
{
    lock_guard<decltype(_internalStateMutex)> lock(_internalStateMutex);
    _preparedTransactions.insert_or_assign(commitID, make_tuple(query, hash, dbCountAtTransactionStart));
}

void SQLite::SharedData::commitTransactionInfo(uint64_t commitID)
{
    lock_guard<decltype(_internalStateMutex)> lock(_internalStateMutex);
    _committedTransactions.insert(_preparedTransactions.extract(commitID));
}

map<uint64_t, tuple<string, string, uint64_t>> SQLite::SharedData::popCommittedTransactions()
{
    lock_guard<decltype(_internalStateMutex)> lock(_internalStateMutex);
    decltype(_committedTransactions) result;
    result = move(_committedTransactions);
    _committedTransactions.clear();
    return result;
}
