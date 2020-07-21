#include <libstuff/libstuff.h>
#include "SQLite.h"

#define DBINFO(_MSG_) SINFO("{" << _filename << "} " << _MSG_)

// Globally shared mutex for locking around commits and creating/destroying instances.
recursive_mutex SQLite::_commitLock;

atomic<int64_t> SQLite::_transactionAttemptCount(0);
mutex SQLite::_pageLogMutex;

// Global map for looking up shared data by file when creating new instances.
map<string, SQLite::SharedData*> SQLite::_sharedDataLookupMap;

// This is our only public static variable. It needs to be initialized after `_commitLock`.
// TODO: This can be removed and just use _commitLock directly if we don't need SLockTimer back.
recursive_mutex& SQLite::g_commitLock(SQLite::_commitLock);

atomic<int> SQLite::passiveCheckpointPageMin(2500); // Approx 10mb
atomic<int> SQLite::fullCheckpointPageMin(25000); // Approx 100mb (pages are assumed to be 4kb)

// Tracing can only be enabled or disabled globally, not per object.
atomic<bool> SQLite::enableTrace(false);

SQLite::SQLite(const string& filename, int cacheSize, bool enableFullCheckpoints, int maxJournalSize,
               int createJournalTables, const string& synchronous, int64_t mmapSizeGB, bool pageLoggingEnabled) :
    whitelist(nullptr),
    _maxJournalSize(maxJournalSize),
    _insideTransaction(false),
    _beginElapsed(0),
    _readElapsed(0),
    _writeElapsed(0),
    _prepareElapsed(0),
    _commitElapsed(0),
    _rollbackElapsed(0),
    _mutexLocked(false),
    _enableRewrite(false),
    _currentlyRunningRewritten(false),
    _timeoutLimit(0),
    _abandonForCheckpoint(false),
    _autoRolledBack(false),
    _noopUpdateMode(false),
    _enableFullCheckpoints(enableFullCheckpoints),
    _queryCount(0),
    _cacheHits(0),
    _useCache(false),
    _isDeterministicQuery(false),
    _pageLoggingEnabled(pageLoggingEnabled),
    _currentTransactionAttemptCount(-1),
    _cacheSize(cacheSize),
    _synchronous(synchronous),
    _mmapSizeGB(mmapSizeGB)
{
    // Perform sanity checks.
    SASSERT(!filename.empty());
    SASSERT(_cacheSize > 0);
    SASSERT(maxJournalSize > 0);

    // Canonicalize our filename and save that version.
    if (filename == ":memory:") {
        // This path is special, it exists in memory. This doesn't actually work correctly with journaling and such, as
        // we'll act as if they're all referencing the same file when we're not. This should therefore only be used
        // with a single SQLite object.
        _filename = filename;
    } else {
        char resolvedPath[PATH_MAX];
        char* result = realpath(filename.c_str(), resolvedPath);
        if (!result) {
            SERROR("Couldn't resolve pathname for: " << filename);
        }
        _filename = resolvedPath;
    }
    SINFO("Opening sqlite database: " << _filename);

    // We lock here to initialize the database. Because there's a global map of currently opened DB files, we lock
    // whenever we might need to insert a new one. These are only ever added or changed in the constructor and
    // destructor.
    SQLITE_COMMIT_AUTOLOCK;

    // sqlite3_config can't run concurrently with *anything* else, so we make sure it's set not only on creating
    // an entry, but on creating the *first* entry.
    if(_sharedDataLookupMap.empty()) {
        // Set the logging callback for sqlite errors.
        sqlite3_config(SQLITE_CONFIG_LOG, _sqliteLogCallback, 0);

        // Enable memory-mapped files.
        if (mmapSizeGB) {
            SINFO("Enabling Memory-Mapped I/O with " << mmapSizeGB << " GB.");
            const int64_t GB = 1024 * 1024 * 1024;
            sqlite3_config(SQLITE_CONFIG_MMAP_SIZE, mmapSizeGB * GB, 16 * 1024 * GB); // Max is 16TB
        }

        // Disable a mutex around `malloc`, which is *EXTREMELY IMPORTANT* for multi-threaded performance. Without this
        // setting, all reads are essentially single-threaded as they'll all fight with each other for this mutex.
        sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);
        sqlite3_initialize();
        SASSERT(sqlite3_threadsafe());

        // Disabled by default, but lets really beat it in. This way checkpointing does not need to wait on locks
        // created in this thread.
        SASSERT(sqlite3_enable_shared_cache(0) == SQLITE_OK);
    }

    // We're the initializer if we're the first one to add this entry to the map.
    auto sharedDataIterator = _sharedDataLookupMap.find(_filename);
    bool initializer = sharedDataIterator == _sharedDataLookupMap.end();
    if (initializer) {
        // Insert our SharedData object into the global map.
        _sharedData = new SharedData();
        _sharedDataLookupMap.emplace(_filename, _sharedData);
    } else {
        // Otherwise, use the existing one.
        _sharedData = sharedDataIterator->second;
    }

    // Open the DB in read-write mode.
    DBINFO((SFileExists(_filename) ? "Opening" : "Creating") << " database '" << _filename << "'.");
    const int DB_WRITE_OPEN_FLAGS = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX;
    SASSERT(!sqlite3_open_v2(filename.c_str(), &_db, DB_WRITE_OPEN_FLAGS, NULL));

    // Turn on page logging if specified.
    if (_pageLoggingEnabled) {
        sqlite3_begin_concurrent_report_enable(_db, 1);
    }

    // WAL is what allows simultaneous read/writing.
    SASSERT(!SQuery(_db, "enabling write ahead logging", "PRAGMA journal_mode = WAL;"));

    if (mmapSizeGB) {
        SASSERT(!SQuery(_db, "enabling memory-mapped I/O", "PRAGMA mmap_size=" + to_string(mmapSizeGB * 1024 * 1024 * 1024) + ";"));
    }

    // PRAGMA legacy_file_format=OFF sets the default for creating new databases, so it must be called before creating
    // any tables to be effective.
    SASSERT(!SQuery(_db, "new file format for DESC indexes", "PRAGMA legacy_file_format = OFF"));

    // Check if synchronous has been set and run query to use a custom synchronous setting
    if (!_synchronous.empty()) {
        SASSERT(!SQuery(_db, "setting custom synchronous commits", "PRAGMA synchronous = " + SQ(_synchronous)  + ";"));
    } else {
        DBINFO("Using SQLite default PRAGMA synchronous");
    }

    // These other pragmas only relate to read/write databases.
    SASSERT(!SQuery(_db, "disabling change counting", "PRAGMA count_changes = OFF;"));

    // Do our own checkpointing.
    sqlite3_wal_hook(_db, _sqliteWALCallback, this);

    // Enable tracing for performance analysis.
    sqlite3_trace_v2(_db, SQLITE_TRACE_STMT, _sqliteTraceCallback, this);

    // Update the cache. -size means KB; +size means pages
    SINFO("Setting cache_size to " << _cacheSize << "KB");
    SQuery(_db, "increasing cache size", "PRAGMA cache_size = -" + SQ(_cacheSize) + ";");

    // Now we (if we're the initializer) verify (and create if non-existent) all of our required journal tables.
    if (initializer) {
        for (int i = -1; i < createJournalTables; i++) {
            if (SQVerifyTable(_db, _getJournalTableName(i, true), "CREATE TABLE " + _getJournalTableName(i, true) +
                              " ( id INTEGER PRIMARY KEY, query TEXT, hash TEXT )")) {
                SHMMM("Created " << _getJournalTableName(i, true) << " table.");
            }
        }

        // And we'll figure out which journal tables actually exist, which may be more than we require. They must be
        // sequential.
        int currentJounalTable = -1;
        while(true) {
            string name = _getJournalTableName(currentJounalTable, true);
            if (SQVerifyTableExists(_db, name)) {
                _sharedData->_journalNames.push_back(name);
                currentJounalTable++;
            } else {
                break;
            }
        }
    }

    // Set our journal table name for this DB handle.
    _journalName = _getJournalTableName(_sharedData->_nextJournalCount.fetch_add(1));

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
        _sharedData->_commitCount.store(commitCount);

        // And then read the hash for that transaction.
        string lastCommittedHash, ignore;
        getCommit(commitCount, ignore, lastCommittedHash);
        _sharedData->_lastCommittedHash.store(lastCommittedHash);

        // If we have a commit count, we should have a hash as well.
        if (commitCount && lastCommittedHash.empty()) {
            SWARN("Loaded commit count " << commitCount << " with empty hash.");
        }
    }

    // Register the authorizer callback which allows callers to whitelist particular data in the DB.
    sqlite3_set_authorizer(_db, _sqliteAuthorizerCallback, this);

    // I tested and found that we could set about 10,000,000 and the number of steps to run and get a callback once a
    // second. This is set to be a bit more granular than that, which is probably adequate.
    sqlite3_progress_handler(_db, 1'000'000, _progressHandlerCallback, this);
}

SQLite::SQLite(const SQLite& from) :
    whitelist(nullptr),
    _sharedData(from._sharedData),
    _filename(from._filename),
    _journalSize(from._journalSize),
    _maxJournalSize(from._maxJournalSize),
    _insideTransaction(false),
    _beginElapsed(0),
    _readElapsed(0),
    _writeElapsed(0),
    _prepareElapsed(0),
    _commitElapsed(0),
    _rollbackElapsed(0),
    _mutexLocked(false),
    _enableRewrite(false),
    _currentlyRunningRewritten(false),
    _timeoutLimit(0),
    _abandonForCheckpoint(false),
    _autoRolledBack(false),
    _noopUpdateMode(false),
    _enableFullCheckpoints(from._enableFullCheckpoints),
    _queryCount(0),
    _cacheHits(0),
    _useCache(false),
    _isDeterministicQuery(false),
    _pageLoggingEnabled(from._pageLoggingEnabled),
    _currentTransactionAttemptCount(-1),
    _cacheSize(from._cacheSize),
    _synchronous(from._synchronous),
    _mmapSizeGB(from._mmapSizeGB)
{
    SINFO("Opening sqlite database: " << _filename);

    // Set our journal table name for this DB handle.
    _journalName = _getJournalTableName(_sharedData->_nextJournalCount.fetch_add(1));

    // Open the DB in read-write mode.
    DBINFO("Opening database '" << _filename << "'.");
    const int DB_WRITE_OPEN_FLAGS = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX;
    SASSERT(!sqlite3_open_v2(_filename.c_str(), &_db, DB_WRITE_OPEN_FLAGS, NULL));

    // Turn on page logging if specified.
    if (_pageLoggingEnabled) {
        sqlite3_begin_concurrent_report_enable(_db, 1);
    }

    if (_mmapSizeGB) {
        SASSERT(!SQuery(_db, "enabling memory-mapped I/O", "PRAGMA mmap_size=" + to_string(_mmapSizeGB * 1024 * 1024 * 1024) + ";"));
    }

    // PRAGMA legacy_file_format=OFF sets the default for creating new databases, so it must be called before creating
    // any tables to be effective.
    SASSERT(!SQuery(_db, "new file format for DESC indexes", "PRAGMA legacy_file_format = OFF"));

    // Check if synchronous has been set and run query to use a custom synchronous setting
    if (!_synchronous.empty()) {
        SASSERT(!SQuery(_db, "setting custom synchronous commits", "PRAGMA synchronous = " + SQ(_synchronous)  + ";"));
    } else {
        DBINFO("Using SQLite default PRAGMA synchronous");
    }

    // These other pragmas only relate to read/write databases.
    SASSERT(!SQuery(_db, "disabling change counting", "PRAGMA count_changes = OFF;"));

    // Do our own checkpointing.
    sqlite3_wal_hook(_db, _sqliteWALCallback, this);

    // Enable tracing for performance analysis.
    sqlite3_trace_v2(_db, SQLITE_TRACE_STMT, _sqliteTraceCallback, this);

    // Update the cache. -size means KB; +size means pages
    SINFO("Setting cache_size to " << _cacheSize << "KB");
    SQuery(_db, "increasing cache size", "PRAGMA cache_size = -" + SQ(_cacheSize) + ";");

    // Register the authorizer callback which allows callers to whitelist particular data in the DB.
    sqlite3_set_authorizer(_db, _sqliteAuthorizerCallback, this);

    // I tested and found that we could set about 10,000,000 and the number of steps to run and get a callback once a
    // second. This is set to be a bit more granular than that, which is probably adequate.
    sqlite3_progress_handler(_db, 1'000'000, _progressHandlerCallback, this);
}

int SQLite::_progressHandlerCallback(void* arg) {
    SQLite* sqlite = static_cast<SQLite*>(arg);
    uint64_t now = STimeNow();
    if (sqlite->_timeoutLimit && now > sqlite->_timeoutLimit) {
        // Timeout! We don't throw here, we let `read` and `write` do it so we don't throw out of the middle of a
        // sqlite3 operation.
        sqlite->_timeoutError = now - sqlite->_timeoutStart;

        // Return non-zero causes sqlite to interrupt the operation.
        return 1;
    } else if (sqlite->_sharedData->_checkpointThreadBusy.load()) {
        SINFO("[checkpoint] Abandoning transaction to unblock checkpoint");
        sqlite->_abandonForCheckpoint = true;
        return 2;
    }
    return 0;
}

void SQLite::_sqliteLogCallback(void* pArg, int iErrCode, const char* zMsg) {
    SSYSLOG(LOG_INFO, "[info] " << "{SQLITE} Code: " << iErrCode << ", Message: " << zMsg);
}

int SQLite::_sqliteTraceCallback(unsigned int traceCode, void* c, void* p, void* x) {
    if (enableTrace && traceCode == SQLITE_TRACE_STMT) {
        SINFO("NORMALIZED_SQL:" << sqlite3_normalized_sql((sqlite3_stmt*)p));
    }
    return 0;
}

int SQLite::_sqliteWALCallback(void* data, sqlite3* db, const char* dbName, int pageCount) {
    SQLite* object = static_cast<SQLite*>(data);
    object->_sharedData->_currentPageCount.store(pageCount);
    // Try a passive checkpoint if full checkpoints aren't enabled, *or* if the page count is less than the required
    // size for a full checkpoint.
    if (!object->_enableFullCheckpoints || pageCount < fullCheckpointPageMin.load()) {
        int passive = passiveCheckpointPageMin.load();
        if (!passive || pageCount < passive) {
            SINFO("[checkpoint] skipping checkpoint with " << pageCount
                  << " pages in WAL file (checkpoint every " << passive << " pages).");
        } else {
            int walSizeFrames = 0;
            int framesCheckpointed = 0;
            uint64_t start = STimeNow();
            int result = sqlite3_wal_checkpoint_v2(db, dbName, SQLITE_CHECKPOINT_PASSIVE, &walSizeFrames, &framesCheckpointed);
            SINFO("[checkpoint] passive checkpoint complete with " << pageCount
                  << " pages in WAL file. Result: " << result << ". Total frames checkpointed: "
                  << framesCheckpointed << " of " << walSizeFrames << " in " << ((STimeNow() - start) / 1000) << "ms.");
        }
    } else {
        // If we get here, then full checkpoints are enabled, and we have enough pages in the WAL file to perform one.
        SINFO("[checkpoint] " << pageCount << " pages behind, beginning complete checkpoint.");

        // This thread will run independently. We capture the variables we need here and pass them by value.
        string filename = object->_filename;
        string dbNameCopy = dbName;
        int alreadyCheckpointing = object->_sharedData->_checkpointThreadBusy.fetch_add(1);
        if (alreadyCheckpointing) {
            SINFO("[checkpoint] Not starting checkpoint thread. It's already running.");
            return SQLITE_OK;
        }
        SDEBUG("[checkpoint] starting thread with count: " << object->_sharedData->_currentPageCount.load());

        // This thread will need it's own SQLite object to work on, since the constructing thread might finish and
        // destroy it's own object, so we let this one persist until the checkpoint thread finishes.
        shared_ptr<SQLite> shared_object = make_shared<SQLite>(*object);
        thread([shared_object, filename, dbNameCopy]() {
            SInitialize("checkpoint");
            uint64_t start = STimeNow();

            // Lock the mutex that keeps anyone from starting a new transaction.
            lock_guard<decltype(shared_object->_sharedData->blockNewTransactionsMutex)> transactionLock(shared_object->_sharedData->blockNewTransactionsMutex);

            while (1) {
                // Lock first, this prevents anyone from updating the count while we're operating here.
                unique_lock<mutex> lock(shared_object->_sharedData->notifyWaitMutex);

                // Now that we have the lock, check the count. If there are no outstanding transactions, we can
                // checkpoint immediately, and then we'll return.
                int count = shared_object->_sharedData->currentTransactionCount.load();

                // Lets re-check if we still need a full check point, it could be that a passive check point runs
                // after we have started this loop and check points a large chunk or all of the pages we were trying
                // to check point here. That means that this thread is now blocking new transactions waiting to run a
                // full check point for no reason. We wait for the page count to be less than half of the required amount
                // to prevent bouncing off of this check every loop. If that's the case, just break out of the this loop
                // and wait for the next full check point to be required.
                int pageCount = shared_object->_sharedData->_currentPageCount.load();
                if (pageCount < (fullCheckpointPageMin.load() / 2)) {
                    SINFO("[checkpoint] Page count decreased below half the threshold, count is now " << pageCount << ", exiting full checkpoint loop.");
                    break;
                } else {
                    SINFO("[checkpoint] Waiting on " << count << " remaining transactions.");
                    for (auto listener : shared_object->_sharedData->_checkpointListeners) {
                        listener->checkpointRequired(*shared_object);
                    }
                }

                if (count == 0) {

                    // Time and run the checkpoint operation.
                    uint64_t checkpointStart = STimeNow();
                    SINFO("[checkpoint] Waited " << ((checkpointStart - start) / 1000)
                          << "ms for pending transactions. Starting complete checkpoint.");
                    int walSizeFrames = 0;
                    int framesCheckpointed = 0;
                    int result = sqlite3_wal_checkpoint_v2(shared_object->_db, dbNameCopy.c_str(), SQLITE_CHECKPOINT_RESTART, &walSizeFrames, &framesCheckpointed);
                    SINFO("[checkpoint] restart checkpoint complete. Result: " << result << ". Total frames checkpointed: "
                          << framesCheckpointed << " of " << walSizeFrames
                          << " in " << ((STimeNow() - checkpointStart) / 1000) << "ms.");

                    // We're done. Anyone can start a new transaction.
                    for (auto listener : shared_object->_sharedData->_checkpointListeners) {
                        listener->checkpointComplete(*shared_object);
                    }
                    break;
                }

                // There are outstanding transactions (or we would have hit `break` above), so we'll wait until
                // someone says the count has changed, and try again.
                shared_object->_sharedData->blockNewTransactionsCV.wait(lock);
            }

            // Allow the next checkpointer.
            shared_object->_sharedData->_checkpointThreadBusy.store(0);
        }).detach();
    }
    return SQLITE_OK;
}

string SQLite::_getJournalQuery(const list<string>& queryParts, bool append) {
    list<string> queries;
    for (const string& name : _sharedData->_journalNames) {
        queries.emplace_back(SComposeList(queryParts, " " + name + " ") + (append ? " " + name : ""));
    }
    string query = SComposeList(queries, " UNION ");
    return query;
}

string SQLite::_getJournalTableName(int64_t journalTableID, bool create) {
    // Return the base name if the number specified is negative.
    if (journalTableID < 0) {
        return "journal";
    }
    if (create) {
        char buff[27] = {0};
        sprintf(buff, "journal%04li", journalTableID);
        return buff;
    } else {
        if (_sharedData->_journalNames.size() == 0) {
            STHROW("Attempting to get a journal table name for existing journals, but there are none!");
        }
        // This deliberately skips `journal` itself, assuming that's in position 1.
        size_t journalTableIndex = (journalTableID % _sharedData->_journalNames.size() - 1) + 1 ;
        return _sharedData->_journalNames[journalTableIndex];
    }
}

SQLite::~SQLite() {
    // Now we can clean up our own data.
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

void SQLite::waitForCheckpoint() {
    lock_guard<mutex> lock(_sharedData->blockNewTransactionsMutex);
}

bool SQLite::beginTransaction(bool useCache, const string& transactionName) {
    SASSERT(!_insideTransaction);
    SASSERT(_uncommittedHash.empty());
    SASSERT(_uncommittedQuery.empty());
    {
        unique_lock<mutex> lock(_sharedData->notifyWaitMutex);
        _sharedData->currentTransactionCount++;
    }
    _sharedData->blockNewTransactionsCV.notify_one();

    // Reset before the query, as it's possible the query sets these.
    _abandonForCheckpoint = false;
    _autoRolledBack = false;

    SDEBUG("[concurrent] Beginning transaction");
    uint64_t before = STimeNow();
    _currentTransactionAttemptCount = -1;
    _insideTransaction = !SQuery(_db, "starting db transaction", "BEGIN CONCURRENT");
    _queryCache.clear();
    _transactionName = transactionName;
    _useCache = useCache;
    _queryCount = 0;
    _cacheHits = 0;
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
            SHMMM("'" << tableName << "' has incorrect schema, need to upgrade? Is '" << collapsedResult << "' expected  '" << collapsedSQL << "'");
            return false;
        }
    }
}

bool SQLite::verifyIndex(const string& indexName, const string& tableName, const string& indexSQLDefinition, bool isUnique, bool createIfNotExists) {
    SINFO("Verifying index '" << indexName << "'. isUnique? " << to_string(isUnique));
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
    uint64_t before = STimeNow();
    _queryCount++;
    if (_useCache) {
        auto foundQuery = _queryCache.find(query);
        if (foundQuery != _queryCache.end()) {
            result = foundQuery->second;
            _cacheHits++;
            return true;
        }
    }
    _isDeterministicQuery = true;
    bool queryResult = !SQuery(_db, "read only query", query, result);
    if (_useCache && _isDeterministicQuery && queryResult) {
        _queryCache.emplace(make_pair(query, result));
    }
    _checkInterruptErrors("SQLite::read"s);
    _readElapsed += STimeNow() - before;
    return queryResult;
}

void SQLite::_checkInterruptErrors(const string& error) {

    // Local error code.
    int errorCode = 0;
    uint64_t time = 0;

    // First check timeout. we want this to override the others, so we can't get stuck in an endless loop where we do
    // something like throw `checkpoint_required_error` forever and never notice that the command has timed out.
    if (_timeoutLimit) {
        uint64_t now = STimeNow();
        if (now > _timeoutLimit) {
            _timeoutError = now - _timeoutStart;
        }
        if (_timeoutError) {
            time = _timeoutError;
            resetTiming();
            errorCode = 1;
        }
    }

    if (_abandonForCheckpoint) {
        errorCode = 2;
    }

    // If we had an interrupt error, and were inside a transaction, and autocommit is now on, we have been auto-rolled
    // back, we won't need to actually do a rollback for this transaction.
    if (errorCode && _insideTransaction && sqlite3_get_autocommit(_db)) {
        SHMMM("Transaction automatically rolled back. Setting _autoRolledBack = true");
        _autoRolledBack = true;
    }

    // Reset this regardless of which error (or both) occurred. If we handled a timeout, this is still done, we don't
    // need to abandon this later.
    _abandonForCheckpoint = false;

    if (errorCode == 1) {
        throw timeout_error("timeout in "s + error, time);
    } else if (errorCode == 2) {
        throw checkpoint_required_error();
    }

    // Otherwise, no error.
}

bool SQLite::write(const string& query) {
    if (_noopUpdateMode) {
        SALERT("Non-idempotent write in _noopUpdateMode. Query: " << query);
        return true;
    }

    // This is literally identical to the idempotent version except for the check for _noopUpdateMode.
    return _writeIdempotent(query);
}

bool SQLite::writeIdempotent(const string& query) {
    return _writeIdempotent(query);
}

bool SQLite::writeUnmodified(const string& query) {
    return _writeIdempotent(query, true);
}

bool SQLite::_writeIdempotent(const string& query, bool alwaysKeepQueries) {
    SASSERT(_insideTransaction);
    _queryCache.clear();
    _queryCount++;
    SASSERT(query.empty() || SEndsWith(query, ";"));                        // Must finish everything with semicolon
    SASSERTWARN(SToUpper(query).find("CURRENT_TIMESTAMP") == string::npos); // Else will be replayed wrong

    // First, check our current state
    SQResult results;
    SASSERT(!SQuery(_db, "looking up schema version", "PRAGMA schema_version;", results));
    SASSERT(!results.empty() && !results[0].empty());
    uint64_t schemaBefore = SToUInt64(results[0][0]);
    uint64_t changesBefore = sqlite3_total_changes(_db);

    // Try to execute the query
    uint64_t before = STimeNow();
    bool result = false;
    bool usedRewrittenQuery = false;
    if (_enableRewrite) {
        int resultCode = SQuery(_db, "read/write transaction", query, 2000 * STIME_US_PER_MS, true);
        if (resultCode == SQLITE_AUTH) {
            // Run re-written query.
            _currentlyRunningRewritten = true;
            SASSERT(SEndsWith(_rewrittenQuery, ";"));
            result = !SQuery(_db, "read/write transaction", _rewrittenQuery);
            usedRewrittenQuery = true;
            _currentlyRunningRewritten = false;
        } else {
            result = !resultCode;
        }
    } else {
        result = !SQuery(_db, "read/write transaction", query);
    }
    _checkInterruptErrors("SQLite::write"s);
    _writeElapsed += STimeNow() - before;
    if (!result) {
        return false;
    }

    // See if the query changed anything
    SASSERT(!SQuery(_db, "looking up schema version", "PRAGMA schema_version;", results));
    SASSERT(!results.empty() && !results[0].empty());
    uint64_t schemaAfter = SToUInt64(results[0][0]);
    uint64_t changesAfter = sqlite3_total_changes(_db);

    // If something changed, or we're always keeping queries, then save this.
    if (alwaysKeepQueries || (schemaAfter > schemaBefore) || (changesAfter > changesBefore)) {
        _uncommittedQuery += usedRewrittenQuery ? _rewrittenQuery : query;
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
    uint64_t commitCount = _sharedData->_commitCount.load();

    // Queue up the journal entry
    string lastCommittedHash = getCommittedHash();
    _uncommittedHash = SToHex(SHashSHA1(lastCommittedHash + _uncommittedQuery));
    uint64_t before = STimeNow();

    // Crete our query.
    string query = "INSERT INTO " + _journalName + " VALUES (" + SQ(commitCount + 1) + ", " + SQ(_uncommittedQuery) + ", " + SQ(_uncommittedHash) + " )";

    // These are the values we're currently operating on, until we either commit or rollback.
    _sharedData->_inFlightTransactions[commitCount + 1] = make_pair(_uncommittedQuery, _uncommittedHash);

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

    // Record DB pages before commit to see how many the commit touches.
    int startPages, dummy;
    sqlite3_db_status(_db, SQLITE_DBSTATUS_CACHE_WRITE, &startPages, &dummy, 0);

    uint64_t before = STimeNow();
    uint64_t beforeCommit = STimeNow();
    if (_pageLoggingEnabled) {
        {
            lock_guard<mutex> lock(_pageLogMutex);
            _currentTransactionAttemptCount = _transactionAttemptCount.fetch_add(1);
            result = SQuery(_db, "committing db transaction", "COMMIT");
        }
    } else {
        result = SQuery(_db, "committing db transaction", "COMMIT");
    }
    SINFO("SQuery 'COMMIT' took " << ((STimeNow() - beforeCommit)/1000) << "ms.");

    // And record pages after the commit.
    int endPages;
    sqlite3_db_status(_db, SQLITE_DBSTATUS_CACHE_WRITE, &endPages, &dummy, 0);

    // Similarly, record WAL file size.
    sqlite3_file *pWal = 0;
    sqlite3_int64 sz;
    sqlite3_file_control(_db, "main", SQLITE_FCNTL_JOURNAL_POINTER, &pWal);
    pWal->pMethods->xFileSize(pWal, &sz);

    // And log both these statistics.
    SINFO("COMMIT operation wrote " << (endPages - startPages) << " pages. WAL file size is " << sz << " bytes.");

    // If there were conflicting commits, will return SQLITE_BUSY_SNAPSHOT
    SASSERT(result == SQLITE_OK || result == SQLITE_BUSY_SNAPSHOT);
    if (result == SQLITE_OK) {
        if (_currentTransactionAttemptCount != -1) {
            const char* report = sqlite3_begin_concurrent_report(_db);
            string logLine = SWHEREAMI + "[row-level-locking] transaction attempt:" +
                             to_string(_currentTransactionAttemptCount) + " committed. report: " +
                             (report ? string(report) : "null"s);
            syslog(LOG_DEBUG, "%s", logLine.c_str());
        }
        _commitElapsed += STimeNow() - before;
        _journalSize = newJournalSize;
        _sharedData->_commitCount++;
        _sharedData->_committedTransactionIDs.insert(_sharedData->_commitCount.load());
        _sharedData->_lastCommittedHash.store(_uncommittedHash);
        SDEBUG("Commit successful (" << _sharedData->_commitCount.load() << "), releasing commitLock.");
        _insideTransaction = false;
        _uncommittedHash.clear();
        _uncommittedQuery.clear();
        _mutexLocked = false;
        {
            unique_lock<mutex> lock(_sharedData->notifyWaitMutex);
            _sharedData->currentTransactionCount--;
        }
        _sharedData->blockNewTransactionsCV.notify_one();
        g_commitLock.unlock();
        _queryCache.clear();
        if (_useCache) {
            SINFO("Transaction commit with " << _queryCount << " queries attempted, " << _cacheHits << " served from cache for '" << _transactionName << "'.");
        }
        _useCache = false;
        _queryCount = 0;
        _cacheHits = 0;
    } else {
        if (_currentTransactionAttemptCount != -1) {
            string logLine = SWHEREAMI  + "[row-level-locking] transaction attempt:" +
                             to_string(_currentTransactionAttemptCount) + " conflict, will roll back.";
            syslog(LOG_DEBUG, "%s", logLine.c_str());
            SINFO("Commit failed, waiting for rollback.");
        }
    }

    // if we got SQLITE_BUSY_SNAPSHOT, then we're *still* holding commitLock, and it will need to be unlocked by
    // calling rollback().
    return result;
}

map<uint64_t, pair<string, string>> SQLite::getCommittedTransactions() {
    SQLITE_COMMIT_AUTOLOCK;

    // Maps a committed transaction ID to the correct query and hash for that transaction.
    map<uint64_t, pair<string, string>> result;

    // If nothing's been committed, nothing to return.
    if (_sharedData->_committedTransactionIDs.empty()) {
        return result;
    }

    // For each transaction that we've committed, we'll remove the that transaction from the "in flight" list, and
    // return that to the caller. This lets SQLiteNode get a list of transactions that have been committed since the
    // last time it called this function, so that it can replicate them to peers.
    for (uint64_t key : _sharedData->_committedTransactionIDs) {
        result[key] = move(_sharedData->_inFlightTransactions.at(key));
        _sharedData->_inFlightTransactions.erase(key);
    }

    // There are no longer any outstanding transactions, so we can clear this.
    _sharedData->_committedTransactionIDs.clear();
    return result;
}

void SQLite::rollback() {
    // Make sure we're actually inside a transaction
    if (_insideTransaction) {
        // Cancel this transaction
        if (_autoRolledBack) {
            SINFO("Transaction was automatically rolled back, not sending 'ROLLBACK'.");
            _autoRolledBack = false;
        } else {
            if (_uncommittedQuery.size()) {
                SINFO("Rolling back transaction: " << _uncommittedQuery.substr(0, 100));
            }
            uint64_t before = STimeNow();
            SASSERT(!SQuery(_db, "rolling back db transaction", "ROLLBACK"));
            _rollbackElapsed += STimeNow() - before;
        }

        if (_currentTransactionAttemptCount != -1) {
            const char* report = sqlite3_begin_concurrent_report(_db);
            string logLine = SWHEREAMI + "[row-level-locking] transaction attempt:" +
                             to_string(_currentTransactionAttemptCount) + " rolled back. report: " +
                             (report ? string(report) : "null"s);
            syslog(LOG_DEBUG, "%s", logLine.c_str());
        }

        // Finally done with this.
        _insideTransaction = false;
        _uncommittedHash.clear();
        if (_uncommittedQuery.size()) {
            SINFO("Rollback successful.");
        }
        _uncommittedQuery.clear();

        // Only unlock the mutex if we've previously locked it. We can call `rollback` to cancel a transaction without
        // ever having called `prepare`, which would have locked our mutex.
        if (_mutexLocked) {
            _mutexLocked = false;
            g_commitLock.unlock();
        }
        {
            unique_lock<mutex> lock(_sharedData->notifyWaitMutex);
            _sharedData->currentTransactionCount--;
        }
        _sharedData->blockNewTransactionsCV.notify_one();
    } else {
        SINFO("Rolling back but not inside transaction, ignoring.");
    }
    _queryCache.clear();
    if (_useCache) {
        SINFO("Transaction rollback with " << _queryCount << " queries attempted, " << _cacheHits << " served from cache for '" << _transactionName << "'.");
    }
    _useCache = false;
    _queryCount = 0;
    _cacheHits = 0;
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

    // If we found a hash, we assume this was a good commit, as we'll allow an empty commit.
    return (!hash.empty());
}

string SQLite::getCommittedHash() {
    return _sharedData->_lastCommittedHash.load();
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
    return _sharedData->_commitCount.load();
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

void SQLite::enableRewrite(bool enable) {
    _enableRewrite = enable;
}

void SQLite::setRewriteHandler(bool (*handler)(int, const char*, string&)) {
    _rewriteHandler = handler;
}

int SQLite::_sqliteAuthorizerCallback(void* pUserData, int actionCode, const char* detail1, const char* detail2,
                                      const char* detail3, const char* detail4)
{
    SQLite* db = static_cast<SQLite*>(pUserData);
    return db->_authorize(actionCode, detail1, detail2, detail3, detail4);
}

int SQLite::_authorize(int actionCode, const char* detail1, const char* detail2, const char* detail3, const char* detail4) {
    // If we've enabled re-writing, see if we need to re-write this query.
    if (_enableRewrite && !_currentlyRunningRewritten && (*_rewriteHandler)(actionCode, detail1, _rewrittenQuery)) {
        // Deny the original query, we'll re-run on the re-written version.
        return SQLITE_DENY;
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
            !strcmp(detail2, "sqlite3_version")
        ) {
            _isDeterministicQuery = false;
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

void SQLite::startTiming(uint64_t timeLimitUS) {
    _timeoutStart = STimeNow();
    _timeoutLimit = _timeoutStart + timeLimitUS;
    _timeoutError = 0;
}

void SQLite::resetTiming() {
    _timeoutLimit = 0;
    _timeoutStart = 0;
    _timeoutError = 0;
}

void SQLite::setUpdateNoopMode(bool enabled) {
    if (_noopUpdateMode == enabled) {
        return;
    }

    // Enable or disable this query.
    string query = "PRAGMA noop_update="s + (enabled ? "ON" : "OFF") + ";";
    SQuery(_db, "setting noop-update mode", query);
    _noopUpdateMode = enabled;

    // If we're inside a transaction, make sure this gets saved so it can be replicated.
    // If we're not (i.e., a transaction's already been rolled back), no need, there's nothing to replicate.
    if (_insideTransaction) {
        _uncommittedQuery += query;
    }
}

bool SQLite::getUpdateNoopMode() const {
    return _noopUpdateMode;
}

void SQLite::addCheckpointListener(SQLite::CheckpointRequiredListener& listener) {
    lock_guard<mutex> lock(_sharedData->_checkpointListenerMutex);
    _sharedData->_checkpointListeners.insert(&listener);
}

void SQLite::removeCheckpointListener(SQLite::CheckpointRequiredListener& listener) {
    lock_guard<mutex> lock(_sharedData->_checkpointListenerMutex);
    _sharedData->_checkpointListeners.erase(&listener);
}

SQLite::SharedData::SharedData() :
_nextJournalCount(-1),
currentTransactionCount(0),
_currentPageCount(0),
_checkpointThreadBusy(0)
{ }
