#pragma once
#include <libstuff/sqlite3.h>

// Convenience macro for locking our static commit lock.
#define SQLITE_COMMIT_AUTOLOCK lock_guard<decltype(SQLite::g_commitLock)> \
        __SSQLITEAUTOLOCK_##__LINE__(SQLite::g_commitLock)

class SQLite {
  public:

    class timeout_error : public exception {
      public :
        timeout_error(const string& e, uint64_t time) : _what(e), _time(time) {};
        virtual ~timeout_error() {};
        const char* what() const noexcept { return _what.c_str(); }
        const uint64_t time() const noexcept { return _time; }
      private:
        string _what;
        uint64_t _time;
    };

    class checkpoint_required_error : public exception {
      public :
        checkpoint_required_error() {};
        virtual ~checkpoint_required_error() {};
        const char* what() const noexcept { return "checkpoint_required"; }
    };

    // Abstract base class for objects that need to be notified when we set `checkpointRequired` and then when tyhat
    // checkpoint is complete.
    class CheckpointRequiredListener {
      public:
        virtual void checkpointRequired(SQLite& db) = 0;
        virtual void checkpointComplete(SQLite& db) = 0;
    };

    // This publicly exposes our core mutex, allowing other classes to perform extra operations around commits and
    // such, when they determine that those operations must be made atomically with operations happening in SQLite.
    // This can be locked with the SQLITE_COMMIT_AUTOLOCK macro, as well.
    static recursive_mutex& g_commitLock;

    // createJournalTables: Creates the specified number of journal tables. If `0`, no tables are created. This
    //                      specifies the total number of journal tables, not new ones, so if there are 50 existent
    //                      tables and you pass `100` here, you get 100 total tables, not 150. 
    //
    // mmapSizeGB: address space to use for memory-mapped IO, in GB.
    SQLite(const string& filename, int cacheSize, bool enableFullCheckpoints, int maxJournalSize,
           int createJournalTables, const string& synchronous = "", int64_t mmapSizeGB = 0, bool pageLoggingEnabled = false);

    // This constructor is not exactly a copy constructor. It creates an other SQLite object based on the first except
    // with a *different* journal table. This avoids a lot of locking around creating structures that we know already
    // exist because we already have a SQLite object for this file.
    SQLite(const SQLite& from);
    ~SQLite();

    // Returns the canonicalized filename for this database
    string getFilename() { return _filename; }

    // Performs a read-only query (eg, SELECT). This can be done inside or outside a transaction. Returns true on
    // success, and fills the 'result' with the result of the query.
    bool read(const string& query, SQResult& result);

    // Performs a read-only query (eg, SELECT) that returns a single value.
    string read(const string& query);

    // Begins a new transaction. Returns true on success. Can optionally be instructed to use the query cache, if so
    // the transaction can be named so that log lines about cache success can be associated to the transaction.
    bool beginTransaction(bool useCache = false, const string& transactionName = "");

    // Begins a new concurrent transaction. Returns true on success. Can optionally be instructed to use the query
    // cache, if so the transaction can be named so that log lines about cache success can be associated to the
    // transaction.
    bool beginConcurrentTransaction(bool useCache = false, const string& transactionName = "");

    // Verifies a table exists and has a particular definition. If the database is left with the right schema, it
    // returns true. If it had to create a new table (ie, the table was missing), it also sets created to true. If the
    // table is already there with the wrong schema, it returns false.
    bool verifyTable(const string& name, const string& sql, bool& created);

    // Verifies an index exists on the given table with the given definition. Optionally create it if it doesn't exist.
    // Be careful, creating an index can be expensive on large tables!
    // Returns false if the index does not exist and was not created.
    bool verifyIndex(const string& indexName, const string& tableName, const string& indexSQLDefinition, bool isUnique, bool createIfNotExists = false);

    // Adds a column to a table.
    bool addColumn(const string& tableName, const string& column, const string& columnType);

    // Performs a read/write query (eg, INSERT, UPDATE, DELETE). This is added to the current transaction's query list.
    // Returns true  on success.
    // If we're in noop-update mode, this call alerts and performs no write, but returns as if it had completed.
    bool write(const string& query);

    // This is the same as `write` except it runs successfully without any warnings or errors in noop-update mode.
    // It's intended to be used for `mockRequest` enabled commands, such that we only run a version of them that's
    // known to be repeatable. What counts as repeatable is up to the individual command.
    bool writeIdempotent(const string& query);

    // This runs a query completely unchanged, always adding it to the uncommitted query, such that it will be recorded
    // in the journal even if it had no effect on the database. This lets replicated or synchronized queries be added
    // to the journal *even if they have no effect* on the rest of the database.
    bool writeUnmodified(const string& query);

    // Enable or disable update-noop mode.
    void setUpdateNoopMode(bool enabled);
    bool getUpdateNoopMode() const;

    // Prepare to commit or rollback the transaction. This also inserts the current uncommitted query into the
    // journal; no additional writes are allowed until the next transaction has begun.
    bool prepare();

    // This enables or disables automatic re-writing. This feature is to support mocked requests and load testing. This
    // overloads set_authorizer to allow a plugin to deny certain queries from running (currently based only on the
    // action being taken and the table being operated on) and instead, run a different query in their place. For
    // instance, this can replace an INSERT statement into a particular table with a no-op, or an INSERT immediately
    // followed by a DELETE. When enabled, this is implemented as follows:
    //
    // Plugin calls `write`.
    // 1. If enableRewrite is ON, the rewriteHandler is run as part of the authorizer.
    // 2. If the rewriteHandler wants to re-write a query, it should return `true` and update the string reference it
    //    was passed (see setRewriteHandler() below).
    // 3. If the rewriteHandler returns true, the initial query will fail with SQLITE_AUTH (warnings for this failure
    //    are suppressed) and the new replacement query will be run in it's place.
    void enableRewrite(bool enable);

    // Update the rewrite handler.
    // The rewrite handler accepts an SQLite action code, a table name, and a refernce to a string.
    // If the action and table name indicate that the query should be re-written, then `newQuery` should be updated to
    // the new query to run, and `true` should be returned. If the query doesn't need to be re-written, then `false`
    // should be returned.
    // This function is only called when enableRewrite is true.
    // Important: there can be only one re-write handler for a given DB at once.
    void setRewriteHandler(bool (*handler)(int, const char*, string&));

    // Commits the current transaction to disk. Returns an sqlite3 result code.
    int commit();

    // Cancels the current transaction and rolls it back
    void rollback();

    // Returns the total number of changes on this database
    int getChangeCount() { return sqlite3_total_changes(_db); }

    // Returns the timing of the last command
    uint64_t getLastTransactionTiming(uint64_t& begin, uint64_t& read, uint64_t& write, uint64_t& prepare,
                                      uint64_t& commit, uint64_t& rollback);

    // Returns the number of changes that were performed in the last query.
    size_t getLastWriteChangeCount();

    // Returns the current value of commitCount, which should be the highest ID of a commit in any handle to the
    // database.
    uint64_t getCommitCount();

    // Returns the current state of the database, as a SHA1 hash of all queries committed.
    string getCommittedHash();

    // Returns what the new state will be of the database if the current transaction is committed.
    string getUncommittedHash() { return _uncommittedHash; }

    // Returns a concatenated string containing all the 'write' queries executed within the current, uncommitted
    // transaction.
    string getUncommittedQuery() { return _uncommittedQuery; }

    // Gets the ROWID of the last insertion (for auto-increment indexes)
    int64_t getLastInsertRowID();

    // Gets any error message associated with the previous query
    string getLastError() { return sqlite3_errmsg(_db); }

    // Returns true if we're inside an uncommitted transaction.
    bool insideTransaction() { return _insideTransaction; }

    // Looks up the exact SQL of a paricular commit to the database, as well as gets the SHA1 hash of the database
    // immediately following tha commit.
    bool getCommit(uint64_t index, string& query, string& hash);

    // Looks up a range of commits
    bool getCommits(uint64_t fromIndex, uint64_t toIndex, SQResult& result);

    // Start a timing operation, that will time out after the given number of microseconds.
    void startTiming(uint64_t timeLimitUS);

    // Reset timing after finishing a timed operation.
    void resetTiming();

    // Register and deregister listeners for checkpoint operations.
    void addCheckpointListener(CheckpointRequiredListener& listener);
    void removeCheckpointListener(CheckpointRequiredListener& listener);

    // This atomically removes and returns committed transactions from our inflight list. SQLiteNode can call this, and
    // it will return a map of transaction IDs to tuples of (query, hash, concurrent), so that those transactions can
    // be replicated out to peers.
    map<uint64_t, tuple<string, string, bool>> getCommittedTransactions();

    // The whitelist is either nullptr, in which case the feature is disabled, or it's a map of table names to sets of
    // column names that are allowed for reading. Using whitelist at all put the database handle into a more
    // restrictive access mode that will deny access for write operations and other potentially risky operations, even
    // in the case that a specific table/column are not being directly requested.
    map<string, set<string>>* whitelist;

    // Call before starting a transaction to make sure we don't interrupt a checkpoint operation.
    void waitForCheckpoint();

    // These are the minimum thresholds for the WAL file, in pages, that will cause us to trigger either a full or
    // passive checkpoint. They're public, non-const, and atomic so that they can be configured on the fly.
    static atomic<int> passiveCheckpointPageMin;
    static atomic<int> fullCheckpointPageMin;

    // Enable/disable SQL statement tracing.
    static atomic<bool> enableTrace;

  private:

    // This structure contains all of the data that's shared between a set of SQLite objects that share the same
    // underlying database file.
    struct SharedData {
        // Constructor.
        SharedData();

        // This is the last committed hash by *any* thread for this file.
        atomic<string> _lastCommittedHash;

        // An identifier used to choose the next journal table to use with this set of DB handles.
        atomic<int64_t> _nextJournalCount;

        // This is a set of transactions IDs that have been successfully committed to the database, but not yet sent to
        // peers.
        set<uint64_t> _committedTransactionIDs;

        // The current commit count, loaded at initialization from the highest commit ID in the DB, and then accessed
        // though this atomic integer. getCommitCount() returns the value of this variable.
        atomic<uint64_t> _commitCount;

        // Names of journal tables for this database.
        vector<string> _journalNames;

        // Explanation: Why do we keep a list of outstanding transactions, instead of just looking them up when we need
        // them (i.e., look up all transaction with an ID greater than the last one sent to peers when we need to send them
        // to peers)?
        // Originally, that had been the idea, but we ran into a problem with the way we send transactions to peers. When
        // doing parallel writes, we'll always write commits to the database in order. We particularly construct locks
        // around both `prepare()` and `commit()` in SQLiteNode to handle this (and yes, the fact that I'm discussing
        // SQLiteNode in SQlite.h is not a sign of great encapsulation). In general then, since rows are always added to
        // the DB and then committed in order, we could just keep a pointer to the last-sent transaction, and send all
        // transactions following that one to peers.
        //
        // However, this breaks down when we need to do a quorum command. In this case, we perform the following actions
        // from the sync thread:
        //
        // 1) processCommand()
        // 2) mutex.lock()
        // 3) sendOutstandingTransactions()
        // 4) prepare()
        // 5) commit() <- this is a distributed commit.
        //
        // We need to sendOutstandingTransactions() before calling commit(), because this is a distributed commit and if we
        // don't send any outstanding transactions to peers before sending the current one, then transactions will arrive
        // at peers out of order. We also need to lock our mutex before calling sendOutstandingTransactions() to prevent
        // any other threads from making new commits while we're sending them, which would result in the same out-of-order
        // sending when we completed sendOutstandingTransactions(), but still had (newly committed) transactions to send.
        //
        // The problem that requires us to keep lists of outstanding transactions is that when we call processCommand() in
        // the current thread, sqlite will use a snapshot of the database taken at that point (the point at which we do
        // `BEGIN CONCURRENT`) until we either commit or rollback the transaction.
        // That means that if any other thread makes a new commit to the database after we've started process(), but before
        // we call sendOutstandingTransactions(), we won't see it from the current thread, because we're operating on on
        // old database snpashot until we either rollback(), which defeats the purpose of committing a new transaction, or
        // we commit(), which we can't do yet because we need to send outstanding transactions first.
        //
        // We could grab out mutex earlier, before calling processCommand(), which would avoid this situation, but it would
        // cause all other threads to wait for the entire duration of processCommand() in this thread, which is the sort of
        // performance problem we're trying to avoid in the first place with parallel writes. Instead, when each thread
        // adds new commits, it makes them available in the following lists, so that we'll have access to them in
        // sendOutstandingTransactions(), even if the current thread is operating on an old DB snapshot.
        //
        // NOTE: Both of the following collections (_inFlightTransactions and _committedtransactionIDs) are shared between
        // all threads and need to be accessed in a synchronized fashion. They do *NOT* implement their own synchronization
        // and must be protected by locking `_commitLock`.
        //
        // This is a map of all currently "in flight" transactions. These are transactions for which a `prepare()` has been
        // called to generate a journal row, but have not yet been sent to peers.
        map<uint64_t, tuple<string, string, bool>> _inFlightTransactions;

        // This mutex prevents any thread starting a new transaction when locked. The checkpoint thread will lock it
        // when required to make sure it can get exclusive use of the DB.
        mutex blockNewTransactionsMutex;

        // These three variables let us notify the checkpoint thread when a transaction ends (or starts, but it will
        // have blocked any new ones from starting by locking blockNewTransactionsMutex).
        mutex notifyWaitMutex;
        condition_variable blockNewTransactionsCV;
        atomic<int> currentTransactionCount;

        // This is the count of current pages waiting to be check pointed. This potentially changes with every wal callback
        // we need to store it across callbacks so we can check if the full check point thread still needs to run.
        atomic<int> _currentPageCount;

        // Used as a flag to prevent starting multiple checkpoint threads simultaneously.
        atomic<int> _checkpointThreadBusy;

        // set of objects listening for checkpoints.
        mutex _checkpointListenerMutex;
        set<SQLite::CheckpointRequiredListener*> _checkpointListeners;
    };

    // We have designed this so that multiple threads can write to multiple journals simultaneously, but we want
    // monotonically increasing commit numbers, so we implement a lock around changing that value. This lock is wrapped
    // and publicly exposed only through 'g_commitLock'. This *should* be part of SharedData and specific to each file
    // we're using, but it isn't because it's externally referenced as a static class member, because we didn't used to
    // support multiple files here. This will cause a performance bottleneck if using multiple files, as they'll both
    // unnecessarily compete for the same commit lock. We also use this global lock for inserting and removing items in
    // _sharedDataLookupMap, and if we were to move this to being per-filename, we'd need a separate lock just for
    // _sharedDataLookupMap.
    static recursive_mutex _commitLock;

    // This map is how a new SQLite object can look up the existing state for the other SQLite objects sharing the same
    // database file. It's a map of canonicalized filename to a sharedData object.
    static map<string, SharedData*> _sharedDataLookupMap;

    // Pointer to our SharedData object. Having a pointer directly to the object avoids having to lock the lookup map
    // to access this memory.
    SharedData* _sharedData;

    // This is the callback function we use to log SQLite's internal errors.
    static void _sqliteLogCallback(void* pArg, int iErrCode, const char* zMsg);

    // Returns the name of a journal table based on it's index.
    string _getJournalTableName(int64_t journalTableID, bool create = false);

    // Attributes
    sqlite3* _db;
    string _filename;
    uint64_t _journalSize;
    uint64_t _maxJournalSize;
    bool _insideTransaction;
    string _uncommittedQuery;
    string _uncommittedHash;
    bool _uncommittedConcurrency;

    // The name of the journal table
    string _journalName;

    // Timing information.
    uint64_t _beginElapsed;
    uint64_t _readElapsed;
    uint64_t _writeElapsed;
    uint64_t _prepareElapsed;
    uint64_t _commitElapsed;
    uint64_t _rollbackElapsed;

    // We keep track of whether we've locked the global mutex so that we know whether or not we need to unlock it when
    // we call `rollback`.
    bool _mutexLocked;

    // Like getCommitCount(), but only callable internally, when we know for certain that we're not in the middle of
    // any transactions. Instead of reading from an atomic var, reads directly from the database.
    uint64_t _getCommitCount();

    bool _writeIdempotent(const string& query, bool alwaysKeepQueries = false);

    // Constructs a UNION query from a list of 'query parts' over each of our journal tables.
    // Fore each table, queryParts will be joined with that table's name as a separator. I.e., if you have a tables
    // named 'journal', 'journal00, and 'journal01', and queryParts of {"SELECT * FROM", "WHERE id > 1"}, we'll create
    // the following subqueries from query parts:
    //
    // "SELECT * FROM journal WHERE id > 1"
    // "SELECT * FROM journal00 WHERE id > 1"
    // "SELECT * FROM journal01 WHERE id > 1"
    //
    // And then we'll join then using UNION into:
    // "SELECT * FROM journal WHERE id > 1
    //  UNION
    //  SELECT * FROM journal00 WHERE id > 1
    //  UNION
    //  SELECT * FROM journal01 WHERE id > 1;"
    //
    //  Note that this wont work if you have a query like "SELECT * FROM journal", with no trailing WHERE clause, as we
    //  only insert the table name *between* adjacent entries in queryParts. We provide the 'append' flag to get around
    //  this limitation.
    string _getJournalQuery(const list<string>& queryParts, bool append = false);

    // Callback function that we'll register for authorizing queries in sqlite.
    static int _sqliteAuthorizerCallback(void*, int, const char*, const char*, const char*, const char*);

    // The following variables maintain the state required around automatically re-writing queries.

    // If true, we'll attempt query re-writing.
    bool _enableRewrite;

    // Pointer to the current handler to determine if a query needs to be rewritten.
    bool (*_rewriteHandler)(int, const char*, string&);

    // When the rewrite handler indicates a query needs to be re-written, the new query is set here.
    string _rewrittenQuery;

    // Causes the current query to skip re-write checking if it's already a re-written query.
    bool _currentlyRunningRewritten;

    // Callback to trace internal sqlite state (used for logging normalized queries).
    static int _sqliteTraceCallback(unsigned int traceCode, void* c, void* p, void* x);

    // Handles running checkpointing operations.
    static int _sqliteWALCallback(void* data, sqlite3* db, const char* dbName, int pageCount);

    // Callback function for progress tracking.
    static int _progressHandlerCallback(void* arg);
    uint64_t _timeoutLimit;
    uint64_t _timeoutStart;
    uint64_t _timeoutError;
    bool _abandonForCheckpoint;

    // Check out various error cases that can interrupt a query.
    // We check them all together because we need to make sure we atomically pick a single one to handle.
    void _checkInterruptErrors(const string& error);

    // Called internally by _sqliteAuthorizerCallback to authorize columns for a query.
    int _authorize(int actionCode, const char* detail1, const char* detail2, const char* detail3, const char* detail4);

    // It's possible for certain transactions (namely, timing out a write operation, see here:
    // https://sqlite.org/c3ref/interrupt.html) to cause a transaction to be automatically rolled back. If this
    // happens, we store a flag internally indicating that we don't need to perform the rollback ourselves. Then when
    // `rollback` is called, we don't double-rollback, generating an error. This allows the externally visible SQLite
    // API to be consistent and not have to handle this special case. Consumers can just always call `rollback` after a
    // failed query, regardless of whether or not it was already rolled back internally.
    bool _autoRolledBack;

    bool _noopUpdateMode;

    bool _enableFullCheckpoints;

    // This section enables caching of query results.

    // A map of queries to their cached results. This is populated only with deterministic queries, and is reset on any
    // write, rollback, or commit.
    map<string, SQResult> _queryCache;

    // Number of queries that have been attempted in this transaction (for metrics only).
    int64_t _queryCount;

    // Number of queries found in cache in this transaction (for metrics only).
    int64_t _cacheHits;

    // Set to true if the cache is in use for this transaction.
    bool _useCache;

    // A string indicating the name of the transaction (typically a command name) for metric purposes.
    string _transactionName;

    // Will be set to false while running a non-deterministic query to prevent it's result being cached.
    bool _isDeterministicQuery;

    bool _pageLoggingEnabled;
    static atomic<int64_t> _transactionAttemptCount;
    static mutex _pageLogMutex;
    int64_t _currentTransactionAttemptCount;

    // Copies of parameters used to initialize the DB that we store if we make child objects based on this one.
    int _cacheSize;
    const string _synchronous;
    int64_t _mmapSizeGB;
};

class SQLitePool {
  public:
    // Create a pool of DB handles.
    SQLitePool(size_t maxDBs, const string& filename, int cacheSize, bool enableFullCheckpoints, int maxJournalSize,
               int createJournalTables, const string& synchronous = "", int64_t mmapSizeGB = 0, bool pageLoggingEnabled = false);
    ~SQLitePool();

    // Get the base object (the first one created, which uses the `journal` table). Waits until this handle is
    // available if it isn't.
    SQLite& getBase();

    // Get any object except the base. Will wait for an available handle if there are already maxDBs.
    SQLite& get();

    // Return an object to the pool.
    void returnToPool(SQLite& object);

  private:
    // Synchronization variables.
    mutex _sync;
    condition_variable _wait;

    size_t _maxDBs;
    // Pointers to all of our objects.
    SQLite* _baseDB;
    set<SQLite*> _availableHandles;
    set<SQLite*> _inUseHandles;
};

class SQLitePoolScopedGet {
  public:
    SQLitePoolScopedGet(SQLitePool& pool) : _pool(pool), _db(_pool.get()) {} 
    ~SQLitePoolScopedGet() {
        _pool.returnToPool(_db);
    }
    SQLite& db() {
        return _db;
    }

  private:
    SQLitePool& _pool;
    SQLite& _db;
};
