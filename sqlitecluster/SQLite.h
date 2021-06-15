#pragma once
#include <libstuff/sqlite3.h>
#include <libstuff/SPerformanceTimer.h>

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

    // We can get a SQLITE_CONSTRAINT error in a write command for two reasons. One is a legitimate error caused
    // by a user trying to insert two rows with the same key. The other is in multi-threaded replication, when
    // transactions start in a different order on a follower than they did on the leader. Consider this example case:
    // CREATE TABLE t (identifier PRIMARY KEY);
    //
    // With the start state on all nodes:
    // INSERT INTO t VALUES(1);
    //
    // If you run these two commands in this order:
    // DELETE FROM t WHERE identifier = 1;
    // INSERT INTO t VALUES(1);
    //
    // They will work just fine. If you run them simultaneously, you might expect that they'd conflict, but they don't
    // because the unique constraints error doesn't get thrown at commit, it gets thrown at the time the `INSERT`
    // command tries to run but the `DELETE` hasn't completed yet. Re-running the `INSERT` after the `DELETE` will work
    // as expected.
    //
    // If we detect this error, we throw the following exception, which just returns an error when encountered in a
    // normal `process` phase of a command on leader, but is treated similarly to a commit conflict in replication, and
    // re-runs the command after the other command (the `DELETE`) has finished.
    class constraint_error : public exception {
      public :
        constraint_error() {};
        virtual ~constraint_error() {};
        const char* what() const noexcept { return "constraint_error"; }
    };

    // Constant to use like a sqlite result code when commits are disabled (see: https://www.sqlite.org/rescode.html)
    // Because the existing codes all use values in the first and second bytes of the int (they're or'ed with values
    // left shifted by 8 bits, see SQLITE_ERROR_MISSING_COLLSEQ in sqlite.h for an example), we left shift by 16 for
    // this to avoid any overlap.
    static const int COMMIT_DISABLED = (1 << 16) | 1;

    // Abstract base class for objects that need to be notified when we set `checkpointRequired` and then when that
    // checkpoint is complete. Why do we need to notify anyone that we're going to do a checkpoint? Because restart
    // checkpoints can't run simultaneously with any other transactions, and thus will block new transactions and wait
    // for all current transactions to finish, but all current transactions may not finish gracefully on their own, and
    // may need to be interrupted by another mechanism. This allows an object with this behavior (blocking during a
    // transaction) to be notified that it needs to either finish or abandon the transaction.
    class CheckpointRequiredListener {
      public:
        virtual void checkpointRequired() = 0;
        virtual void checkpointComplete() = 0;
    };

    // minJournalTables: Creates journal tables through the specified number. If `-1` is passed, only `journal` is
    //                   created. If some value larger than -1 is passed, then journals `journal0000 through
    //                   journalNNNN` are created (or left alone if such tables already exist). If -2 or less is
    //                   passed, no tables are created.
    //
    // mmapSizeGB: address space to use for memory-mapped IO, in GB.
    SQLite(const string& filename, int cacheSize, int maxJournalSize, int minJournalTables,
           const string& synchronous = "", int64_t mmapSizeGB = 0, bool pageLoggingEnabled = false);

    // Compatibility constructor. Remove when AuthTester::getStripeSQLiteDB no longer uses this outdated version.
    SQLite(const string& filename, int cacheSize, int maxJournalSize, int minJournalTables, int synchronous) :
        SQLite(filename, cacheSize, maxJournalSize, minJournalTables, "") {}
    
    // This constructor is not exactly a copy constructor. It creates an other SQLite object based on the first except
    // with a *different* journal table. This avoids a lot of locking around creating structures that we know already
    // exist because we already have a SQLite object for this file.
    SQLite(const SQLite& from);
    ~SQLite();

    // Returns the canonicalized filename for this database
    const string& getFilename() { return _filename; }

    // Performs a read-only query (eg, SELECT). This can be done inside or outside a transaction. Returns true on
    // success, and fills the 'result' with the result of the query.
    bool read(const string& query, SQResult& result);

    // Performs a read-only query (eg, SELECT) that returns a single value.
    string read(const string& query);

    // Types of transactions that we can begin.
    enum class TRANSACTION_TYPE {
        SHARED,
        EXCLUSIVE
    };

    // Begins a new transaction. Returns true on success. If type is EXCLUSIVE, locks the commit mutex to guarantee
    // that this transaction cannot conflict with any others.
    bool beginTransaction(TRANSACTION_TYPE type = TRANSACTION_TYPE::SHARED);

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
    int commit(const string& description = "UNSPECIFIED");

    // Cancels the current transaction and rolls it back.
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

    // A static version of the above that can be used in initializers.
    static bool getCommit(sqlite3* db, const vector<string> journalNames, uint64_t index, string& query, string& hash);

    // Looks up a range of commits.
    bool getCommits(uint64_t fromIndex, uint64_t toIndex, SQResult& result);

    // Start a timing operation, that will time out after the given number of microseconds.
    void startTiming(uint64_t timeLimitUS);

    // Reset timing after finishing a timed operation.
    void resetTiming();

    // Register and deregister listeners for checkpoint operations. See the comments on `CheckpointRequiredListener`
    // above for why checkpoint listeners are useful.
    void addCheckpointListener(CheckpointRequiredListener& listener);
    void removeCheckpointListener(CheckpointRequiredListener& listener);

    // This atomically removes and returns committed transactions from our internal list. SQLiteNode can call this, and
    // it will return a map of transaction IDs to tuples of (query, hash, dbCountAtTransactionStart), so that those
    // transactions can be replicated out to peers.
    map<uint64_t, tuple<string,string, uint64_t>> popCommittedTransactions();

    // The whitelist is either nullptr, in which case the feature is disabled, or it's a map of table names to sets of
    // column names that are allowed for reading. Using whitelist at all put the database handle into a more
    // restrictive access mode that will deny access for write operations and other potentially risky operations, even
    // in the case that a specific table/column are not being directly requested.
    map<string, set<string>>* whitelist = nullptr;

    // Call before starting a transaction to make sure we don't interrupt a checkpoint operation.
    void waitForCheckpoint();

    // These are the minimum thresholds for the WAL file, in pages, that will cause us to trigger either a full or
    // passive checkpoint. They're public, non-const, and atomic so that they can be configured on the fly.
    static atomic<int> passiveCheckpointPageMin;
    static atomic<int> fullCheckpointPageMin;

    // Enable/disable SQL statement tracing.
    static atomic<bool> enableTrace;

    // Calling this before starting a transaction will prevent the next transaction from being interrupted by a restart
    // checkpoint and restarting. This causes a potential performance issue so only do this if it's *really important*
    // that this transaction isn't interrupted. The primary reason for adding this was to enable slow but very
    // infrequent transactions to complete, even though they take longer than the typical interval between restart
    // checkpoints to complete, thus causing an endless cycle of interrupted transactions.
    void disableCheckpointInterruptForNextTransaction() { _enableCheckpointInterrupt = false; }

    // public read-only accessor for _dbCountAtStart.
    uint64_t getDBCountAtStart() const;

    // This is the callback function we use to log SQLite's internal errors.
    static void _sqliteLogCallback(void* pArg, int iErrCode, const char* zMsg);

    // If commits are disabled, calling commit() will return an error without committing. This can be used to guarantee
    // no commits can happen "late" from slow threads that could otherwise write to a DB being shutdown.
    void setCommitEnabled(bool enable);

  private:
    // This structure contains all of the data that's shared between a set of SQLite objects that share the same
    // underlying database file.
    class SharedData {
      public:
        // Constructor.
        SharedData();

        // Add and remove and call checkpoint listeners in a thread-safe way.
        void addCheckpointListener(CheckpointRequiredListener& listener);
        void removeCheckpointListener(CheckpointRequiredListener& listener);
        void checkpointRequired();
        void checkpointComplete();

        // Enable or disable commits for the DB.
        void setCommitEnabled(bool enable);

        // Update the shared state of the DB to include the newest commit with the newest hash. This needs to be done
        // after completing a commit and before releasing the commit lock.
        void incrementCommit(const string& commitHash);

        // This removes and returns all committed transactions.
        map<uint64_t, tuple<string, string, uint64_t>> popCommittedTransactions();

        // This is the last committed hash by *any* thread for this file.
        atomic<string> lastCommittedHash;

        // An identifier used to choose the next journal table to use with this set of DB handles. Only used to
        // initialize new objects.
        atomic<int64_t> nextJournalCount;

        // When `SQLite::prepare` is called, we need to save a set of info that will be broadcast to peers when the
        // transaction is ultimately committed. This should be cleared out if the transaction is rolled back.
        void prepareTransactionInfo(uint64_t commitID, const string& query, const string& hash, uint64_t dbCountAtTransactionStart);

        // When a transaction that was prepared is committed, we move the data from the prepared list to the committed
        // list.
        void commitTransactionInfo(uint64_t commitID);

        // The current commit count, loaded at initialization from the highest commit ID in the DB, and then accessed
        // though this atomic integer. getCommitCount() returns the value of this variable.
        atomic<uint64_t> commitCount;

        // Mutex to serialize commits to this DB. This should be locked anytime a thread needs to commit to the DB, or
        // needs to prevent other threads from committing to the DB (such as to guarantee there are no commit conflicts
        // during a transaction).
        recursive_mutex commitLock;

        // This mutex prevents any thread starting a new transaction when locked. The checkpoint thread will lock it
        // when required to make sure it can get exclusive use of the DB.
        shared_timed_mutex blockNewTransactionsMutex;

        // These three variables let us notify the checkpoint thread when a transaction ends (or starts, but it will
        // have blocked any new ones from starting by locking blockNewTransactionsMutex).
        mutex notifyWaitMutex;
        condition_variable blockNewTransactionsCV;
        atomic<int> currentTransactionCount;

        // TODO: These should be private or renamed.
        // This is the count of current pages waiting to be check pointed. This potentially changes with every wal callback
        // we need to store it across callbacks so we can check if the full check point thread still needs to run.
        atomic<int> _currentPageCount;


        // Used as a flag to prevent starting multiple checkpoint threads simultaneously.
        atomic<int> _checkpointThreadBusy;

        // If set to false, this prevents any thread from being able to commit to the DB.
        atomic<bool> _commitEnabled;

        SPerformanceTimer _commitLockTimer;

        // We keep track of the commitCount at each complete checkpoint, so that we can throttle the frequency of
        // checkpoints to not more often than every N commits.
        atomic<uint64_t> lastCompleteCheckpointCommitCount;

      private:
        // The data required to replicate transactions, in two lists, depending on whether this has only been prepared
        // or if it's been committed.
        map<uint64_t, tuple<string, string, uint64_t>> _preparedTransactions;
        map<uint64_t, tuple<string, string, uint64_t>> _committedTransactions;

        // set of objects listening for checkpoints.
        set<SQLite::CheckpointRequiredListener*> _checkpointListeners;
        
        // This mutex is locked when we need to change the state of the _shareData object. It is shared between a
        // variety of operations (i.e., inserting checkpoint listeners, updating _committedTransactions, etc.
        recursive_mutex _internalStateMutex;
    };

    // Initializers to support RAII-style allocation in constructors.
    static string initializeFilename(const string& filename);
    static SharedData& initializeSharedData(sqlite3* db, const string& filename, const vector<string>& journalNames);
    static sqlite3* initializeDB(const string& filename, int64_t mmapSizeGB);
    static vector<string> initializeJournal(sqlite3* db, int minJournalTables);
    static uint64_t initializeJournalSize(sqlite3* db, const vector<string>& journalNames);
    void commonConstructorInitialization();

    // The filename of this DB, canonicalized to its full path on disk.
    const string _filename;

    // The maximum number of rows to store in the journal before we start truncating old ones.
    uint64_t _maxJournalSize;

    // The underlying sqlite3 DB handle.
    sqlite3* _db;

    // Names of ALL journal tables for this database.
    const vector<string> _journalNames;

    // Pointer to our SharedData object, which is shared between all SQLite DB objects for the same file.
    SharedData& _sharedData;

    // The name of the journal table that this particular DB handle with write to.
    const string _journalName;

    // The current size of the journal, in rows. TODO: Why isn't this in SharedData?
    uint64_t _journalSize;

    // True when we have a transaction in progress.
    bool _insideTransaction = false;

    // The new query and new hash to add to the journal for a transaction that's nearing completion, before we commit
    // it.
    string _uncommittedQuery;
    string _uncommittedHash;

    // Returns the name of a journal table based on it's index.
    static string getJournalTableName(vector<string>& journalNames, int64_t journalTableID, bool create = false);

    // The latest transaction ID at the start of the current transaction (note: it is allowed for this to be *higher*
    // than the state inside the transaction, if another thread committed to the DB while we were in
    // `beginTransaction`).
    uint64_t _dbCountAtStart = 0;

    // Timing information.
    uint64_t _beginElapsed = 0;
    uint64_t _readElapsed = 0;
    uint64_t _writeElapsed = 0;
    uint64_t _prepareElapsed = 0;
    uint64_t _commitElapsed = 0;
    uint64_t _rollbackElapsed = 0;

    // We keep track of whether we've locked the global mutex so that we know whether or not we need to unlock it when
    // we call `rollback`. Note that this indicates whether this object has locked the mutex, not whether the mutex is
    // locked (i.e., this is `false` if some other DB object has locked the mutex).
    bool _mutexLocked = false;

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

    // Static version for initializers.
    static string _getJournalQuery(const vector<string>& journalNames, const list<string>& queryParts, bool append = false);

    // Callback function that we'll register for authorizing queries in sqlite.
    static int _sqliteAuthorizerCallback(void*, int, const char*, const char*, const char*, const char*);

    // The following variables maintain the state required around automatically re-writing queries.

    // If true, we'll attempt query re-writing.
    bool _enableRewrite = false;

    // Pointer to the current handler to determine if a query needs to be rewritten.
    bool (*_rewriteHandler)(int, const char*, string&);

    // When the rewrite handler indicates a query needs to be re-written, the new query is set here.
    string _rewrittenQuery;

    // Causes the current query to skip re-write checking if it's already a re-written query.
    bool _currentlyRunningRewritten = false;

    // Callback to trace internal sqlite state (used for logging normalized queries).
    static int _sqliteTraceCallback(unsigned int traceCode, void* c, void* p, void* x);

    // Handles running checkpointing operations.
    static int _sqliteWALCallback(void* data, sqlite3* db, const char* dbName, int pageCount);

    // Callback function for progress tracking.
    static int _progressHandlerCallback(void* arg);
    uint64_t _timeoutLimit = 0;
    uint64_t _timeoutStart;
    uint64_t _timeoutError;
    bool _abandonForCheckpoint = false;
    bool _enableCheckpointInterrupt = true;

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
    bool _autoRolledBack = false;

    bool _noopUpdateMode = false;

    // A map of queries to their cached results. This is populated only with deterministic queries, and is reset on any
    // write, rollback, or commit.
    map<string, SQResult> _queryCache;

    // Number of queries that have been attempted in this transaction (for metrics only).
    int64_t _queryCount = 0;

    // Number of queries found in cache in this transaction (for metrics only).
    int64_t _cacheHits = 0;

    // A string indicating the name of the transaction (typically a command name) for metric purposes.
    string _transactionName;

    // Will be set to false while running a non-deterministic query to prevent it's result being cached.
    bool _isDeterministicQuery = false;

    bool _pageLoggingEnabled;
    static atomic<int64_t> _transactionAttemptCount;
    static mutex _pageLogMutex;
    int64_t _currentTransactionAttemptCount = -1;

    // Copies of parameters used to initialize the DB that we store if we make child objects based on this one.
    int _cacheSize;
    const string _synchronous;
    int64_t _mmapSizeGB;

    // This is a bit of a weird construct. We lock this in the destructor for an SQLite object because we spawn a
    // separate thread to do checkpoints, and that thread needs this object to exist until it finishes, so we lock
    // until that thread completes. This can go away when we no longer have dedicated checkpoint threads.
    mutex _destructorMutex;
};
