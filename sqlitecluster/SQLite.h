#pragma once
#include <libstuff/sqlite3.h>

// Convenience macro for locking our static commit lock.
#define SQLITE_COMMIT_AUTOLOCK SLockTimerGuard<decltype(SQLite::g_commitLock)> \
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

    // This publicly exposes our core mutex, allowing other classes to perform extra operations around commits and
    // such, when they determine that those operations must be made atomically with operations happening in SQLite.
    // This can be locked with the SQLITE_COMMIT_AUTOLOCK macro, as well.
    static SLockTimer<recursive_mutex> g_commitLock;

    // Loads a database and confirms its schema
    // The journalTable and numJournalTables parameters are maybe less than straightforward, here's what they mean:
    //
    // journalTable: this is the numerical id of the journalTable that this DB will *write* to. It can be -1 to
    //               indicate that the table to use is 'journal', otherwise it will be journalNNNN, where NNNN is the
    //               integer passed in. The actual table name use will always have at least four digits (leading 0s
    //               for numbers less than 1000).
    //
    // maxRequiredJournalTableID: This is the maximum journal table ID that we'll verify. If it's -1, we'll only verify
    //                            'journal' and no numbered tables.
    SQLite(const string& filename, int cacheSize, int autoCheckpoint, int maxJournalSize, int journalTable,
           int maxRequiredJournalTableID, const string& synchronous = "");
    ~SQLite();

    // Returns the canonicalized filename for this database
    string getFilename() { return _filename; }

    // Performs a read-only query (eg, SELECT). This can be done inside or outside a transaction. Returns true on
    // success, and fills the 'result' with the result of the query.
    bool read(const string& query, SQResult& result);

    // Performs a read-only query (eg, SELECT) that returns a single value.
    string read(const string& query);

    // Begins a new transaction. Returns true on success.
    bool beginTransaction();

    // Begins a new concurrent transaction. Returns true on success.
    bool beginConcurrentTransaction();

    // Verifies a table exists and has a particular definition. If the database is left with the right schema, it
    // returns true. If it had to create a new table (ie, the table was missing), it also sets created to true. If the
    // table is already there with the wrong schema, it returns false.
    bool verifyTable(const string& name, const string& sql, bool& created);

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
    bool getUpdateNoopMode();

    // Prepare to commit or rollback the transaction. This also inserts the current uncommitted query into the
    // journal; no additional writes are allowed until the next transaction has begun.
    bool prepare();

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

    // This atomically removes and returns committed transactions from our inflight list. SQLiteNode can call this, and
    // it will return a map of transaction IDs to pairs of (query, hash), so that those transactions can be replicated
    // out to peers.
    map<uint64_t, pair<string,string>> getCommittedTransactions();

    // The whitelist is either nullptr, in which case the feature is disabled, or it's a map of table names to sets of
    // column names that are allowed for reading. Using whitelist at all put the database handle into a more
    // restrictive access mode that will deny access for write operations and other potentially risky operations, even
    // in the case that a specific table/column are not being directly requested.
    map<string, set<string>>* whitelist;

  private:

    // This structure contains all of the data that's shared between a set of SQLite objects that share the same
    // underlying database file.
    struct SharedData {
        // This is the last committed hash by *any* thread for this file.
        atomic<string> _lastCommittedHash;

        // This is a set of transactions IDs that have been successfully committed to the database, but not yet sent to
        // peers.
        set<uint64_t> _committedTransactionIDs;

        // The current commit count, loaded at initialization from the highest commit ID in the DB, and then accessed
        // though this atomic integer. getCommitCount() returns the value of this variable.
        atomic<uint64_t> _commitCount;

        // Names of journal tables for this database.
        list<string> _journalNames;

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
        map<uint64_t, pair<string, string>> _inFlightTransactions;
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
    // database file. It's a map of canonicalized filename to a reference count and a sharedData object.
    static map<string, pair<int, SharedData*>> _sharedDataLookupMap; 

    // Pointer to our SharedData object. Having a pointer directly to the object avoids having to lock the lookup map
    // to access this memory.
    SharedData* _sharedData;

    // This is the callback function we use to log SQLite's internal errors.
    static void _sqliteLogCallback(void* pArg, int iErrCode, const char* zMsg);

    // Returns the name of a journal table based on it's index.
    static string _getJournalTableName(int journalTableID);

    // Attributes
    sqlite3* _db;
    string _filename;
    uint64_t _journalSize;
    uint64_t _maxJournalSize;
    bool _insideTransaction;
    string _uncommittedQuery;
    string _uncommittedHash;

    // The name of the journal table, computed from the 'journalTable' parameter passed to our constructor.
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

    // Callback function for progress tracking.
    static int _progressHandlerCallback(void* arg);
    uint64_t _timeoutLimit;
    uint64_t _timeoutStart;
    uint64_t _timeoutError;

    // Check the timing of the current query and throw if the limit's exceeded.
    void _checkTiming(const string& error);

    // Called internally by _sqliteAuthorizerCallback to authorize columns for a query.
    int _authorize(int actionCode, const char* table, const char* column);

    // It's possible for certain transactions (namely, timing out a write operation, see here:
    // https://sqlite.org/c3ref/interrupt.html) to cause a transaction to be automatically rolled back. If this
    // happens, we store a flag internally indicating that we don't need to perform the rollback ourselves. Then when
    // `rollback` is called, we don't double-rollback, generating an error. This allows the externally visible SQLite
    // API to be consistent and not have to handle this special case. Consumers can just always call `rollback` after a
    // failed query, regardless of whether or not it was already rolled back internally.
    bool _autoRolledBack;

    bool _noopUpdateMode;
};
