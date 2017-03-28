#pragma once
#include <libstuff/sqlite3.h>

// Convenience macro for locking our static commit lock.
#define SQLITE_COMMIT_AUTOLOCK SLockTimerGuard<decltype(SQLite::commitLock)> __SSQLITEAUTOLOCK_##__LINE__(SQLite::commitLock)

class SQLite {
  public:
    // Loads a database and confirms its schema
    // The journalTable and numJournalTables parameters are maybe less than straightforward, here's what they mean:
    // journalTable: this is the numerical id of the journalTable that this DB will *write* to. It can be -1 to
    // indicate that the table to use is 'journal', otherwise it will be journalNN, where NN is the integer passed in.
    // The actual table name use will always have at least two digits (leading 0 for numbers less than 10).
    // minJournalTables: This will make sure at least this many journal tables exist. The format of this number is the
    // same as for journalTable - i.e., -1 verifies only that 'journal' exists.
    SQLite(const string& filename, int cacheSize, int autoCheckpoint, int maxJournalSize, int journalTable,
           int minJournalTables);
    virtual ~SQLite();

    // Returns the filename for this database
    string getFilename() { return _filename; }

    // Performs a read-only query (eg, SELECT). This can be done inside or outside a transaction. Returns true on
    // success, and fills the 'result' with the result of the query.
    bool read(const string& query, SQResult& result);

    // Performs a read-only query (eg, SELECT) that returns a single cell.
    string read(const string& query);

    // Begins a new transaction. Returns true on success.
    bool beginTransaction();

    // Begins a new concurrent transaction. Returns true on success.
    bool beginConcurrentTransaction();

    // This publicly exposes our core mutex, allowing other classes to perform extra operations around commits and
    // such, when they determine that those operations must be made atomically with operations happening in SQLite.
    static SLockTimer<recursive_mutex> commitLock;

    // Verifies a table exists and has a particular definition. If the database is left with the right schema, it
    // returns true. If it had to create a new table (ie, the table was missing), it also sets created to true. If the
    // table is already there with the wrong schema, it returns false.
    bool verifyTable(const string& name, const string& sql, bool& created);

    // Adds a column to a table.
    bool addColumn(const string& tableName, const string& column, const string& columnType);

    // Performs a read/write query (eg, INSERT, UPDATE, DELETE). This is added to the current transaction's query list.
    // Returns true  on success.
    bool write(const string& query);

    // Prepare to commit or rollback the transaction. This also inserts the current uncommitted query into the
    // journal; no additional writes are allowed until the next transaction has begun.
    bool prepare();

    // Commits the current transaction to disk. Returns an SQLite result code.
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

    // Return whether the last write changed anything
    bool lastWriteChanged() { return _lastWriteChanged; }

    // Like getCommitCount, but return query and hash.
    void getLatestCommit(uint64_t& id, string& query, string& hash);

    // Returns the number of commits on this database.
    uint64_t getCommitCount();

    // Returns the current state of the database, as a SHA1
    // hash of all queries committed.
    string getCommittedHash();

    // Returns what the new state will be of the database if
    // the current transaction is committed
    string getUncommittedHash() { return _uncommittedHash; }

    // Returns a concatenated string containing all the 'write' queries
    // executed within the current, uncommitted transaction.
    string getUncommittedQuery() { return _uncommittedQuery; }

    // Gets the ROWID of the last insertion (for auto-increment indexes)
    int64_t getLastInsertRowID();

    // Gets any error message associated with the previous query
    string getLastError() { return sqlite3_errmsg(_db); }

    // Returns true if we're inside an uncommitted transaction.
    bool insideTransaction() { return _insideTransaction; }

    // Looks up the exact SQL of a paricular commit to the
    // database, as well as gets the SHA1 hash of the database
    // immediately following tha commit.
    bool getCommit(uint64_t index, string& query, string& hash);

    // Looks up a range of commits
    bool getCommits(uint64_t fromIndex, uint64_t toIndex, SQResult& result);

    // This is the callback function we use to log SQLite's internal errors.
    static void sqliteLogCallback(void* pArg, int iErrCode, const char* zMsg);

    // Constructs a UNION query from a list of 'query parts' over each of our journal tables.
    // Fore each table, queryParts will be joined with that table's name as a separator. I.e., if you have a tables
    // named 'journal', 'journal00, and 'journal00', and queryParts of {"SELECT * FROM", "WHERE id > 1"}, we'll create
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
    string getJournalQuery(const list<string>& queryParts, bool append = false);

    // This atomically removes and returns committed transactions from our inflight list. SQLiteNode can call this, and
    // it will return a map of transaction IDs to pairs of (query, hash), so that those transactions can be replicated
    // out to peers.
    map<uint64_t, pair<string,string>> getCommittedTransactions();

  protected:
    sqlite3* _db;

  private:
    // Attributes
    string _filename;
    uint64_t _journalSize;
    uint64_t _maxJournalSize;
    bool _insideTransaction;

    uint64_t _uncommittedID = 0;
    string _uncommittedQuery;
    string _uncommittedHash;

    // This is the last committed hash by *any* thread.
    static atomic<string> _lastCommittedHash;

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

    // NOTE: Both of the following collections (_inFlightTransactions and _committedtransactionIDs) are shared between
    // all threads and need to be accessed in a synchronized fashion. They do *NOT* implement their own synchronization
    // and must be protected by locking `_commitLock`.

    // This is a map of all currently "in flight" transactions. These are transactions for which a `prepare()` has been
    // called to generate a journal row, but have not yet been sent to peers.
    static map<uint64_t, pair<string, string>> _inFlightTransactions;

    // This is a set of transactions IDs that have been successfully committed to the database, but not yet sent to
    // peers.
    static set<uint64_t> _committedtransactionIDs;

    // This is true when we have a transaction that we haven't committed, but does show up in the journal table. This
    // lets us report an accurate commit count, because sometimes it's one less than the highest entry in the table.
    // TODO: We might be able to re-use a different var for this purpose.
    bool _uncommittedTransaction = false;
    bool _lastWriteChanged;
    uint64_t _beginElapsed;
    uint64_t _readElapsed;
    uint64_t _writeElapsed;
    uint64_t _prepareElapsed;
    uint64_t _commitElapsed;
    uint64_t _rollbackElapsed;

    // The name of the journal table, computed from the 'journalTable' parameter passed to our constructor.
    string _journalName;

    // A list of all the journal tables names.
    list<string> _allJournalNames;

    // Sets all the pragmas we want on the provided db handle.
    void _setPragmas(sqlite3* db, int autoCheckpoint);

    // You're only supposed to configure SQLite options before initializing the library, but the library provides no
    // way to check if it's been initialized, so we store our own value here.
    static bool sqliteInitialized;

    static string getJournalTableName(int journalTableID);

    // We have designed this so that multiple threads can write to multiple journals simultaneously, but we want
    // monotonically increasing commit numbers, so we implement a lock around changing that value.
    static recursive_mutex _commitLock;

    // Like getCommitCount(), but only callable internally, when we know for certain that we're not in the middle of
    // any transactions.
    uint64_t _getCommitCount();

    // Static atomic commit count. Any thread can read or update this.
    static atomic<uint64_t> _commitCount;

    static atomic<int> _dbInitialized;

};
