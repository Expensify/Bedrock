// SQLite.h
#pragma once
#include <libstuff/sqlite3.h>

// SQLite
class SQLite {
  public: // External API
    // Loads a database and confirms its schema
    SQLite(const string& filename, int cacheSize, int autoCheckpoint, bool readOnly, int maxJournalSize);
    virtual ~SQLite();

    // Returns the filename for this database
    string getFilename() { return _filename; }

    // Performs a read-only query (eg, SELECT).  This can be done inside
    // or outside a transaction.  Returns true on success, and fills the
    // 'result' with the result of the query.
    bool read(const string& query, SQResult& result);

    // Performs a read-only query (eg, SELECT) that returns a single cell.
    string read(const string& query);

    // Begins a new transaction.  Returns true on success.
    bool beginTransaction();

    // Verifies a table exists and has a particular definition.  If the database
    // is left with the right schema, it returns true.  If it had to create a
    // new table (ie, the table was missing), it also sets created to true.  If
    // the table is already there with the wrong schema, it returns false.
    bool verifyTable(const string& name, const string& sql, bool& created);

    // Adds a column to a table.
    bool addColumn(const string& tableName, const string& column, const string& columnType);

    // Performs a read/write query (eg, INSERT, UPDATE, DELETE).  This
    // is added to the current transaction's query list.  Returns true
    // on success.
    bool write(const string& query);

    // Prepare to commit or rollback the transaction.  This also inserts
    // the current uncommitted query into the journal; no additional writes
    // are allowed until the next transaction has begun.
    bool prepare();

    // Commits the current transaction to disk
    void commit();

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

    // Returns the number of commits on this database.
    uint64_t getCommitCount();

    // Returns the current state of the database, as a SHA1
    // hash of all queries committed.
    string getCommittedHash() { return _committedHash; }

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

  protected: // Internal API
    sqlite3* _db;
    uint64_t _commitCount;

  private:
    // Attributes
    string _filename;
    uint64_t _journalSize;
    uint64_t _maxJournalSize;
    string _committedHash;
    bool _insideTransaction;
    string _uncommittedQuery;
    string _uncommittedHash;
    bool _lastWriteChanged;
    bool _readOnly;
    uint64_t _beginElapsed;
    uint64_t _readElapsed;
    uint64_t _writeElapsed;
    uint64_t _prepareElapsed;
    uint64_t _commitElapsed;
    uint64_t _rollbackElapsed;

    // Sets all the pragmas we want on the provided db handle.
    void _setPragmas(sqlite3* db, int autoCheckpoint);

    // You're only supposed to configure SQLite options before initializing the
    // library, but the library provides no
    // way to check if it's been initialized, so we store our own value here.
    static bool sqliteInitialized;
};
