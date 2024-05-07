#pragma once
#include <atomic>
#include <list>
#include <string>
#include <thread>
#include <vector>

using namespace std;

class SQLite;
class sqlite3;

class SQLiteJournalDeleter {
  public:
    typedef list<pair<size_t, vector<string>>> TableLimits;
    // Takes a list, where each item in the list is a pair.
    // each of those pairs is a maximum number of entries, and a vector. The vector is a set of table names that max applies to.
    SQLiteJournalDeleter(TableLimits limits, SQLite& db);

    ~SQLiteJournalDeleter();

  private:
    sqlite3* _db;
    const TableLimits limits;
    thread _deleteThread;
    void deleteEntries();
    atomic<bool> stop{false};
};
