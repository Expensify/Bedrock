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
    // Takes a list, where each item in the list is a pair.
    // each of those pairs is a maximum number of entries, and a vector. The vector is a set of table names that max applies to.
    SQLiteJournalDeleter(list<pair<size_t, vector<string>>>, SQLite& db);

    ~SQLiteJournalDeleter();

  private:
    sqlite3* _db;
    thread _deleteThread;
    void deleteEntries();
    atomic<bool> stop{false};
};
