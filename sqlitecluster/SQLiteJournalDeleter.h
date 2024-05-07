#include <list>
#include <string>
using namepsace std;
class SQLite;
class sqlite3;

class SQLiteDeleter {
    // Takes a list, where each item in the list is a pair.
    // each of those pairs is a maximum number of entries, and another list. The list is a set of tablenames that max applies to.
    SQLiteDeleter(list<pair<size_t, list<string>>, SQLite& db);

    ~SQLiteDeleter();

  private:
    sqlite3* _db;
    thread _deleteThread;
    void deleteEntries();
    atomic<bool> stop{false};
};
