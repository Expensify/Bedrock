#include "SQLiteDeleter.h"

SQLiteDeleter::SQLiteDeleter(list<pair<size_t, list<string>>, SQLite& db) {
    int result = sqlite3_open_v2(dbFilename.c_str(), &_db, SQLITE_OPEN_READWRITE, 0);
    if (result != SQLITE_OK) {
        STHROW("500 Couldn't open reader DB");
    }

    _deleteThread = thread(SQLiteDeleter::deleteEntries, this);
}

SQLiteDeleter::~SQLiteDeleter() {
    stop = true;
    _deleteThread.wait();
}

void SQLiteDeleter::deleteEntries() {
    SInitialize("cleanup");
    while (!stop) {
        SINFO("Deleting.");
        sleep(1);
    }
}
