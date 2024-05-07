#include "libstuff/sqlite3.h"
#include "SQLiteJournalDeleter.h"
#include "sqlitecluster/SQLite.h"

SQLiteJournalDeleter::SQLiteJournalDeleter(SQLiteJournalDeleter::TableLimits limits, SQLite& db) :
  limits(limits) {
    int result = sqlite3_open_v2(db.getFilename().c_str(), &_db, SQLITE_OPEN_READWRITE, 0);
    if (result != SQLITE_OK) {
        STHROW("500 Couldn't open reader DB");
    }

    _deleteThread = thread(&SQLiteJournalDeleter::deleteEntries, this);
}

SQLiteJournalDeleter::~SQLiteJournalDeleter() {
    stop = true;
    _deleteThread.join();
}

void SQLiteJournalDeleter::deleteEntries() {
    SInitialize("cleanup");
    while (!stop) {
        SINFO("Deleting.");
        sleep(1);
    }
}
