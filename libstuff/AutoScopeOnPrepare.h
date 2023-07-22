#pragma once
#include "../SQLiteCluster/SQLite.h"
using namespace std;

// RAII-style mechanism for automatically setting and unsetting an on prepare handler
class AutoScopeOnPrepare {
  public:
    AutoScopeOnPrepare(bool enable, SQLite& db, void (*handler)(SQLite& _db, int64_t tableID));
    ~AutoScopeOnPrepare();

  private:
    bool _enable;
    SQLite& _db;
    void (*_handler)(SQLite& _db, int64_t tableID);
};