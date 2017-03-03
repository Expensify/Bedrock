#pragma once
#include "SQLiteCommand.h"
class SQLite;

// SQLite core is an abstract command processor class.
class SQLiteCore {
  public:
    // Constructor that stores the database object we'll be working on.
    SQLiteCore(SQLite& db);

    // Call after process returns true to commit the command. Can return false if the commit results in a conflict.
    bool commitCommand(SQLiteCommand& command);

  protected:
    SQLite& _db;
};
