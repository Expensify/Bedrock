#pragma once

class SQLiteUtils {
  public:
       // Generates a random ID and checks the given tableName and column to ensure
       // uniqueness.
      static int64_t getRandomID(SQLite& db, const string& tableName, const string& column);
};
