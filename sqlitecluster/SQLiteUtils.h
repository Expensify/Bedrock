#pragma once

class SQLiteUtils {
  public:
      /**
       * Gets the next ID available for random IDs on a given table and column
       *
       * @param db
       * @param tableName
       * @param column
       * @return the new ID
       */
      static int64_t getRandomID(SQLite& db, const string& tableName, const string& column);
};
