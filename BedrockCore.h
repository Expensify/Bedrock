#pragma once
#include <sqlitecluster/SQLiteCore.h>
class BedrockCommand;

class BedrockCore : public SQLiteCore {
  public:
    BedrockCore(SQLite& db);

    // Implements the abstract API of SQLiteCore.
    bool peekCommand(SQLiteCommand& command);
    bool processCommand(SQLiteCommand& command);

    // Lets plugins upgrade the database to conform to whatever schema they require.
    void upgradeDatabase();

  private:
    void _handleCommandException(BedrockCommand& command, const string& e, bool wasProcessing);
};
