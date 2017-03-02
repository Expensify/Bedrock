#pragma once
#include <sqlitecluster/SQLiteCommand.h>

class BedrockCommand : public SQLiteCommand {
  public:
    // Constructor to convert from an existing SQLiteCommand;
    BedrockCommand(SQLiteCommand &&);

    // Constructor to make an empty object.
    BedrockCommand();

    // Move assignment operator.
    BedrockCommand& operator=(BedrockCommand&& from);

    //Default move constructor.
    BedrockCommand (BedrockCommand &&) = default;

    // We add a field for an HTTPS request on top of the base interface.
    SHTTPSManager::Transaction* httpsRequest;
};
