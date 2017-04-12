#pragma once
#include <sqlitecluster/SQLiteCommand.h>

class BedrockCommand : public SQLiteCommand {
  public:
    enum Priority {
        PRIORITY_MIN = 0,
        PRIORITY_LOW = 250,
        PRIORITY_NORMAL = 500,
        PRIORITY_HIGH = 750,
        PRIORITY_MAX = 1000
    };

    // Constructor to make an empty object.
    BedrockCommand();

    // Constructor to convert from an existing SQLiteCommand (by move).
    BedrockCommand(SQLiteCommand&& from);

    // Move constructor.
    BedrockCommand(BedrockCommand&& from);

    // Constructor to initialize via a request object (by move).
    BedrockCommand(SData&& _request);

    // Constructor to initialize via a request object (by copy).
    BedrockCommand(SData _request);

    // Destructor.
    ~BedrockCommand();

    // Move assignment operator.
    BedrockCommand& operator=(BedrockCommand&& from);

    // If the `peek` portion of this command needs to make an HTTPS request, this is where we store it.
    SHTTPSManager::Transaction* httpsRequest;

    // Each command is assigned a priority.
    Priority priority;

    // We track how many times we `peek` and `process` each command.
    int peekCount;
    int processCount;

  private:
    // Set certain initial state on construction. Common functionality to several constructors.
    void _init();
};
