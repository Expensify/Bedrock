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

    enum TIMING_INFO {
        PEEK,
        PROCESS,
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

    // Add a summary of our timing info to our response object.
    void finalizeTimingInfo();

    // If the `peek` portion of this command needs to make an HTTPS request, this is where we store it.
    SHTTPSManager::Transaction* httpsRequest;

    // Each command is assigned a priority.
    Priority priority;

    // We track how many times we `peek` and `process` each command.
    int peekCount;
    int processCount;

    // A list of timing sets, with an info type, start, and end.
    list<tuple<TIMING_INFO, uint64_t, uint64_t>> timingInfo;

  private:
    // Set certain initial state on construction. Common functionality to several constructors.
    void _init();
};
