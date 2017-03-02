#pragma once
#include "SQLiteNode.h"

class SQLiteCommand {
  public:
    enum Priority {
        PRIORITY_MIN = 0,
        PRIORITY_LOW = 250,
        PRIORITY_NORMAL = 500,
        PRIORITY_HIGH = 750,
        PRIORITY_MAX = 1000
    };

    // Attributes
    SQLiteNode::Peer* initiator;
    string id;
    SData transaction;  // Used inside SQLiteNode
    SData request;      // Original request
    STable jsonContent; // Accumulated response content
    SData response;     // Final response
    Priority priority;
    uint64_t creationTimestamp;
    uint64_t replicationStartTimestamp;
    uint64_t processingTime;
    SQLiteNode::ConsistencyLevel writeConsistency;
    bool complete;

    // Keep track of some state as we go through everything that needs to be done here.
    int peekCount;
    int processCount;

    // No longer the responsibility of SQLiteNode, goes to Server.
    // **NOTE: httpsRequest is used to store a pointer to a
    //         secondary SHTTPSManager request; this can be
    //         initiated in _peekCommand(), and the command won't
    //         be processed by _processCommand() until the request
    //         has completed.
    // SHTTPSManager::Transaction* httpsRequest;

    // Default Constructor.
    SQLiteCommand();

    // Move constructor.
    SQLiteCommand(SQLiteCommand&& from) = default;

    // Default move assignment operator.
    SQLiteCommand& operator=(SQLiteCommand&& from) = default;

    // Destructor.
    ~SQLiteCommand() {}
};
