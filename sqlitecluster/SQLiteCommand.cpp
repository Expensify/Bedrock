#include "SQLiteCommand.h"

// Default constructor.
SQLiteCommand::SQLiteCommand() :
    initiator(nullptr),
    priority(SQLiteCommand::PRIORITY_NORMAL),
    creationTimestamp(0),
    replicationStartTimestamp(0),
    processingTime(0),
    writeConsistency(SQLiteNode::ASYNC),
    complete(false),
    peekCount(0),
    processCount(0)
{ }
