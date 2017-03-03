#include "SQLiteCommand.h"

SQLiteCommand::SQLiteCommand(SData&& _request) : 
    initiator(nullptr),
    request(move(_request)),
    writeConsistency(SQLiteNode::ASYNC),
    complete(false),
    creationTimestamp(0),
    replicationStartTimestamp(0),
    processingTime(0)
{
    // Initialize the consistency, if supplied.
    if (request.isSet("writeConsistency")) {
        int tempConsistency = request.calc("writeConsistency");
        switch (tempConsistency) {
            // For any valid case, we just set the value directly.
            case SQLiteNode::ASYNC:
            case SQLiteNode::ONE:
            case SQLiteNode::QUORUM:
                writeConsistency = static_cast<SQLiteNode::ConsistencyLevel>(tempConsistency);
                break;
            default:
                // But an invalid case gets set to ASYNC, and a warning is thrown.
                SWARN("'" << request.methodLine << "' requested invalid consistency: " << writeConsistency);
                writeConsistency = SQLiteNode::ASYNC;
                break;
        }
    }
}

SQLiteCommand::SQLiteCommand() :
    initiator(nullptr),
    writeConsistency(SQLiteNode::ASYNC),
    complete(false),
    creationTimestamp(0),
    replicationStartTimestamp(0),
    processingTime(0)
{ }
