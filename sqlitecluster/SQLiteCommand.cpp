#include "SQLiteCommand.h"

SQLiteCommand::SQLiteCommand(SData&& _request) : 
    initiatingPeerID(0),
    initiatingClientID(0),
    request(move(_request)),
    writeConsistency(SQLiteNode::ASYNC),
    complete(false),
    executeTimestamp(SToUInt64(request["commandExecuteTime"]) ?: STimeNow()),
    commitCount(SToUInt64(request["commitCount"]) ?: 0)
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
    initiatingPeerID(0),
    initiatingClientID(0),
    writeConsistency(SQLiteNode::ASYNC),
    complete(false),
    executeTimestamp(0)
{ }
