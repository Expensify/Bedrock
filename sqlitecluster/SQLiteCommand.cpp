#include "SQLiteCommand.h"

SQLiteCommand::SQLiteCommand(SData&& _request) : 
    initiatingPeerID(0),
    initiatingClientID(0),
    request(move(_request)),
    writeConsistency(SQLiteNode::ASYNC),
    complete(false),
    escalationTimeUS(0),
    creationTime(STimeNow())
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

    // If the request doesn't specify an execution time, default to right now.
    if (!request.isSet("commandExecuteTime")) {
        request["commandExecuteTime"] = to_string(STimeNow());
    }
}

SQLiteCommand::SQLiteCommand() :
    initiatingPeerID(0),
    initiatingClientID(0),
    writeConsistency(SQLiteNode::ASYNC),
    complete(false),
    escalationTimeUS(0),
    creationTime(STimeNow())
{ }
