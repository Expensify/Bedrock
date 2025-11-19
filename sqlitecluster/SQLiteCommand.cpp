#include "SQLiteCommand.h"

#include <libstuff/libstuff.h>
#include <libstuff/SRandom.h>

SData SQLiteCommand::preprocessRequest(SData&& request)
{
    // If the request doesn't specify an execution time, default to right now.
    if (request.isSet("commandExecuteTime")) {
        // We are deprecating `commandExecuteTime` so need to figure out where it's used.
        auto now = STimeNow();
        auto executeTime = request.calcU64("commandExecuteTime");
        if (executeTime > now + 5'000'000) {
            auto difference = executeTime - now;
            SINFO("Command '" << request.methodLine << "' requested execution time " << difference << "us in the future.");
        }
    }

    // Add a request ID if one was missing.
    if (!request.isSet("requestID")) {
        string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        string requestID;
        for (int i = 0; i < 6; i++) {
            requestID += chars[SRandom::rand64() % chars.size()];
        }
        request["requestID"] = requestID;
    }
    return move(request);
}

SQLiteCommand::SQLiteCommand(SData&& _request) :
    privateRequest(preprocessRequest(move(_request))),
    request(privateRequest),
    initiatingClientID(0),
    writeConsistency(SQLiteNode::ASYNC),
    complete(false),
    escalationTimeUS(0),
    creationTime(STimeNow()),
    escalated(false)
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

SQLiteCommand::SQLiteCommand(SQLiteCommand&& from) :
    privateRequest(move(from.privateRequest)),
    request(privateRequest),
    initiatingClientID(from.initiatingClientID),
    id(move(from.id)),
    jsonContent(move(from.jsonContent)),
    response(move(from.response)),
    writeConsistency(from.writeConsistency),
    complete(from.complete),
    escalationTimeUS(from.escalationTimeUS),
    creationTime(from.creationTime),
    escalated(from.escalated)
{
}

SQLiteCommand& SQLiteCommand::operator=(SQLiteCommand&& from) noexcept
{
    privateRequest = move(from.privateRequest);
    const_cast<SData&>(request) = privateRequest;
    initiatingClientID = from.initiatingClientID;
    id = move(from.id);
    jsonContent = move(from.jsonContent);
    response = move(from.response);
    writeConsistency = from.writeConsistency;
    complete = from.complete;
    escalationTimeUS = from.escalationTimeUS;
    creationTime = from.creationTime;
    escalated = from.escalated;

    return *this;
}

SQLiteCommand::SQLiteCommand() :
    privateRequest(),
    request(privateRequest),
    initiatingClientID(0),
    writeConsistency(SQLiteNode::ASYNC),
    complete(false),
    escalationTimeUS(0),
    creationTime(STimeNow()),
    escalated(false)
{
}
