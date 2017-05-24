#include <libstuff/libstuff.h>
#include "BedrockCommand.h"

BedrockCommand::BedrockCommand() :
    SQLiteCommand(),
    httpsRequest(nullptr),
    priority(PRIORITY_NORMAL),
    peekCount(0),
    processCount(0)
{ }

BedrockCommand::~BedrockCommand() {
    if (httpsRequest) {
        httpsRequest->owner.closeTransaction(httpsRequest);
        httpsRequest = nullptr;
    }
}

BedrockCommand::BedrockCommand(SQLiteCommand&& from) :
    SQLiteCommand(std::move(from)),
    httpsRequest(nullptr),
    priority(PRIORITY_NORMAL),
    peekCount(0),
    processCount(0)
{
    _init();
}

BedrockCommand::BedrockCommand(BedrockCommand&& from) :
    SQLiteCommand(std::move(from)),
    httpsRequest(from.httpsRequest),
    priority(from.priority),
    peekCount(from.peekCount),
    processCount(from.processCount),
    timingInfo(from.timingInfo)
{
    // The move constructor (and likewise, the move assignment operator), don't simply copy this pointer value, but
    // they clear it from the old object, so that when its destructor is called, the HTTPS transaction isn't closed.
    from.httpsRequest = nullptr;
}

BedrockCommand::BedrockCommand(SData&& _request) :
    SQLiteCommand(move(_request)),
    httpsRequest(nullptr),
    priority(PRIORITY_NORMAL),
    peekCount(0),
    processCount(0)
{
    _init();
}

BedrockCommand::BedrockCommand(SData _request) :
    SQLiteCommand(move(_request)),
    httpsRequest(nullptr),
    priority(PRIORITY_NORMAL),
    peekCount(0),
    processCount(0)
{
    _init();
}

BedrockCommand& BedrockCommand::operator=(BedrockCommand&& from) {
    if (this != &from) {
        // The current incarnation of this object is going away, if it had an httpsRequest, we'll need to destroy it,
        // or it will leak and never get cleaned up.
        if (httpsRequest) {
            httpsRequest->owner.closeTransaction(httpsRequest);
        }
        httpsRequest = from.httpsRequest;
        from.httpsRequest = nullptr;

        // Update our other properties.
        peekCount = from.peekCount;
        processCount = from.processCount;
        priority = from.priority;
        timingInfo = from.timingInfo;

        // And call the base class's move constructor as well.
        SQLiteCommand::operator=(move(from));
    }

    return *this;
}

void BedrockCommand::_init() {
    // Initialize the priority, if supplied.
    if (request.isSet("priority")) {
        int tempPriority = request.calc("priority");
        switch (tempPriority) {
            // For any valid case, we just set the value directly.
            case BedrockCommand::PRIORITY_MIN:
            case BedrockCommand::PRIORITY_LOW:
            case BedrockCommand::PRIORITY_NORMAL:
            case BedrockCommand::PRIORITY_HIGH:
            case BedrockCommand::PRIORITY_MAX:
                priority = static_cast<Priority>(tempPriority);
                break;
            default:
                // But an invalid case gets set to NORMAL, and a warning is logged.
                SWARN("'" << request.methodLine << "' requested invalid priority: " << tempPriority);
                priority = PRIORITY_NORMAL;
                break;
        }
    }
}

void BedrockCommand::finalizeTimingInfo() {
    uint64_t peekTotal = 0;
    uint64_t processTotal = 0;
    for (const auto& entry: timingInfo) {
        if (get<0>(entry) == PEEK) {
            peekTotal += get<2>(entry) - get<1>(entry);
        } else if (get<0>(entry) == PROCESS) {
            processTotal += get<2>(entry) - get<1>(entry);
        }
    }

    /*
    for (const auto& upstream : {"peekTime", "processTime", "totalTime") {
        auto it = response.nameValueMap.find(upstream);
        if (it != response.nameValueMap.end()) {
            auto temp = *it;
            response.nameValueMap.erase(it);
            response.nameValueMap[uppercase] = temp;
        }
    }
    */

    // If someone upstream had set these (typically, master), we'll save those values and add our own.
    if (response.isSet("peekTime")) {
        response["upstreamPeekTime"] = response["peekTime"];
    }
    if (response.isSet("processTime")) {
        response["upstreamProcessTime"] = response["processTime"];
    }
    if (response.isSet("totalTime")) {
        response["upstreamTotalTime"] = response["totalTime"];
    }

    // And set the values we have, if any.
    if(peekTotal) {
        response["peekTime"] = to_string(peekTotal);
    } else {
        response.erase("peekTime");
    }
    if (processTotal) {
        response["processTime"] = to_string(processTotal);
    } else {
        response.erase("processTime");
    }

    // The entire lifetime of this object.
    uint64_t total = STimeNow() - creationTime;
    if (total) {
        response["totalTime"] = to_string(total);
    } else {
        response.erase("totalTime");
    }

    // And the time it spent in escalation to master (if any).
    if (escalationTimeUS) {
        response["escalationTime"] = to_string(escalationTimeUS);
    } else {
        response.erase("escalationTime");
    }
}
