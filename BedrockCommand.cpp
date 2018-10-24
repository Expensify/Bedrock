#include <libstuff/libstuff.h>
#include "BedrockCommand.h"

mutex BedrockCommand::timeoutMutex;
multimap<uint64_t, const BedrockCommand*> BedrockCommand::commandTimeouts;

void BedrockCommand::_addCommandTimeout(const BedrockCommand* cmd) {
    // Insert in timeout info.
    lock_guard<mutex> lock(timeoutMutex);
    commandTimeouts.insert(make_pair(cmd->_timeout, cmd));
}

int64_t BedrockCommand::_getTimeout(const SData& request) {
    // Timeout is the default, unless explicitly supplied, or if Connection: forget is set.
    int64_t timeout =  DEFAULT_TIMEOUT;
    if (request.isSet("timeout")) {
        timeout = request.calc("timeout");
    } else if (SIEquals(request["connection"], "forget")) {
        timeout = DEFAULT_TIMEOUT_FORGET;
    }

    // Convert to microseconds.
    timeout *= 1000;

    int64_t start = request.isSet("commandExecuteTime") ? request.calc64("commandExecuteTime") : STimeNow();
    return timeout + start;
}

BedrockCommand::BedrockCommand() :
    SQLiteCommand(),
    priority(PRIORITY_NORMAL),
    peekCount(0),
    processCount(0),
    peekedBy(nullptr),
    processedBy(nullptr),
    onlyProcessOnSyncThread(false),
    dead(false),
    _inProgressTiming(INVALID, 0, 0),
    _timeout(_getTimeout(request))
{
    _addCommandTimeout(this);
}

BedrockCommand::~BedrockCommand() {
    // Lock before doing any cleanup, this way if timing out occurs right at destruction, there's no ambiguity as to
    // who cleaned this up.
    lock_guard<mutex> lock(timeoutMutex);
    for (auto request : httpsRequests) {
        request->owner.closeTransaction(request);
    }

    // Remove in timeout info.
    auto items = commandTimeouts.equal_range(_timeout);

    int count = 0;
    for (auto it = items.first; it != items.second; ++it) {
        count++;
        if (it->second == this) {
            // delete this item an be done.
            commandTimeouts.erase(it);
            break;
        }
    }
    if (!count) {
        SWARN("No items found in timeout map to remove!");
    }
}

BedrockCommand::BedrockCommand(SQLiteCommand&& from) :
    SQLiteCommand(move(from)),
    priority(PRIORITY_NORMAL),
    peekCount(0),
    processCount(0),
    peekedBy(nullptr),
    processedBy(nullptr),
    onlyProcessOnSyncThread(false),
    dead(false),
    _inProgressTiming(INVALID, 0, 0),
    _timeout(_getTimeout(request))
{
    _init();
}

BedrockCommand::BedrockCommand(BedrockCommand&& from) :
    SQLiteCommand(move(from)),
    httpsRequests(move(from.httpsRequests)),
    priority(from.priority),
    peekCount(from.peekCount),
    processCount(from.processCount),
    peekedBy(from.peekedBy),
    processedBy(from.processedBy),
    timingInfo(from.timingInfo),
    onlyProcessOnSyncThread(from.onlyProcessOnSyncThread),
    crashIdentifyingValues(move(from.crashIdentifyingValues)),
    dead(from.dead.load()),
    _inProgressTiming(from._inProgressTiming),
    _timeout(from._timeout)
{
    // The move constructor (and likewise, the move assignment operator), don't simply copy these pointer values, but
    // they clear them from the old object, so that when its destructor is called, the HTTPS transactions aren't
    // closed.
    from.httpsRequests.clear();

    _addCommandTimeout(this);
}

BedrockCommand::BedrockCommand(SData&& _request) :
    SQLiteCommand(move(_request)),
    priority(PRIORITY_NORMAL),
    peekCount(0),
    processCount(0),
    peekedBy(nullptr),
    processedBy(nullptr),
    onlyProcessOnSyncThread(false),
    dead(false),
    _inProgressTiming(INVALID, 0, 0),
    _timeout(_getTimeout(request))
{
    _init();
}

BedrockCommand::BedrockCommand(SData _request) :
    SQLiteCommand(move(_request)),
    priority(PRIORITY_NORMAL),
    peekCount(0),
    processCount(0),
    peekedBy(nullptr),
    processedBy(nullptr),
    onlyProcessOnSyncThread(false),
    dead(false),
    _inProgressTiming(INVALID, 0, 0),
    _timeout(_getTimeout(request))
{
    _init();
}

BedrockCommand& BedrockCommand::operator=(BedrockCommand&& from) {
    if (this != &from) {
        // The current incarnation of this object is going away, if it had an httpsRequest, we'll need to destroy it,
        // or it will leak and never get cleaned up.
        for (auto request : httpsRequests) {
            if (!request->response) {
                SWARN("Closing unfinished httpRequest by assigning over it. This was probably a mistake.");
            }
            request->owner.closeTransaction(request);
        }
        httpsRequests = move(from.httpsRequests);
        from.httpsRequests.clear();

        // Update our other properties.
        peekCount = from.peekCount;
        processCount = from.processCount;
        peekedBy = from.peekedBy;
        processedBy = from.processedBy;
        priority = from.priority;
        timingInfo = from.timingInfo;
        onlyProcessOnSyncThread = from.onlyProcessOnSyncThread;
        crashIdentifyingValues = move(from.crashIdentifyingValues);
        dead.store(from.dead.load());
        _inProgressTiming = from._inProgressTiming;
        _timeout = from._timeout;

        // And call the base class's move constructor as well.
        SQLiteCommand::operator=(move(from));

        _addCommandTimeout(this);
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
    _addCommandTimeout(this);
}

void BedrockCommand::startTiming(TIMING_INFO type) {
    if (get<0>(_inProgressTiming) != INVALID ||
        get<1>(_inProgressTiming) != 0
       ) {
        SWARN("Starting timing, but looks like it was already running.");
    }
    get<0>(_inProgressTiming) = type;
    get<1>(_inProgressTiming) = STimeNow();
    get<2>(_inProgressTiming) = 0;
}

void BedrockCommand::stopTiming(TIMING_INFO type) {
    if (get<0>(_inProgressTiming) != type ||
        get<1>(_inProgressTiming) == 0
       ) {
        SWARN("Stopping timing, but looks like it wasn't already running.");
    }

    // Add it to the list of timing info.
    get<2>(_inProgressTiming) = STimeNow();
    timingInfo.push_back(_inProgressTiming);

    // And reset it for next use.
    get<0>(_inProgressTiming) = INVALID;
    get<1>(_inProgressTiming) = 0;
    get<2>(_inProgressTiming) = 0;
}

bool BedrockCommand::areHttpsRequestsComplete() const {
    for (auto request : httpsRequests) {
        if (!request->response) {
            return false;
        }
    }
    return true;
}

void BedrockCommand::finalizeTimingInfo() {
    uint64_t peekTotal = 0;
    uint64_t processTotal = 0;
    uint64_t commitWorkerTotal = 0;
    uint64_t commitSyncTotal = 0;
    uint64_t queueWorkerTotal = 0;
    uint64_t queueSyncTotal = 0;
    for (const auto& entry: timingInfo) {
        if (get<0>(entry) == PEEK) {
            peekTotal += get<2>(entry) - get<1>(entry);
        } else if (get<0>(entry) == PROCESS) {
            processTotal += get<2>(entry) - get<1>(entry);
        } else if (get<0>(entry) == COMMIT_WORKER) {
            commitWorkerTotal += get<2>(entry) - get<1>(entry);
        } else if (get<0>(entry) == COMMIT_SYNC) {
            commitSyncTotal += get<2>(entry) - get<1>(entry);
        } else if (get<0>(entry) == QUEUE_WORKER) {
            queueWorkerTotal += get<2>(entry) - get<1>(entry);
        } else if (get<0>(entry) == QUEUE_SYNC) {
            queueSyncTotal += get<2>(entry) - get<1>(entry);
        }
    }

    // The lifespan of the object up until now.
    uint64_t totalTime = STimeNow() - creationTime;

    // Time that wasn't accounted for in all the other metrics.
    uint64_t unaccountedTime = totalTime - (peekTotal + processTotal + commitWorkerTotal + commitSyncTotal +
                                            escalationTimeUS + queueWorkerTotal + queueSyncTotal);

    // Build a map of the values we care about.
    map<string, uint64_t> valuePairs = {
        {"peekTime",        peekTotal},
        {"processTime",     processTotal},
        {"totalTime",       totalTime},
        {"escalationTime",  escalationTimeUS},
        {"unaccountedTime", unaccountedTime},
    };

    // We also want to know what master did if we're on a slave.
    uint64_t upstreamPeekTime = 0;
    uint64_t upstreamProcessTime = 0;
    uint64_t upstreamUnaccountedTime = 0;
    uint64_t upstreamTotalTime = 0;

    // Now promote any existing values that were set upstream. This prepends `upstream` and makes the first existing
    // character of the name uppercase, (i.e. myValue -> upstreamMyValue), letting us keep anything that was set by the
    // master server. We clear these values after setting the new ones, so that we can add our own values.
    for (const auto& p : valuePairs) {
        auto it = response.nameValueMap.find(p.first);
        if (it != response.nameValueMap.end()) {
            string temp = it->second;
            response.nameValueMap.erase(it);
            response.nameValueMap[string("upstream") + (char)toupper(p.first[0]) + (p.first.substr(1))] = temp;

            // Note the upstream times for our logline.
            if (p.first == "peekTime") {
                upstreamPeekTime = SToUInt64(temp);
            }
            else if (p.first == "processTime") {
                upstreamProcessTime = SToUInt64(temp);
            }
            else if (p.first == "unaccountedTime") {
                upstreamUnaccountedTime = SToUInt64(temp);
            }
            else if (p.first == "totalTime") {
                upstreamTotalTime = SToUInt64(temp);
            }
        }
    }

    // Log all this info.
    SINFO("command '" << request.methodLine << "' timing info (ms): "
          << peekTotal/1000 << " (" << peekCount << "), "
          << processTotal/1000 << " (" << processCount << "), "
          << commitWorkerTotal/1000 << ", "
          << commitSyncTotal/1000 << ", "
          << queueWorkerTotal/1000 << ", "
          << queueSyncTotal/1000 << ", "
          << totalTime/1000 << ", "
          << unaccountedTime/1000 << ", "
          << escalationTimeUS/1000 << ". Upstream: "
          << upstreamPeekTime/1000 << ", "
          << upstreamProcessTime/1000 << ", "
          << upstreamTotalTime/1000 << ", "
          << upstreamUnaccountedTime/1000 << "."
    );

    // And here's where we set our own values.
    for (const auto& p : valuePairs) {
        if (p.second) {
            response[p.first] = to_string(p.second);
        }
    }
}

// pop and push specializations for SSynchronizedQueue that record timing info.
template<>
BedrockCommand SSynchronizedQueue<BedrockCommand>::pop() {
    SAUTOLOCK(_queueMutex);
    if (!_queue.empty()) {
        BedrockCommand item = move(_queue.front());
        _queue.pop_front();
        item.stopTiming(BedrockCommand::QUEUE_SYNC);
        return item;
    }
    throw out_of_range("No commands");
}

template<>
void SSynchronizedQueue<BedrockCommand>::push(BedrockCommand&& cmd) {
    SAUTOLOCK(_queueMutex);
    SINFO("Enqueuing command '" << cmd.request.methodLine << "', with " << _queue.size() << " commands already queued.");
    // Just add to the queue
    _queue.push_back(move(cmd));
    _queue.back().startTiming(BedrockCommand::QUEUE_SYNC);

    // Write arbitrary buffer to the pipe so any subscribers will be awoken.
    // **NOTE: 1 byte so write is atomic.
    SASSERT(write(_pipeFD[1], "A", 1));
}
