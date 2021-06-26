#include <libstuff/libstuff.h>
#include "BedrockCommand.h"
#include "BedrockPlugin.h"

atomic<size_t> BedrockCommand::_commandCount(0);

const string BedrockCommand::defaultPluginName("NO_PLUGIN");

BedrockCommand::BedrockCommand(SQLiteCommand&& baseCommand, BedrockPlugin* plugin, bool escalateImmediately_) :
    SQLiteCommand(move(baseCommand)),
    priority(PRIORITY_NORMAL),
    peekCount(0),
    processCount(0),
    repeek(false),
    crashIdentifyingValues(*this),
    escalateImmediately(escalateImmediately_),
    _plugin(plugin),
    _inProgressTiming(INVALID, 0, 0),
    _timeout(_getTimeout(request))
{
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
    _commandCount++;
}

const string& BedrockCommand::getName() const {
    if (_plugin) {
        return _plugin->getName();
    }
    return defaultPluginName;
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

    int64_t start;
    if (request.isSet("commandExecuteTime")) {
        start = request.calc64("commandExecuteTime");
    } else {
        SWARN("BedrockCommand '" + request.methodLine + "' created with no commandExecuteTime, should be done in base constructor!");
        start = STimeNow();
    }
    return timeout + start;
}

BedrockCommand::~BedrockCommand() {
    for (auto request : httpsRequests) {
        request->manager.closeTransaction(request);
    }
    _commandCount--;
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

void BedrockCommand::reset(BedrockCommand::STAGE stage) {
    if (stage == STAGE::PEEK) {
        jsonContent.clear();
        response.clear();
    }
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

    // We also want to know what leader did if we're on a follower.
    uint64_t upstreamPeekTime = 0;
    uint64_t upstreamProcessTime = 0;
    uint64_t upstreamUnaccountedTime = 0;
    uint64_t upstreamTotalTime = 0;

    // Now promote any existing values that were set upstream. This prepends `upstream` and makes the first existing
    // character of the name uppercase, (i.e. myValue -> upstreamMyValue), letting us keep anything that was set by the
    // leader server. We clear these values after setting the new ones, so that we can add our own values.
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
