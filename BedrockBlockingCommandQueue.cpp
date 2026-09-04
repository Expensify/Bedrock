#include <BedrockBlockingCommandQueue.h>

void BedrockBlockingCommandQueue::startTiming(unique_ptr<BedrockCommand>& command)
{
    command->startTiming(BedrockCommand::QUEUE_BLOCKING);
}

void BedrockBlockingCommandQueue::stopTiming(unique_ptr<BedrockCommand>& command)
{
    command->stopTiming(BedrockCommand::QUEUE_BLOCKING);
}

BedrockBlockingCommandQueue::BedrockBlockingCommandQueue() :
    BedrockCommandQueue(function<void(unique_ptr<BedrockCommand>&)>(startTiming),
                        function<void(unique_ptr<BedrockCommand>&)>(stopTiming))
{
}

uint64_t BedrockBlockingCommandQueue::_now() const
{
    return STimeNow();
}

void BedrockBlockingCommandQueue::push(unique_ptr<BedrockCommand>&& command)
{
    const string dimension = getBlockingDimension(command->blockingQueueRateLimitIdentifier, command->request.methodLine);
    if (!dimension.empty()) {
        SINFO("Blocking queue rate limited command rejected", {
            {"dimension", dimension},
            {"identifier", command->blockingQueueRateLimitIdentifier},
            {"command", command->request.methodLine}
        });
        STHROW("503 Blocking queue rate limited (" + dimension + ")");
    }

    // Base class acquires its own `_queueMutex`.
    BedrockCommandQueue::push(move(command));
}

unique_ptr<BedrockCommand> BedrockBlockingCommandQueue::_dequeue()
{
    auto command = BedrockCommandQueue::_dequeue();

    // Marking it complete skips processing in `BedrockServer::runCommand`, which replies to already-complete commands.
    const string dimension = getBlockingDimension(command->blockingQueueRateLimitIdentifier, command->request.methodLine);
    if (!dimension.empty()) {
        // The blocking worker sets its own prefix after `get()` returns, so without this the rejection logs the
        // thread-local defaults instead of the request that got the 503.
        SAUTOPREFIX(command->request);
        SINFO("Blocking queue rate limited command rejected", {
            {"dimension", dimension},
            {"identifier", command->blockingQueueRateLimitIdentifier},
            {"command", command->request.methodLine}
        });
        command->response.methodLine = "503 Blocking queue rate limited (" + dimension + ")";
        command->complete = true;
    }

    return command;
}

void BedrockBlockingCommandQueue::clear()
{
    clearRateLimits();
    BedrockCommandQueue::clear();
}

size_t BedrockBlockingCommandQueue::clearRateLimits()
{
    size_t size = 0;
    {
        lock_guard<decltype(_identifierStates.mapMutex)> lock(_identifierStates.mapMutex);
        size += _identifierStates.states.size();
        _identifierStates.states.clear();
    }
    {
        lock_guard<decltype(_commandStates.mapMutex)> lock(_commandStates.mapMutex);
        size += _commandStates.states.size();
        _commandStates.states.clear();
    }
    return size;
}

STable BedrockBlockingCommandQueue::getState()
{
    const uint64_t now = _now();

    // Collect the tracked count and the currently-blocked identifiers in a map. Holds the map mutex while briefly
    // locking each entry to read `blockedUntil`; no code path takes an entry lock before the map mutex, so this
    // can't deadlock.
    auto inspect = [now](StateMap& map, size_t& tracked, list<string>& blocked) {
        lock_guard<decltype(map.mapMutex)> lock(map.mapMutex);
        tracked = map.states.size();
        for (const auto& p : map.states) {
            lock_guard<decltype(p.second->m)> stateLock(p.second->m);
            if (p.second->blockedUntil > now) {
                blocked.push_back(p.first);
            }
        }
    };

    size_t trackedIdentifiers = 0, trackedCommands = 0;
    list<string> blockedIdentifiers, blockedCommands;
    inspect(_identifierStates, trackedIdentifiers, blockedIdentifiers);
    inspect(_commandStates, trackedCommands, blockedCommands);

    STable content;
    content["blockingTimeWindowMS"] = to_string(_windowUS.load() / 1000);
    content["blockingIdentifierThresholdMS"] = to_string(_identifierThresholdUS.load() / 1000);
    content["blockingCommandThresholdMS"] = to_string(_commandThresholdUS.load() / 1000);
    content["blockingBlockDurationMS"] = to_string(_blockDurationUS.load() / 1000);
    content["blockingTrackedIdentifiers"] = to_string(trackedIdentifiers);
    content["blockingBlockedIdentifiers"] = SComposeList(blockedIdentifiers);
    content["blockingTrackedCommands"] = to_string(trackedCommands);
    content["blockingBlockedCommands"] = SComposeList(blockedCommands);
    return content;
}

uint64_t BedrockBlockingCommandQueue::setWindow(const uint64_t windowUS)
{
    return _windowUS.exchange(windowUS);
}

uint64_t BedrockBlockingCommandQueue::setIdentifierThreshold(const uint64_t thresholdUS)
{
    return _identifierThresholdUS.exchange(thresholdUS);
}

uint64_t BedrockBlockingCommandQueue::setCommandThreshold(const uint64_t thresholdUS)
{
    return _commandThresholdUS.exchange(thresholdUS);
}

uint64_t BedrockBlockingCommandQueue::setBlockDuration(const uint64_t durationUS)
{
    return _blockDurationUS.exchange(durationUS);
}

void BedrockBlockingCommandQueue::recordExecutionTime(const string& identifier, const string& commandName, uint64_t elapsedUS)
{
    const uint64_t now = _now();
    _recordAndCheck(_identifierStates, identifier, _identifierThresholdUS.load(), now, elapsedUS, "identifier");
    _recordAndCheck(_commandStates, commandName, _commandThresholdUS.load(), now, elapsedUS, "command");
}

string BedrockBlockingCommandQueue::getBlockingDimension(const string& identifier, const string& commandName)
{
    // Hot path: called by push() and by _dequeue() under the base `_queueMutex`. Keep it O(1) by reading only
    // the precomputed block deadline. The windowed time is summed in recordExecutionTime, off the blocking thread.
    const uint64_t now = _now();
    if (_isBlocked(_identifierStates, identifier, now)) {
        return "identifier";
    }
    if (_isBlocked(_commandStates, commandName, now)) {
        return "command";
    }
    return "";
}

shared_ptr<BedrockBlockingCommandQueue::DimensionState> BedrockBlockingCommandQueue::_getOrCreateState(StateMap& map, const string& key)
{
    lock_guard<decltype(map.mapMutex)> lock(map.mapMutex);
    auto [it, inserted] = map.states.try_emplace(key);
    if (inserted) {
        it->second = make_shared<DimensionState>();
    }
    return it->second;
}

shared_ptr<BedrockBlockingCommandQueue::DimensionState> BedrockBlockingCommandQueue::_getState(StateMap& map, const string& key)
{
    lock_guard<decltype(map.mapMutex)> lock(map.mapMutex);
    auto it = map.states.find(key);
    return it == map.states.end() ? nullptr : it->second;
}

void BedrockBlockingCommandQueue::_recordAndCheck(StateMap& map, const string& key, uint64_t thresholdUS, uint64_t now, uint64_t elapsedUS, const string& dimension)
{
    if (key.empty() || thresholdUS == 0) {
        return;
    }

    auto state = _getOrCreateState(map, key);
    lock_guard<decltype(state->m)> lock(state->m);

    // Already inside an active block: a block lasts a fixed duration and is not extended by further hits, so
    // there's nothing to record or recompute until it expires.
    if (state->blockedUntil > now) {
        return;
    }

    state->commands.push_back({now, elapsedUS});

    const uint64_t windowUS = _windowUS.load();

    // Drop samples that finished before the current window and sum the ones that are still valid.
    uint64_t total = 0;
    for (auto it = state->commands.begin(); it != state->commands.end();) {
        const uint64_t timeSinceFinishedUS = now > it->finishTime ? now - it->finishTime : 0;
        if (timeSinceFinishedUS >= windowUS) {
            it = state->commands.erase(it);
            continue;
        }

        // Doing the line below will allow us to count for partial timings spent in a command. Let's say the window is 100s,
        // and we had a command that took 50s and finished 80s ago. We should only count 20s of that command, since the other
        // 30s are outside the window. So the result would be min(50s, 100 - 80) = min(50, 20) = 20s.
        total += min(it->elapsedTime, windowUS - timeSinceFinishedUS);
        ++it;
    }

    if (total > thresholdUS) {
        state->blockedUntil = now + _blockDurationUS.load();
        SINFO("Blocking queue rate limit reached, blocking", {
            {"dimension", dimension},
            {"identifier", key},
            {"timeMS", to_string(total / 1000)},
            {"thresholdMS", to_string(thresholdUS / 1000)},
            {"blockDurationMS", to_string(_blockDurationUS.load() / 1000)}
        });
    } else if (total > LOG_THRESHOLD_US) {
        // Log-only monitoring: the identifier is heavy but still under its block threshold.
        SINFO("Blocking queue rate limit above logging threshold", {
            {"dimension", dimension},
            {"identifier", key},
            {"timeMS", to_string(total / 1000)},
            {"thresholdMS", to_string(LOG_THRESHOLD_US / 1000)}
        });
    }
}

bool BedrockBlockingCommandQueue::_isBlocked(StateMap& map, const string& key, uint64_t now)
{
    if (key.empty()) {
        return false;
    }
    auto state = _getState(map, key);
    if (!state) {
        return false;
    }
    lock_guard<decltype(state->m)> lock(state->m);
    return state->blockedUntil > now;
}
