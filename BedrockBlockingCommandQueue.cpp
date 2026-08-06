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
    if (isBlocked(command->blockingQueueRateLimitIdentifier, command->request.methodLine)) {
        STHROW("503 Blocking queue rate limited (time)");
    }

    // Base class acquires its own (non-recursive) `_queueMutex`.
    BedrockCommandQueue::push(move(command));
}

/**
 * Dequeues a command and rejects it if its account or command name is rate limited.
 * Called by `BedrockCommandQueue::get()` with the base `_queueMutex` held. Calling any base method that reacquires `_queueMutex` would deadlock.
 */
unique_ptr<BedrockCommand> BedrockBlockingCommandQueue::_dequeue()
{
    auto command = BedrockCommandQueue::_dequeue();

    // Marking it complete skips processing in `BedrockServer::runCommand`, which replies to already-complete commands.
    if (isBlocked(command->blockingQueueRateLimitIdentifier, command->request.methodLine)) {
        command->response.methodLine = "503 Blocking queue rate limited (time)";
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
        lock_guard<decltype(_accountStates.mapMutex)> lock(_accountStates.mapMutex);
        size += _accountStates.states.size();
        _accountStates.states.clear();
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

    // Count tracked and currently-blocked identifiers in a map. Holds the map mutex while briefly locking each
    // entry to read `blockedUntil`; no code path takes an entry lock before the map mutex, so this can't deadlock.
    auto countBlocked = [now](StateMap& map, size_t& tracked, size_t& blocked) {
        lock_guard<decltype(map.mapMutex)> lock(map.mapMutex);
        tracked = map.states.size();
        blocked = 0;
        for (const auto& p : map.states) {
            lock_guard<decltype(p.second->m)> stateLock(p.second->m);
            if (p.second->blockedUntil > now) {
                blocked++;
            }
        }
    };

    size_t trackedAccounts = 0, blockedAccounts = 0, trackedCommands = 0, blockedCommands = 0;
    countBlocked(_accountStates, trackedAccounts, blockedAccounts);
    countBlocked(_commandStates, trackedCommands, blockedCommands);

    STable content;
    content["blockingTimeWindowMs"] = to_string(_windowUS.load() / 1000);
    content["blockingAccountThresholdMs"] = to_string(_accountThresholdUS.load() / 1000);
    content["blockingCommandThresholdMs"] = to_string(_commandThresholdUS.load() / 1000);
    content["blockingBlockDurationMs"] = to_string(_blockDurationUS.load() / 1000);
    content["blockingTrackedAccounts"] = to_string(trackedAccounts);
    content["blockingBlockedAccounts"] = to_string(blockedAccounts);
    content["blockingTrackedCommands"] = to_string(trackedCommands);
    content["blockingBlockedCommands"] = to_string(blockedCommands);
    return content;
}

uint64_t BedrockBlockingCommandQueue::setWindow(uint64_t windowUS)
{
    return _windowUS.exchange(windowUS);
}

uint64_t BedrockBlockingCommandQueue::setAccountThreshold(uint64_t thresholdUS)
{
    return _accountThresholdUS.exchange(thresholdUS);
}

uint64_t BedrockBlockingCommandQueue::setCommandThreshold(uint64_t thresholdUS)
{
    return _commandThresholdUS.exchange(thresholdUS);
}

uint64_t BedrockBlockingCommandQueue::setBlockDuration(uint64_t durationUS)
{
    return _blockDurationUS.exchange(durationUS);
}

void BedrockBlockingCommandQueue::recordExecutionTime(const string& accountID, const string& commandName, uint64_t elapsedUS)
{
    const uint64_t now = _now();
    _recordAndCheck(_accountStates, accountID, _accountThresholdUS.load(), now, elapsedUS, "account");
    _recordAndCheck(_commandStates, commandName, _commandThresholdUS.load(), now, elapsedUS, "command");
}

bool BedrockBlockingCommandQueue::isBlocked(const string& accountID, const string& commandName)
{
    // Hot path: called by push() and by _dequeue() under the base `_queueMutex`. Keep it O(1) by reading only
    // the precomputed block deadline. The windowed time is summed in recordExecutionTime, off the blocking thread.
    const uint64_t now = _now();
    return _isBlocked(_accountStates, accountID, now) || _isBlocked(_commandStates, commandName, now);
}

shared_ptr<BedrockBlockingCommandQueue::IdentifierState> BedrockBlockingCommandQueue::_getOrCreateState(StateMap& map, const string& key)
{
    lock_guard<decltype(map.mapMutex)> lock(map.mapMutex);
    auto [it, inserted] = map.states.try_emplace(key);
    if (inserted) {
        it->second = make_shared<IdentifierState>();
    }
    return it->second;
}

shared_ptr<BedrockBlockingCommandQueue::IdentifierState> BedrockBlockingCommandQueue::_getState(StateMap& map, const string& key)
{
    lock_guard<decltype(map.mapMutex)> lock(map.mapMutex);
    auto it = map.states.find(key);
    return it == map.states.end() ? nullptr : it->second;
}

void BedrockBlockingCommandQueue::_recordAndCheck(StateMap& map, const string& key, uint64_t thresholdUS, uint64_t now, uint64_t elapsedUS, const char* dimension)
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

    // Drop samples that finished before the window. They're ordered oldest-first, so expired ones are a
    // contiguous prefix. Guard the subtraction so a future-dated sample (clock skew) can't underflow.
    while (!state->commands.empty() && now > state->commands.front().finishTime && now - state->commands.front().finishTime >= windowUS) {
        state->commands.pop_front();
    }

    // Sum the time spent in the window, crediting each sample only for the part that lies inside it.
    uint64_t total = 0;
    for (const auto& command : state->commands) {
        const uint64_t age = now > command.finishTime ? now - command.finishTime : 0;
        if (age >= windowUS) {
            continue;
        }
        total += min(command.elapsedTime, windowUS - age);
    }

    if (total > thresholdUS) {
        state->blockedUntil = now + _blockDurationUS.load();
        SINFO("Blocking queue rate limit (time), blocking dimension=" << dimension << " identifier=" << key
              << " timeMS=" << (total / 1000) << " thresholdMS=" << (thresholdUS / 1000)
              << " blockDurationMS=" << (_blockDurationUS.load() / 1000));
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
