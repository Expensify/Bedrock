#include "BedrockConflictMetrics.h"

// Initialize non-const static variables.
recursive_mutex BedrockConflictMetrics::_mutex;
map<string, BedrockConflictMetrics> BedrockConflictMetrics::_conflictInfoMap;
double BedrockConflictMetrics::_fraction = 0.10;
int BedrockConflictMetrics::_threshold = _fraction * COMMAND_COUNT;

BedrockConflictMetrics::BedrockConflictMetrics(const string& commandName) :
_commandName(commandName)
{ }

void BedrockConflictMetrics::success() {
    ++_totalSuccessCount;
    _results.reset(_resultsPtr);
    ++_resultsPtr;
    _resultsPtr %= COMMAND_COUNT;
}

void BedrockConflictMetrics::conflict() {
    ++_totalConflictCount;
    _results.set(_resultsPtr);
    ++_resultsPtr;
    _resultsPtr %= COMMAND_COUNT;
}

int BedrockConflictMetrics::recentSuccessCount() {
    return COMMAND_COUNT - _results.count();
}

int BedrockConflictMetrics::recentConflictCount() {
    return _results.count();
}

void BedrockConflictMetrics::recordConflict(const string& commandName) {
    SAUTOLOCK(_mutex);

    // Look up, and create if necessary, the info object for this command.
    auto it = _conflictInfoMap.find(commandName);
    if (it == _conflictInfoMap.end()) {
        // We need to insert this command.
        auto itPair = _conflictInfoMap.emplace(make_pair(commandName, commandName));
        it = itPair.first;
    }
    it->second.conflict();
    SINFO("Multi-write conflict recorded for " << commandName);
}

void BedrockConflictMetrics::recordSuccess(const string& commandName) {
    SAUTOLOCK(_mutex);

    // Look up, and create if necessary, the info object for this command.
    auto it = _conflictInfoMap.find(commandName);
    if (it == _conflictInfoMap.end()) {
        // We need to insert this command.
        auto itPair = _conflictInfoMap.emplace(make_pair(commandName, commandName));
        it = itPair.first;
    }
    it->second.success();
    SINFO("Multi-write success recorded for " << commandName);
}

bool BedrockConflictMetrics::multiWriteOK(const string& commandName) {
    SAUTOLOCK(_mutex);

    // Look up the command in the list of commands.
    auto it = _conflictInfoMap.find(commandName);
    if (it == _conflictInfoMap.end()) {
        // If we don't find it, we assume it's OK to multi-write.
        SINFO("Multi-write command '" << commandName << "' not tracked in BedrockConflictMetrics. Assuming OK.");
        return true;
    }

    // Otherwise, we check to see if it's recent conflict count is too high for multi-write.
    auto& metric = it->second;
    int conflicts = metric.recentConflictCount();
    uint64_t totalAttempts = metric._totalConflictCount + metric._totalSuccessCount;
    bool result = conflicts < _threshold;
    string resultString = result ? "OK" : "DENIED";
    SINFO("Multi-write " << resultString << " for command '" << commandName << "' recent conflicts: "
          << conflicts << "/" << min((uint64_t)COMMAND_COUNT, totalAttempts) << ".");

    // And now that we know whether or not we can multi-write this, see if that's different than the last time we
    // checked for this command, so we can do extra logging if so.
    if (result != metric._lastCheckOK) {
        SINFO("Multi-write changing to " << resultString << " for command '" << commandName
              << "' recent conflicts: " << conflicts << "/" << min((uint64_t)COMMAND_COUNT, totalAttempts)
              << ", total conflicts: " << metric._totalConflictCount << "/" << totalAttempts << ".");
    }
    metric._lastCheckOK = result;

    return result;
}

string BedrockConflictMetrics::getMultiWriteDeniedCommands() {
    SAUTOLOCK(_mutex);
    set<string> commands;
    for (auto& pair : _conflictInfoMap) {
        auto& metric = pair.second;
        if (metric.recentConflictCount() >= _threshold) {
            commands.insert(metric._commandName);
        }
    }
    return SComposeList(commands);
}

void BedrockConflictMetrics::setFraction(double fraction) {
    SAUTOLOCK(_mutex);
    if (fraction > 0.0 && fraction < 1.0) {
        _fraction = fraction;
        _threshold = _fraction * COMMAND_COUNT;
    }
}
