#include "BedrockConflictMetrics.h"

// Initialize static variables.
recursive_mutex BedrockConflictMetrics::_mutex;
map<string, BedrockConflictMetrics> BedrockConflictMetrics::_conflictInfoMap;
int BedrockConflictMetrics::_threshold = (int)(BedrockConflictMetrics::_fraction * BedrockConflictMetrics::COMMAND_COUNT);

BedrockConflictMetrics::BedrockConflictMetrics(const string& commandName) :
_commandName(commandName)
{ }

void BedrockConflictMetrics::success() {
    ++_totalSuccessCount;
    _results.set(_resultsPtr);
    ++_resultsPtr;
    _resultsPtr %= COMMAND_COUNT;
}

void BedrockConflictMetrics::conflict() {
    ++_totalConflictCount;
    _results.reset(_resultsPtr);
    ++_resultsPtr;
    _resultsPtr %= COMMAND_COUNT;
}

int BedrockConflictMetrics::recentSuccessCount() {
    return _results.count();
}

int BedrockConflictMetrics::recentConflictCount() {
    return COMMAND_COUNT - _results.count();
}

uint64_t BedrockConflictMetrics::totalSuccessCount() {
    return _totalSuccessCount;
}

uint64_t BedrockConflictMetrics::totalConflictCount() {
    return _totalConflictCount;
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
    it->second.success();
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
    it->second.conflict();
}

bool BedrockConflictMetrics::multiWriteOK(const string& commandName) {
    SAUTOLOCK(_mutex);
    auto it = _conflictInfoMap.find(commandName);
    if (it == _conflictInfoMap.end()) {
        SINFO("Command '" << commandName << "' not tracked in BedrockConflictMetrics. Assuming OK.");
        return true;
    }
    int conflicts = it->second.recentConflictCount();
    if (conflicts >= _threshold) {
        SINFO("Not multi-writing command '" << commandName << "' because " << conflicts << " of the last "
              << COMMAND_COUNT << " calls have conflicted.");
        return false;
    }
    SINFO("Multi-writing command '" << commandName << "'. Only " << conflicts << " of the last "
          << COMMAND_COUNT << " calls have conflicted.");
    return true;
}
