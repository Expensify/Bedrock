#include "BedrockConflictManager.h"

BedrockConflictInfo::BedrockConflictInfo(const string& commandName) :
_commandName(commandName)
{ }

void BedrockConflictInfo::success() {
    ++_totalSuccessCount;
    _results.set(_resultsPtr);
    ++_resultsPtr;
    _resultsPtr %= COMMAND_COUNT;
}

void BedrockConflictInfo::conflict() {
    ++_totalConflictCount;
    _results.reset(_resultsPtr);
    ++_resultsPtr;
    _resultsPtr %= COMMAND_COUNT;
}

int BedrockConflictInfo::recentSuccessCount() {
    return _results.count();
}

int BedrockConflictInfo::recentConflictCount() {
    return COMMAND_COUNT - _results.count();
}

uint64_t BedrockConflictInfo::totalSuccessCount() {
    return _totalSuccessCount;
}

uint64_t BedrockConflictInfo::totalConflictCount() {
    return _totalConflictCount;
}

recursive_mutex BedrockConflictManager::_mutex;
map<string, BedrockConflictInfo> BedrockConflictManager::_conflictInfoMap;
int BedrockConflictManager::_threshold = (int)(BedrockConflictManager::_fraction * BedrockConflictInfo::COMMAND_COUNT);

void BedrockConflictManager::commandConflicted(const string& commandName) {
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

void BedrockConflictManager::commandSucceeded(const string& commandName) {
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

bool BedrockConflictManager::multiWriteEnabled(const string& commandName) {
    SAUTOLOCK(_mutex);
    auto it = _conflictInfoMap.find(commandName);
    if (it == _conflictInfoMap.end()) {
        SINFO("Command '" << commandName << "' not tracked in BedrockConflictManager. Assuming OK.");
        return true;
    }
    int conflicts = it->second.recentConflictCount();
    if (conflicts >= _threshold) {
        SINFO("Not multi-writing command '" << commandName << "' because " << conflicts << " of the last "
              << BedrockConflictInfo::COMMAND_COUNT << " calls have conflicted.");
        return false;
    }
    SINFO("Multi-writing command '" << commandName << "'. Only " << conflicts << " of the last "
          << BedrockConflictInfo::COMMAND_COUNT << " calls have conflicted.");
    return true;
}
