/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    BedrockConflictManager.h
 * Path:    BedrockConflictManager.h
 * Pair:    BedrockConflictManager.cpp
 *
 * INTENT
 *   Tracks how often each command name touches which database tables
 *   during commit conflicts, for profiling which commands/tables conflict
 *   most.
 *
 * OBJECTS
 *   BedrockConflictManagerCommandInfo - per-command tally: a total count
 *       plus a table-name to use-count map.
 *   BedrockConflictManager - thread-safe collector; recordTables() tallies
 *       one command's table touches, generateReport() renders a
 *       plaintext summary.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   The private mutex `m` breaks the repo's leading-underscore convention
 *   for private members, while `_commandInfo` right below it follows it.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>

using namespace std;

class BedrockConflictManagerCommandInfo {
public:
    size_t count = 0;
    map<string, size_t> tableUseCounts;
};

class BedrockConflictManager {
public:
    BedrockConflictManager();
    void recordTables(const string& commandName, const set<string>& tables);
    string generateReport();

private:
    mutex m;
    map<string, BedrockConflictManagerCommandInfo> _commandInfo;
};
