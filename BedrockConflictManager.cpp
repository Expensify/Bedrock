#include "BedrockConflictManager.h"
#include <libstuff/libstuff.h>

BedrockConflictManager::BedrockConflictManager() {
}

void BedrockConflictManager::recordTables(const string& commandName, const set<string>& tables) {
    {
        lock_guard<mutex> lock(m);
        auto commandInfo = _commandInfo.find(commandName);
        if (commandInfo == _commandInfo.end()) {
            commandInfo = _commandInfo.emplace(make_pair(commandName, BedrockConflictManagerCommandInfo())).first;
        }

        // Increase the count of the command in general.
        commandInfo->second.count++;

        // And for each table (that's not a journal).
        for (auto& t : tables) {
            // Skip journal.
            if (SStartsWith(t, "journal")) {
                continue;
            }

            // Does this command already have this table?
            auto tableInfo = commandInfo->second.tableUseCounts.find(t);
            if (tableInfo == commandInfo->second.tableUseCounts.end()) {
                tableInfo = commandInfo->second.tableUseCounts.emplace(make_pair(t, 1)).first;
            } else {
                tableInfo->second++;
            }
        }
    }

    // And increase the count for each used table.
    SINFO("Command " << commandName << " used tables: " << SComposeList(tables));
}

string BedrockConflictManager::generateReport() {
    stringstream out;
    {
        lock_guard<mutex> lock(m);
        for (auto& p : _commandInfo) {
            const string& commandName = p.first;
            const BedrockConflictManagerCommandInfo& commandInfo = p.second;

            out << "Command: " << commandName << endl;
            out << "Total Count: " << commandInfo.count << endl;
            out << "Table usage:" << endl;
            for (const auto& t : commandInfo.tableUseCounts) {
                const string& tableName = t.first;
                const size_t& count = t.second;
                out << tableName << ": " << count << endl;
                out << endl;
            }
        }
    }
    return out.str();
}
