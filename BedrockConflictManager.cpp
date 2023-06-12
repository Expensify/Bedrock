#include "BedrockConflictManager.h"
#include <libstuff/libstuff.h>

BedrockConflictManager::BedrockConflictManager() {
}

void BedrockConflictManager::recordTables(const string& commandName, const set<string>& tables) {
    {
        lock_guard<mutex> lock(m);
        auto commandInfoIt = _commandInfo.find(commandName);
        if (commandInfoIt == _commandInfo.end()) {
            commandInfoIt = _commandInfo.emplace(make_pair(commandName, BedrockConflictManagerCommandInfo())).first;
        }
        BedrockConflictManagerCommandInfo& commandInfo = commandInfoIt->second;

        // Increase the count of the command in general.
        commandInfo.count++;

        // And for each table (that's not a journal).
        for (auto& table : tables) {
            // Skip journals, they change on every instance of a command running and thus aren't useful for profiling which commands access which tables most frequently.
            if (SStartsWith(table, "journal")) {
                continue;
            }

            if (table == "json_each") {
                continue;
            }

            // Does this command already have this table?
            auto tableInfoIt = commandInfo.tableUseCounts.find(table);
            if (tableInfoIt == commandInfo.tableUseCounts.end()) {
                tableInfoIt = commandInfo.tableUseCounts.emplace(make_pair(table, 1)).first;
            } else {
                // tableInfoIt is an iterator into a map<string, size_t> (tableUseCounts), where the key is the table name and the value is the count of uses for this command.
                // Incrementing `second` increases the count.
                tableInfoIt->second++;
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
            out << "Table usage" << endl;
            for (const auto& table : commandInfo.tableUseCounts) {
                const string& tableName = table.first;
                const size_t& count = table.second;
                out << "    " << tableName << ": " << count << endl;
            }
            out << endl;
        }
    }
    return out.str();
}

mutex PageLockGuard::controlMutex;
map<int64_t, PageLockGuard::MPair> PageLockGuard::mutexCounts;
list<int64_t> PageLockGuard::mutexOrder;

PageLockGuard::PageLockGuard(int64_t page) : _page(page) {
    lock_guard<mutex> lock(controlMutex);
    auto result = mutexCounts.find(page);
    if (result == mutexCounts.end()) {
        result = mutexCounts.emplace(piecewise_construct, forward_as_tuple(page), forward_as_tuple()).first;
        mutexOrder.push_front(page);
    } else {
        result->second.count++;
    }
    _mref = &(result->second.m);
    _mref->lock();
}

PageLockGuard::~PageLockGuard() {
    _mref->unlock();
}
