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
map<int64_t, mutex> PageLockGuard::mutexes;
list<int64_t> PageLockGuard::mutexOrder;
map<int64_t, list<int64_t>::iterator> PageLockGuard::mutexOrderFastLookup;
map<int64_t, int64_t> PageLockGuard::mutexCounts;

PageLockGuard::PageLockGuard(int64_t page) : _page(page) {
    if (_page == 0) {
        return;
    }

    // We need access to the mutex outside of the control lock, so that we aren't blocking other PageLockGuard users while we wait for the page.
    mutex* m;
    {
        lock_guard<mutex> lock(controlMutex);
        auto mutexPair = mutexes.find(_page);
        if (mutexPair == mutexes.end()) {
            mutexPair = mutexes.emplace(piecewise_construct, forward_as_tuple(_page), forward_as_tuple()).first;
            mutexCounts.emplace(make_pair(_page, 1l));
            mutexOrder.push_front(_page);
            mutexOrderFastLookup.emplace(make_pair(_page, mutexOrder.begin()));
        } else {
            // Increment the reference count.
            mutexCounts[_page]++;

            // If the current mutex was already at the front of the order list, no updates are needed.
            if (mutexOrder.front() != _page) {
                // Erase the old location of this mutex in the order list and move it to the front.
                mutexOrder.erase(mutexOrderFastLookup.at(_page));
                mutexOrder.push_front(_page);

                // And save the new fast lookup at the front.
                mutexOrderFastLookup[_page] = mutexOrder.begin();
            }
        }

        static const size_t MAX_PAGE_MUTEXES = 500;
        if (mutexes.size() > MAX_PAGE_MUTEXES) {
            size_t iterationsToTry = mutexes.size() - MAX_PAGE_MUTEXES;
            auto pageIt = mutexOrder.end();
            for (size_t i = 0; i < iterationsToTry; i++) {
                pageIt--;
                int64_t pageToDelete = *pageIt;
                if (mutexCounts[pageToDelete] == 0) {
                    mutexes.erase(pageToDelete);
                    mutexCounts.erase(pageToDelete);
                    mutexOrderFastLookup.erase(pageToDelete);
                    pageIt = mutexOrder.erase(pageIt);
                }
            }
        }

        m = &mutexPair->second;
    }
    m->lock();
}

PageLockGuard::~PageLockGuard() {
    if (_page == 0) {
        return;
    }

    lock_guard<mutex> lock(controlMutex);
    auto mutexPair = mutexes.find(_page);
    mutexPair->second.unlock();

    auto mutexCount = mutexCounts.find(_page);
    mutexCount->second--;
}
