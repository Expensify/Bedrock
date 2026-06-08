#pragma once
#include <list>
#include <mutex>
#include <map>

using namespace std;

class ConflictLockGuard {
public:
    ConflictLockGuard(uint64_t identifier);
    ~ConflictLockGuard();

private:

    // For controlling access to internals.
    static mutex controlMutex;
    static map<uint64_t, mutex> mutexes;
    static map<uint64_t, int64_t> mutexCounts;
    static list<uint64_t> mutexOrder;
    static map<uint64_t, list<uint64_t>::iterator> mutexOrderFastLookup;
    uint64_t _identifier;
};
