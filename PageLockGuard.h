#pragma once
#include <list>
#include <mutex>
#include <map>

using namespace std;

class PageLockGuard {
public:
    PageLockGuard(int64_t pageNumber);
    ~PageLockGuard();

private:

    // For controlling access to internals.
    static mutex controlMutex;
    static map<int64_t, mutex> mutexes;
    static map<int64_t, int64_t> mutexCounts;
    static list<int64_t> mutexOrder;
    static map<int64_t, list<int64_t>::iterator> mutexOrderFastLookup;
    int64_t _pageNumber;
};
