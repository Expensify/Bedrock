/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    ConflictLockGuard.h
 * Path:    ConflictLockGuard.h
 * Pair:    ConflictLockGuard.cpp
 *
 * INTENT
 *   RAII scoped lock keyed by an opaque uint64_t identifier (e.g. a
 *   commit-conflict key), backed by a shared, refcounted, LRU-pruned pool
 *   of mutexes so concurrent commands only block each other when they
 *   collide on the same identifier.
 *
 * OBJECTS
 *   ConflictLockGuard - RAII guard; the constructor blocks until the
 *       mutex for `identifier` is held and the destructor releases it.
 *       identifier == 0 is treated as a no-op (no locking).
 *   ConflictLockGuard::controlMutex/mutexes/mutexCounts/mutexOrder/
 *       mutexOrderFastLookup (static) - shared pool of per-identifier
 *       mutexes, their reference counts, and an LRU order used to prune
 *       the pool once it grows past a cap.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   The private static members (controlMutex, mutexes, mutexCounts,
 *   mutexOrder, mutexOrderFastLookup) lack the repo's leading-underscore
 *   convention for private members; only the instance member _identifier
 *   follows it.
 * ─────────────────────────────────────────────────────────────────────*/
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
