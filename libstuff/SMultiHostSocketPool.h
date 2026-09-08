/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SMultiHostSocketPool.h
 * Path:    libstuff/SMultiHostSocketPool.h
 * Pair:    SMultiHostSocketPool.cpp
 *
 * INTENT
 *   Keys a separate SSocketPool per hostname so callers can get/return
 *   sockets to any number of hosts through one object, rather than
 *   managing one SSocketPool per host themselves.
 *
 * OBJECTS
 *   SMultiHostSocketPool  - owns a mutex-guarded map<host, SSocketPool>;
 *                            getSocket/returnSocket look up (creating if
 *                            needed) the pool for a given host and
 *                            delegate to it.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; sits naturally beside SSocketPool, which it wraps.
 *
 * NAMING QUALITY
 *   Fits repo convention.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <libstuff/STCPManager.h>
#include <libstuff/SSocketPool.h>

class SMultiHostSocketPool {
public:
    SMultiHostSocketPool();
    ~SMultiHostSocketPool();

    // Returns an existing or new socket.
    unique_ptr<STCPManager::Socket> getSocket(const string& host);

    // Makes an existing socket available to be used again.
    void returnSocket(unique_ptr<STCPManager::Socket>&& s, const string& host);

private:
    mutex _poolMutex;
    map<string, SSocketPool> _pools;
};
