/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SMultiHostSocketPool.cpp
 * Path:    libstuff/SMultiHostSocketPool.cpp
 * Pair:    SMultiHostSocketPool.h
 *
 * INTENT
 *   Implements SMultiHostSocketPool; see the header for the public
 *   contract.
 *
 * OBJECTS
 *   SMultiHostSocketPool::SMultiHostSocketPool/~SMultiHostSocketPool - empty;
 *                            no owned resources need explicit setup/teardown
 *                            (the map's SSocketPool entries clean up themselves).
 *   SMultiHostSocketPool::getSocket    - under `_poolMutex`, finds or
 *                            emplaces the per-host SSocketPool and returns
 *                            a socket from it.
 *   SMultiHostSocketPool::returnSocket - under `_poolMutex`, returns the
 *                            socket to its host's pool if one still exists.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Fits repo convention.
 * ─────────────────────────────────────────────────────────────────────*/
#include "SMultiHostSocketPool.h"

SMultiHostSocketPool::SMultiHostSocketPool()
{
}

SMultiHostSocketPool::~SMultiHostSocketPool()
{
}

unique_ptr<STCPManager::Socket> SMultiHostSocketPool::getSocket(const string& host)
{
    lock_guard<mutex> lock(_poolMutex);
    auto pool = _pools.find(host);
    if (pool == _pools.end()) {
        pool = _pools.emplace(host, host).first;
    }

    return pool->second.getSocket();
}

void SMultiHostSocketPool::returnSocket(unique_ptr<STCPManager::Socket>&& s, const string& host)
{
    lock_guard<mutex> lock(_poolMutex);
    auto pool = _pools.find(host);
    if (pool != _pools.end()) {
        pool->second.returnSocket(move(s));
    }
}
