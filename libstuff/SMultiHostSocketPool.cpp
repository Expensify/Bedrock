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
