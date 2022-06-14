#include "SMultiHostSocketPool.h"

SMultiHostSocketPool::SMultiHostSocketPool() {}
SMultiHostSocketPool::~SMultiHostSocketPool() {}

unique_ptr<STCPManager::Socket> SMultiHostSocketPool::getSocket(const string& host) {
    lock_guard<mutex> lock(_poolMutex);
    if (!_pools[host]) {
        _pools[host] = make_unique<SSocketPool>(host);
    }

    return _pools[host]->getSocket();
}

void SMultiHostSocketPool::returnSocket(unique_ptr<STCPManager::Socket>&& s, const string& host) {
    lock_guard<mutex> lock(_poolMutex);
    if (_pools[host]) {
        _pools[host]->returnSocket(move(s));
    }
}
