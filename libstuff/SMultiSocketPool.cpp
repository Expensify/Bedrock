#include "SMultiSocketPool.h"

SMultiSocketPool::SMultiSocketPool() {}
SMultiSocketPool::~SMultiSocketPool() {}

unique_ptr<STCPManager::Socket> SMultiSocketPool::getSocket(const string& host) {
    lock_guard<mutex> lock(_poolMutex);
    if (!_pools[host]) {
        _pools[host] = make_unique<SSocketPool>(host);
    }

    return _pools[host]->getSocket();
}

void SMultiSocketPool::returnSocket(unique_ptr<STCPManager::Socket>&& s, string host) {
    lock_guard<mutex> lock(_poolMutex);
    if (_pools[host]) {
        _pools[host]->returnSocket(move(s));
    }
}
