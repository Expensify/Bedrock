#include "SMultiSocketPool.h"

SMultiSocketPool::SMultiSocketPool() {}
SMultiSocketPool::~SMultiSocketPool() {}

// Sockets are specific to the host we are trying to connect to. If we were to
// use a single socket pool to try to connect to multiple hosts, we would have
// to reset the pool every time we had a new host, effectively defeating the
// purpose. This multi socket pool maintains a separate pool per host this node
// is trying to connect to.
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
