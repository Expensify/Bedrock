#include "SQLiteSocketPool.h"

SQLiteSocketPool::SQLiteSocketPool(const string& host)
  : host(host),
    _timeoutThread(&SQLiteSocketPool::_timeoutThreadFunc, this) {
}

SQLiteSocketPool::~SQLiteSocketPool() {
    exit = true;
    _timeoutThread.join();
}

void SQLiteSocketPool::_timeoutThreadFunc() {
    while (!exit) {
        this_thread::sleep_for(1s);
    }
}

unique_ptr<STCPManager::Socket> SQLiteSocketPool::getSocket() {
    {
        lock_guard<mutex> lock(_poolMutex);
        _pruneOldSockets();
        if (_sockets.size()) {
            pair<chrono::steady_clock::time_point, unique_ptr<STCPManager::Socket>> s = move(_sockets.front());
            _sockets.pop_front();
            return move(s.second);
        }
    }

    // If we get here, we need to create a socket to return. No need to hold the lock, so it goes out of scope.
    try {
        // TODO: Allow S_socket to take a parsed address instead of redoing all the parsing above.
        return unique_ptr<STCPManager::Socket>(new STCPManager::Socket(host, nullptr));
    } catch (const SException& exception) {
        return nullptr;
    }
}

void SQLiteSocketPool::returnSocket(unique_ptr<STCPManager::Socket>&& s) {
    lock_guard<mutex> lock(_poolMutex);
    _pruneOldSockets();
    _sockets.emplace_back(make_pair(chrono::steady_clock::now(), move(s)));
}

void SQLiteSocketPool::_pruneOldSockets() {
    // Doesn't lock because private. Public functions should lock before calling.
    auto last = _sockets.begin();
    while(last->first < (chrono::steady_clock::now() - timeout)) {
        last++;
    }

    // This calls the destructor for each item in the list, closing the sockets.
    _sockets.erase(_sockets.begin(), last);
}
