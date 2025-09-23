#include "SSocketPool.h"

SSocketPool::SSocketPool(const string& host)
  : host(host),
    _timeoutThread(&SSocketPool::_timeoutThreadFunc, this) {
}

SSocketPool::~SSocketPool() {
    {
        unique_lock<mutex> lock(_poolMutex);
        _exit = true;
    }
    _poolCV.notify_one();
    _timeoutThread.join();
}

void SSocketPool::_timeoutThreadFunc() {
    // NOTE: there's nothing preventing multiple threads with the same name.
    SInitialize("SocketPool");
    while (true) {
        unique_lock<mutex> lock(_poolMutex);

        // If `exit` is set, we are done.
        if (_exit) {
            return;
        }

        // Prune any sockets that expired already.
        if (_sockets.size()) {
            auto now = chrono::steady_clock::now();
            auto last = _sockets.begin();
            while (last != _sockets.end() && ((last->first + timeout) < now)) {
                last++;
            }

            // This calls the destructor for each item in the list, closing the sockets.
            _sockets.erase(_sockets.begin(), last);
        }

        // If there are still sockets, the next wakeup is `timeout` after the first one.
        if (_sockets.size()) {
            _poolCV.wait_until(lock, _sockets.front().first + timeout);
        } else {
            // If there are no more sockets, we sleep until we're interrupted.
            _poolCV.wait(lock);
        }
    }
}

unique_ptr<STCPManager::Socket> SSocketPool::getSocket() {
    {
        // If there's an existing socket, return it.
        lock_guard<mutex> lock(_poolMutex);
        if (_sockets.size()) {
            pair<chrono::steady_clock::time_point, unique_ptr<STCPManager::Socket>> s = move(_sockets.front());
            _sockets.pop_front();
            return move(s.second);
        }
    }

    // If we get here, we need to create a socket to return. No need to hold the lock, so it goes out of scope.
    try {
        // TODO: Allow S_socket to take a parsed address instead of redoing all the parsing each time.
        return unique_ptr<STCPManager::Socket>(new STCPManager::Socket(host));
    } catch (const SException& exception) {
        return nullptr;
    }
}

void SSocketPool::returnSocket(unique_ptr<STCPManager::Socket>&& s) {
    if (s == nullptr) {
        SWARN("[SOCKET] Trying to return a null socket to the pool, this should not happen.");
        return;
    }

    bool needWake = false;
    {
        lock_guard<mutex> lock(_poolMutex);
        _sockets.emplace_back(make_pair(chrono::steady_clock::now(), move(s)));
        if (_sockets.size() == 1) {
            needWake = true;
        }
    }

    // Notify the waiting thread that we have something for it to do in 10s.
    if (needWake) {
        _poolCV.notify_one();
    }
}
