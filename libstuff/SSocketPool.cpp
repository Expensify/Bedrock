#include "SSocketPool.h"

SSocketPool::SSocketPool(const string& host)
  : host(host),
    _timeoutThread(&SSocketPool::_timeoutThreadFunc, this) {
    SDEBUG("[SOCKET] Creating SocketPool for host " <<  host); // OK
}

SSocketPool::~SSocketPool() {
    {
        unique_lock<mutex> lock(_poolMutex);
        _exit = true;
    }
    SDEBUG("[SOCKET] Destroying SocketPool for host " << host << " with " << _sockets.size() << " remaining sockets."); // OK
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
            SDEBUG("[SOCKET] Exiting socket pool thread."); // OK
            return;
        }

        // Prune any sockets that expired already.
        auto now = chrono::steady_clock::now();
        auto last = _sockets.begin();
        while (((last->first + timeout) < now) && last != _sockets.end()) {
            last++;
        }

        string msg = "About to remove sockets 10s before now (" + to_string(now.time_since_epoch().count()) + "): ";

        auto it = _sockets.begin();
        while (it != last) {
            msg += to_string(it->first.time_since_epoch().count()) + ", ";
            it++;
        }
        SDEBUG("[SOCKET] " << msg << "DONE"); // OK

        // This calls the destructor for each item in the list, closing the sockets.
        auto temp = _sockets.size();
        _sockets.erase(_sockets.begin(), last);
        SDEBUG("[SOCKET] Pruned " << (temp - _sockets.size()) << " old sockets. From " << temp << " to " << _sockets.size()); // OK

        // If there are still sockets, the next wakeup is `timeout` after the first one.
        if (_sockets.size()) {
            SDEBUG("[SOCKET] Waiting until " << (_sockets.front().first + timeout).time_since_epoch().count() << " for next socket prune."); // OK
            _poolCV.wait_until(lock, _sockets.front().first + timeout);
        } else {
            // If there are no more sockets, we sleep until we're interrupted.
            SDEBUG("[SOCKET] Waiting indefinitely for next socket prune."); // OK
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
            SDEBUG("[SOCKET] Returning existing socket " << s.second->s); // OK
            return move(s.second);
        }
    }

    // If we get here, we need to create a socket to return. No need to hold the lock, so it goes out of scope.
    try {
        // TODO: Allow S_socket to take a parsed address instead of redoing all the parsing above.
        auto ptr = unique_ptr<STCPManager::Socket>(new STCPManager::Socket(host, nullptr));
        SDEBUG("[SOCKET] Returning new socket " << ptr->s); // OK
        ptr->pool = true;
        return ptr;
        //return unique_ptr<STCPManager::Socket>(new STCPManager::Socket(host, nullptr));
    } catch (const SException& exception) {
        return nullptr;
    }
}

void SSocketPool::returnSocket(unique_ptr<STCPManager::Socket>&& s) {
    bool needWake = false;
    {
        lock_guard<mutex> lock(_poolMutex);
        SDEBUG("[SOCKET] Replacing socket " << s->s); // OK
        _sockets.emplace_back(make_pair(chrono::steady_clock::now(), move(s)));
        if (_sockets.size() == 1) {
            needWake = true;
        }
    }

    // Notify the waiting thread that we have something for it to do in 10s.
    if (needWake) {
        SDEBUG("[SOCKET] Notifying thread that we have a new socket."); // OK
        _poolCV.notify_one();
    }
}
